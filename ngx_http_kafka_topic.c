/*
 * ngx_http_kafka_topic.c
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */

#include "ngx_http_kafka_upstream.h"
#include "ngx_http_kafka_broker.h"
#include "ngx_http_kafka_buf.h"
#include "ngx_http_kafka_topic.h"

static void ngx_http_kafka_toppar_update(ngx_kfk_topic_t *topic,
	ngx_kfk_part_meta_t *part);
static void ngx_http_kafka_toppar_force_send(ngx_event_t *ev);
static void ngx_http_kafka_toppar_leader_update(ngx_kfk_toppar_t *toppar,
	ngx_kfk_broker_t *broker);
static void ngx_http_kafka_toppar_move_to_valid(ngx_kfk_toppar_t *toppar);
static void ngx_http_kafka_destroy_toppar(ngx_kfk_toppar_t *toppar,
	ngx_int_t err);


void
ngx_http_kafka_topic_update(ngx_kfk_ctx_t *kfk, ngx_kfk_top_meta_t *tm)
{
	ngx_kfk_topic_t		*topic;
	ngx_kfk_toppar_t	*toppars;
	ngx_uint_t			 i;

	topic = ngx_http_kafka_topic_find(kfk, tm->topic.data, tm->topic.len);
	if (topic == NULL) {
		return;
	}

#if (NGX_KFK_DEBUG)
	debug(kfk->log, "[kafka] update topic(%D, name: %V, part_cnt: %D", 
          tm->err, &tm->topic, tm->part_cnt);
#endif

	if (tm->err != 0) {
		if (tm->err < 0 || tm->err > KAFKA_LAST_ERR) {
			ngx_log_error(NGX_LOG_ERR, kfk->log, 0,
						  "[kafka] [%V] metadata unknow error: %i",
						  &tm->topic, &tm->err);

		} else {
			ngx_log_error(NGX_LOG_ERR, kfk->log, 0, "[kafka] [%V] metadata error: %s",
						  &tm->topic, ngx_kfk_strerr[tm->err]);
		}

		topic->state = NGX_KFK_TOPIC_UNKNOWN;
		/* for topic error, we must destroy all partition and backup unsend messages */
		ngx_http_kafka_destroy_topic(topic, tm->err);
		return;
	}

	if (topic->toppars == NULL) {
		topic->toppars = ngx_array_create(kfk->pool, tm->part_cnt,
										  sizeof(ngx_kfk_toppar_t));
    
		if (topic->toppars == NULL) {
			ngx_log_error(NGX_LOG_ALERT, kfk->log, 0, "[kafka] no enough memroy");
			return;
		}
	}

	topic->state = NGX_KFK_TOPIC_EXIST;

	for (i = 0; i < (ngx_uint_t)tm->part_cnt; i++) {
#if (NGX_KFK_DEBUG)
        debug(kfk->log, "[kafka] update partition "
              "(err:%D, partition:%D, leader:%D)",
              (int32_t)tm->part[i].err, tm->part[i].id, tm->part[i].leader);
#endif

		ngx_http_kafka_toppar_update(topic, &tm->part[i]);
	}

	toppars = topic->toppars->elts;
	for (i = tm->part_cnt; i < topic->toppars->nelts; i++) {
        if (toppars[i].broker) {
            ngx_queue_remove(&toppars[i].queue);
            toppars[i].broker = NULL;
        }

		ngx_http_kafka_toppar_move_to_valid(&toppars[i]);
	}

	topic->toppars->nelts = tm->part_cnt;
}

ngx_kfk_topic_t*
ngx_http_kafka_topic_find(ngx_kfk_ctx_t *kfk, u_char *data, size_t len)
{
	ngx_kfk_topic_t		*topics;
	ngx_uint_t			 i;

	topics = kfk->topics->elts;
	for (i = 0; i < kfk->topics->nelts; i++) {
		if (ngx_strncmp(topics[i].name.data, data, len) == 0) {

			return &topics[i];
		}
	}

	return NULL;
}

void
ngx_http_kafka_destroy_topic(ngx_kfk_topic_t *topic, ngx_int_t err)
{
	ngx_kfk_toppar_t	*toppars;
	ngx_uint_t			 i, n;

	if (topic->toppars == NULL) {
		return;
	}

	n = topic->toppars->nelts;
	toppars = topic->toppars->elts;

	for (i = 0; i < n; i++) {
		ngx_http_kafka_destroy_toppar(&toppars[i], err);
	}

	topic->toppars->nelts = 0;
}

void
ngx_http_kafka_toppar_free_to_out(ngx_kfk_toppar_t *toppar)
{
	ngx_kfk_buf_t				*buf, *out;
    int32_t						*size, *msgset_size;

	buf = toppar->free;

	if (buf == NULL || buf->cnt <= 0) {
		return;
	}

	/* size */
	size = ngx_kfk_header_size_ptr(buf);

	/* msgset size */
	msgset_size = ngx_kfk_header_msgset_size_ptr(buf);
	*size += *msgset_size;

	*msgset_size = (int32_t)htonl((uint32_t)*msgset_size);
	*size = (int32_t)htonl((uint32_t)*size);

	/* corr id */
	ngx_kfk_header_corr_id(buf) = (int32_t)htonl(toppar->kfk->corr_id++);

	buf->timeout = buf->kfk->cf->msg_timeout + ngx_current_msec;

	/* concat neighbor buf chain */
	out = toppar->out;
	if (out != NULL) {
		*out->last_chain = buf->chain;
	}

	*toppar->last_out = buf;
	toppar->last_out = &buf->next;

	toppar->free = NULL;

#if (NGX_KFK_STATUS)
	if (buf->kfk->cf->status) {
		(void)ngx_atomic_fetch_add(ngx_kfk_out_buf, 1);
	}
#endif

#if (NGX_KFK_DEBUG)
	{
        ngx_chain_t     *cl;
        ngx_uint_t       i = 0;
		u_char          *q, *p, temp[512];
		cl = buf->chain;
		while (i++ < 4) {
			for (p = cl->buf->pos, q = temp; p < cl->buf->last; p++) {
				q = ngx_snprintf(q, sizeof(temp) - (q - temp), "%Xi ", *p);
			}

			debug(buf->kfk->log, "[kafka] producer req chain: %*s", q - temp, temp);

			cl = cl->next;
		}
	}

	debug(buf->kfk->log, "[kafka] %ui messages to %V-%ui out buf", buf->cnt,
          &toppar->topic->name, toppar->partition);
#endif
}

ngx_kfk_toppar_t*
ngx_http_kafka_toppar_get_valid(ngx_kfk_topic_t *topic, ngx_uint_t partition)
{
	ngx_kfk_toppar_t	*toppars;
	ngx_uint_t			 i, n;

	toppars = topic->toppars->elts;
	n = topic->toppars->nelts;

	if (partition > n) {
		return NULL;
	}

	for (i = 0; i < n; i++) {
		if (toppars[(partition + i) % n].broker != NULL) {
			return &toppars[(partition + i) % n];
		}
	}

	return NULL;
}

static void
ngx_http_kafka_toppar_update(ngx_kfk_topic_t *topic, ngx_kfk_part_meta_t *part)
{
	ngx_kfk_ctx_t		*kfk;
	ngx_kfk_broker_t	*broker;
	ngx_kfk_toppar_t	*toppar;

	kfk = topic->kfk;

	toppar = ngx_http_kafka_toppar_get(topic, part->id);

	if (toppar == NULL) {
		toppar = ngx_array_push(topic->toppars);
		if (toppar == NULL) {
			ngx_log_error(NGX_LOG_ALERT, kfk->log, 0, "[kafka] no enough memroy");
			return;
		}

		ngx_memzero(toppar, sizeof(ngx_kfk_toppar_t));
		/* toppar->free = NULL
		 * toppar->out = NULL
		 * toppar->broker = NULL
		 */
		toppar->kfk = kfk;
		toppar->partition = part->id;
		toppar->topic = topic;
		toppar->last_out = &toppar->out;

		toppar->send_timer.data = toppar;
		toppar->send_timer.handler = ngx_http_kafka_toppar_force_send;
		toppar->send_timer.log = kfk->log;
	}

	broker = ngx_http_kafka_broker_find(kfk, part->leader);

	ngx_http_kafka_toppar_leader_update(toppar, broker);
}

static void
ngx_http_kafka_toppar_force_send(ngx_event_t *ev)
{
	ngx_http_kafka_upstream_t	*u;
	ngx_kfk_broker_t			*broker;
	ngx_kfk_toppar_t			*toppar;

	toppar = ev->data;

#if (NGX_KFK_DEBUG)
	debug(ev->log, "[kafka] [%V-%ui] force send timer",
		  &toppar->topic->name, toppar->partition);
#endif

	ngx_http_kafka_toppar_free_to_out(toppar);

	broker = toppar->broker;
	if (broker != NULL && broker->state == NGX_KFK_BROKER_UP) {
		u = broker->upstream;

		ngx_http_kafka_upstream_enable_write_event(broker, u);

		u->send_request(broker, u);
	}
}

static void
ngx_http_kafka_toppar_leader_update(ngx_kfk_toppar_t *toppar,
	ngx_kfk_broker_t *broker)
{
	ngx_kfk_ctx_t		*kfk;
	ngx_kfk_topic_t		*topic;

	if (toppar->broker == broker) {
		return;
	}

	topic = toppar->topic;
	kfk = topic->kfk;

    if (toppar->broker) {
        ngx_queue_remove(&toppar->queue);
    }

	if (broker == NULL) {
		ngx_log_error(NGX_LOG_CRIT, kfk->log, 0, "[kafka] [%V-%ui] lose leader",
					  &topic->name, toppar->partition);
        
        toppar->broker = NULL;
		ngx_http_kafka_toppar_move_to_valid(toppar);

		return;
	}

	if (toppar->broker == NULL) {
		ngx_log_error(NGX_LOG_INFO, kfk->log, 0, "[kafka] [%V-%ui] get leader",
					  &topic->name, toppar->partition);
	} else {
		ngx_log_error(NGX_LOG_INFO, kfk->log, 0,
					  "[kafka] [%V-%ui] change leader to %V:%ui",
					  &topic->name, toppar->partition,
					  &broker->host, broker->port);
	}

    toppar->broker = broker;
	ngx_queue_insert_tail(&broker->toppars, &toppar->queue);
}

static void
ngx_http_kafka_toppar_move_to_valid(ngx_kfk_toppar_t *toppar)
{
	ngx_kfk_toppar_t	*new_toppar;
	ngx_kfk_buf_t		*out;

	new_toppar = ngx_http_kafka_toppar_get_valid(toppar->topic, 0);
	if (new_toppar == NULL) {
		ngx_http_kafka_destroy_toppar(toppar, KAFKA_LEADER_NOT_AVAILABLE);
		return;
	}

	ngx_http_kafka_toppar_free_to_out(toppar);

	out = toppar->out;
	while (out != NULL) {
		ngx_kfk_header_partition(out) = (int32_t)htonl(new_toppar->partition);

		out = out->next;
	}

	*toppar->last_out = new_toppar->out;
	new_toppar->out = out;

	toppar->out = NULL;
	toppar->last_out = &toppar->out;
}

static void
ngx_http_kafka_destroy_toppar(ngx_kfk_toppar_t *toppar, ngx_int_t err)
{
	ngx_kfk_buf_t	*out, *next;
	ngx_int_t		 n = 0;

	if (toppar->broker) {
		ngx_queue_remove(&toppar->queue);
		toppar->broker = NULL;
	}

	if (toppar->send_timer.timer_set) {
		ngx_del_timer(&toppar->send_timer);
	}

	if (toppar->free) {
		ngx_http_kafka_destroy_buf(toppar->free, err);
		toppar->free = NULL;
	}

	out = toppar->out;

	while (out != NULL) {
		next = out->next;
		ngx_http_kafka_destroy_buf(out, err);
		out = next;
		n++;
	}

#if (NGX_KFK_STATUS)
	if (n && toppar->kfk->cf->status) {
		(void)ngx_atomic_fetch_add(ngx_kfk_out_buf, -n);
	}
#endif

	toppar->out = NULL;
	toppar->last_out = &toppar->out;
}
