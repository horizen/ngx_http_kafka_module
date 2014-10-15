/*
 * ngx_http_kafka_broker.c
 *
 *  Created on: 2014年9月21日
 *      Author: yw
 */

#include "ngx_http_kafka_buf.h"
#include "ngx_http_kafka_upstream.h"
#include "ngx_http_kafka_topic.h"
#include "ngx_http_kafka_broker.h"

#define NGX_KAFKA_MIN_RSP_SIZE 8
#define PREMALLOC_PART_META_CNT 20

static void ngx_http_kafka_broker_create_request(ngx_kfk_broker_t *broker);
static ngx_int_t ngx_http_kafka_broker_process_response(ngx_kfk_broker_t *broker);
static ngx_int_t ngx_http_kafka_producer_process_response(ngx_kfk_broker_t *broker,
	ngx_kfk_buf_t *buf, ngx_buf_t *b);
static ngx_int_t ngx_http_kafka_meta_process_response(ngx_kfk_broker_t *broker,
	ngx_kfk_buf_t *buf, ngx_buf_t *b);
static void ngx_http_kafka_handle_producer_error(ngx_kfk_broker_t *broker, 
    ngx_int_t err);
static void ngx_http_kafka_update_broker(ngx_kfk_ctx_t *kfk, ngx_str_t *host,
	ngx_uint_t port, ngx_int_t id);
static ngx_kfk_buf_t* ngx_http_kafka_broker_waitrsp_find(ngx_kfk_broker_t *broker,
	int32_t corr_id);


ngx_kfk_broker_t *
ngx_http_kafka_new_broker(ngx_kfk_ctx_t *kfk, ngx_str_t *host,
	ngx_uint_t port, ngx_int_t id)
{
	ngx_http_kafka_upstream_t	*u;
	ngx_kfk_broker_t			*broker;
	u_char 						*p;

	broker = ngx_alloc(sizeof(ngx_kfk_broker_t), kfk->log);

	if (broker == NULL) {
		return NULL;
	}

	ngx_memzero(broker, sizeof(ngx_kfk_broker_t));
	/*
	 *  broker->toppars = NULL
	 *  broker->rsp_size = 0
	 *  broker->upstream = NULL
	 *  broker->wait = NULL
	 *  broker->meta_query = 0
	 */

	broker->pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, kfk->log);
	if (broker->pool == NULL) {
		ngx_free(broker);
		return NULL;
	}

	broker->pool->log = kfk->log;

	p = ngx_palloc(broker->pool, host->len);
	if (p == NULL) {
		ngx_destroy_pool(broker->pool);
		ngx_free(broker);

		return NULL;
	}

	ngx_memcpy(p, host->data, host->len);
	broker->host.data = p;
	broker->host.len = host->len;

	broker->id = id;
	broker->port = port;

	broker->kfk = kfk;
	broker->log = kfk->log;

	broker->reconn.data = broker;
	broker->reconn.handler = ngx_http_kafka_upstream_reconn;
	broker->reconn.log = kfk->log;

	broker->state = NGX_KFK_BROKER_DOWN;

	broker->last_wait = &broker->wait;

	u = ngx_http_kafka_upstream_create(broker);
	if (u == NULL) {
		ngx_destroy_pool(broker->pool);
		ngx_free(broker);

		return NULL;
	}

	u->create_request = ngx_http_kafka_broker_create_request;
	u->process_response = ngx_http_kafka_broker_process_response;

	broker->upstream = u;

    ngx_queue_init(&broker->toppars);

	ngx_queue_insert_tail(&kfk->brokers, &broker->queue);

    debug(kfk->log, "[kafka] add a broker (%V:%ui)", host, port);
	return broker;
}

ngx_kfk_broker_t*
ngx_http_kafka_broker_find(ngx_kfk_ctx_t *kfk, ngx_int_t id)
{
	ngx_kfk_broker_t	*broker;
	ngx_queue_t			*q;

	if (id == -1) {
		return NULL;
	}

    for (q = ngx_queue_head(&kfk->brokers);
         q != ngx_queue_sentinel(&kfk->brokers);
         q = ngx_queue_next(q))
    {
        broker = ngx_queue_data(q, ngx_kfk_broker_t, queue);

        if (broker->id == id) {
        	return broker;
        }
    }

    return NULL;
}

void
ngx_http_kafka_broker_waitrsp_timeout_scan(ngx_kfk_broker_t *broker)
{
	ngx_kfk_buf_t	*out, *next, **prev;
	ngx_int_t		 n = 0;

	out = broker->wait;
	prev = &broker->wait;

	while (out != NULL) {
		next = out->next;

		if (out->timeout < ngx_current_msec) {
			ngx_http_kafka_destroy_buf(out, NGX_KAFKA_MESSAGE_TIMED_OUT);
			*prev = next;
			n++;

		} else {
			prev = &out->next;
		}

		out = next;
	}
#if (NGX_KFK_STATUS)
	if (n && broker->kfk->cf->status) {
		(void)ngx_atomic_fetch_add(ngx_kfk_wait_buf, -n);
	}
#endif

}

void
ngx_http_kafka_destroy_broker(ngx_kfk_broker_t *broker)
{
	ngx_kfk_buf_t	 *buf, *next;
	ngx_int_t		  n = 0;

	debug(broker->log, "[kafka] [%V:%ui] destroyed",
		  &broker->host, broker->port);

	if (broker->upstream) {
		ngx_http_kafka_upstream_finalize(broker, broker->upstream);
	}

	if (broker->reconn.timer_set) {
		ngx_del_timer(&broker->reconn);
	}

	if (broker != broker->kfk->meta_broker) {
		buf = broker->wait;

		while (buf != NULL) {
			next = buf->next;
			ngx_http_kafka_destroy_buf(buf, NGX_KAFKA_BROKER_DOWN);
			buf = next;
			n++;
		}
#if (NGX_KFK_STATUS)
		if (broker->kfk->cf->status) {
			(void)ngx_atomic_fetch_add(ngx_kfk_wait_buf, -n);
		}
#endif
		broker->wait = NULL;
		broker->last_wait = &broker->wait;
	}

	ngx_queue_remove(&broker->queue);

	ngx_destroy_pool(broker->pool);

	ngx_free(broker);
}

static void
ngx_http_kafka_broker_create_request(ngx_kfk_broker_t *broker)
{
	ngx_http_kafka_upstream_t	*u;
	ngx_kfk_toppar_t			*toppar;
	ngx_kfk_ctx_t				*kfk;
    ngx_kfk_buf_t				*out, *next;
	ngx_queue_t					*last, *q;

#if (NGX_KFK_STATUS)
	ngx_int_t					 n = 0;
#endif

	kfk = broker->kfk;
	u = broker->upstream;

	if (broker == kfk->meta_broker) {
        if (broker->meta_query) {
	    	u->request_bufs = kfk->meta_buf.chain;
		    kfk->meta_buf.next = broker->wait;
    		broker->wait = &kfk->meta_buf;
        }

	} else {
        q = ngx_queue_head(&broker->toppars);
        if (q == ngx_queue_sentinel(&broker->toppars)) {
            return;
        }

        last = ngx_queue_last(&broker->toppars);
        out = NULL;
        toppar = NULL;
        
        do {
            q = ngx_queue_head(&broker->toppars);

            toppar = ngx_queue_data(q, ngx_kfk_toppar_t, queue);
            out = toppar->out;

#if (NGX_KFK_DEBUG)
	        debug(kfk->log, "[kafka] [%V:%ui] scan partition: %V-%ui, out: %p", 
                  &broker->host, broker->port, &toppar->topic->name, 
                  toppar->partition, out);
#endif

            ngx_queue_remove(q);
            ngx_queue_insert_tail(&broker->toppars, q);

        } while(q != last && out == NULL);

		if (toppar && toppar->send_timer.timer_set) {
			ngx_del_timer(&toppar->send_timer);
		}

		if (out == NULL) {
			return;
		}

        debug(broker->log, "[kafka] out chain: %p", out->chain);

		u->request_bufs = out->chain;

		if (kfk->cf->acks > 0) {
			*broker->last_wait = out;
			broker->last_wait = toppar->last_out;

#if (NGX_KFK_STATUS)
			while (out != NULL) {
				n++;
				out = out->next;
			}
			if (n && kfk->cf->status) {
				(void)ngx_atomic_fetch_add(ngx_kfk_out_buf, -n);
				(void)ngx_atomic_fetch_add(ngx_kfk_wait_buf, n);
			}
#endif
		} else {
			while (out != NULL) {
				next = out->next;
				ngx_http_kafka_destroy_buf(out, 0);
				out = next;
			}
		}

		toppar->out = NULL;
        toppar->last_out = &toppar->out;
	}
}

static ngx_int_t
ngx_http_kafka_broker_process_response(ngx_kfk_broker_t *broker)
{
	ngx_kfk_buf_t 	*buf;
	ngx_buf_t		*b;
	u_char			*p;
	int32_t 		 size, corr_id;
	ngx_int_t		 rc;

	b = &broker->upstream->buffer;

	while(b->pos < b->last) {
		if (broker->rsp_size == 0) {
			_read_i32(b, &size);
			broker->rsp_size = size;
		}
        
        size = broker->rsp_size;

		if (size < NGX_KAFKA_MIN_RSP_SIZE) {
			return NGX_KAFKA_INVALID_MSG_SIZE;
		}

		_check_len(b, size);

		p = b->pos + size;

		_read_i32(b, &corr_id);
              
        debug(broker->log, "[kafka] [%V:%ui] response, size: %D, corr_id: %D", 
              &broker->host, broker->port, size, corr_id);

		buf = ngx_http_kafka_broker_waitrsp_find(broker, corr_id);

		if (buf == NULL) {
			ngx_log_error(NGX_LOG_ERR, broker->log, 0,
						  "[kafka] [%V:%ui] outdate response because of unknown corrid: %D",
						  &broker->host, broker->port, corr_id);

		} else {
			if (corr_id == 0) {
				broker->meta_query = 0;

				rc = ngx_http_kafka_meta_process_response(broker, buf, b);

				if (rc != NGX_OK) {
					return rc;
				}

			} else {
				rc = ngx_http_kafka_producer_process_response(broker, buf, b);

				if (rc != NGX_OK) {
					return rc;
				}
			}
		}

		b->pos = p;
		broker->rsp_size = 0;
	}

	b->pos = b->start;
	b->last = b->start;

	return NGX_OK;

again:

	if (b->pos != b->start) {
		ngx_memmove(b->start, b->pos, b->last - b->pos);
		b->last = b->start + (b->last - b->pos);
		b->pos = b->start;
	}

	return NGX_AGAIN;
}

static ngx_int_t
ngx_http_kafka_meta_process_response(ngx_kfk_broker_t *broker,
	ngx_kfk_buf_t *buf, ngx_buf_t *b)
{
	ngx_kfk_top_meta_t	 tmeta;
	ngx_kfk_part_meta_t	*pmeta, prealloc_pmeta[PREMALLOC_PART_META_CNT];
	ngx_str_t 			 host;
	int32_t 			 i, j, id, port;
	int32_t				 broker_cnt, top_cnt, part_cnt, rep_cnt, isr_cnt;

	_read_i32(b, &broker_cnt);

	for(i = 0; i < broker_cnt; i++) {
		_read_i32(b, &id);
		_read_str(b, &host);
		_read_i32(b, &port);

		debug(broker->log, "[kafka] update broker (id: %D, host: %V, port: %D)",
			  id, &host, port);

		ngx_http_kafka_update_broker(broker->kfk, &host, port, id);
	}

	_read_i32(b, &top_cnt);

	for(i = 0; i < top_cnt; i++) {
		_read_i16(b, &tmeta.err);
		_read_str(b, &tmeta.topic);

		_read_i32(b, &part_cnt);

		if (part_cnt > PREMALLOC_PART_META_CNT) {
			pmeta = ngx_calloc(part_cnt * sizeof(ngx_kfk_part_meta_t),
							   broker->log);
		} else {
			pmeta = prealloc_pmeta;
		}

		if (pmeta == NULL) {
			return NGX_KAFKA_NO_MEMORY;
		}

		tmeta.part_cnt = part_cnt;
		tmeta.part = pmeta;

		for (j = 0; j < part_cnt; j++) {
			_read_i16(b, &pmeta[j].err);
			_read_i32(b, &pmeta[j].id);
			_read_i32(b, &pmeta[j].leader);

			_read_i32(b, &rep_cnt);
			_skip(b, rep_cnt * sizeof(int32_t));

			_read_i32(b, &isr_cnt);
			_skip(b, isr_cnt * sizeof(int32_t));

		}

		ngx_http_kafka_topic_update(broker->kfk, &tmeta);

		if (part_cnt > PREMALLOC_PART_META_CNT) {
			ngx_free(pmeta);
		}
	}

	return NGX_OK;

again:
	return NGX_KAFKA_INVALID_RESPONSE;
}

static ngx_int_t
ngx_http_kafka_producer_process_response(ngx_kfk_broker_t *broker,
	ngx_kfk_buf_t *buf, ngx_buf_t *b)
{
	ngx_str_t 	topic;
	int32_t 	top_cnt, par_cnt, partition;
	int16_t 	err = 0;

	_read_i32(b, &top_cnt);
	if (top_cnt != 1) {
		err = NGX_KAFKA_INVALID_TOPIC_CNT;
		goto error;
	}

	_read_str(b, &topic);

	_read_i32(b, &par_cnt);
	if (par_cnt != 1) {
		err = NGX_KAFKA_INVALID_PARTITION_CNT;
		goto error;
	}

	_read_i32(b, &partition);
	_read_i16(b, &err);

	/* skip offset */
	_skip(b, 8);

	debug(broker->log, "[kafka] [%V:%ui] producer response "
		  "(topic:%V, partition:%D, err:%D)",
		  &broker->host, broker->port, &topic, partition, err);

	if (err != 0) {
		if (err > KAFKA_LAST_ERR) {
			return NGX_KAFKA_INVALID_ERROR;
		}

		ngx_http_kafka_handle_producer_error(broker, err);
	}

	ngx_http_kafka_destroy_buf(buf, err);

	return NGX_OK;

again:
	return NGX_KAFKA_INVALID_RESPONSE;

error:
	ngx_http_kafka_destroy_buf(buf, err);

	return err;
}

static void
ngx_http_kafka_handle_producer_error(ngx_kfk_broker_t *broker, ngx_int_t err)
{
	switch(err) {

	case KAFKA_OFFSET_OUT_OF_RANGE:
	case KAFKA_OFFSET_METADATA_TOO_LARGE_CODE:
	case KAFKA_OFFSETS_LOAD_IN_PROGRESS_CODE:
	case KAFKA_NOT_COORDINATOR_FOR_CONSUMER_CODE:
	case KAFKA_CONSUMERE_COORDINATOR_NOT_AVAILABLE_CODE:

		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] producer strange error: %s",
					  &broker->host, broker->port, ngx_kfk_strerr[err]);
		break;

	case KAFKA_INVALID_MESSAGE:
	case KAFKA_INVALID_MESSAGE_SIZE:
	case KAFKA_MESSAGE_SIZE_TOO_LARGE:

		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] producer unexpected error: %s",
					  &broker->host, broker->port, ngx_kfk_strerr[err]);
		break;

	/* TODO retry */
	case KAFKA_REQUEST_TIMED_OUT:

		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] producer error: %s",
					  &broker->host, broker->port, ngx_kfk_strerr[err]);
		break;

	case KAFKA_ERR_UNKNOWN:

		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] producer error: unknown",
					  &broker->host, broker->port);

		ngx_http_kafka_meta_force_refresh(&broker->kfk->meta_ev);
		break;

	default:

		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] producer error: %s",
					  &broker->host, broker->port, ngx_kfk_strerr[err]);

		ngx_http_kafka_meta_force_refresh(&broker->kfk->meta_ev);
	}
}

static void
ngx_http_kafka_update_broker(ngx_kfk_ctx_t *kfk, ngx_str_t *host,
	ngx_uint_t port, ngx_int_t id)
{
	ngx_kfk_broker_t	*broker;
	u_char				*p;

	broker = ngx_http_kafka_broker_find(kfk, id);

	if (broker != NULL) {
		p = broker->host.data;

		if (ngx_strncmp(p, host->data, host->len) != 0
			|| broker->port != port)
		{
            debug(kfk->log, "[kafka] broker %i changed to [%V:%ui]", id,
                  host, port);

			if (broker->host.len < host->len) {
				p = ngx_palloc(broker->pool, host->len);

				if (p == NULL) {
					ngx_log_error(NGX_LOG_ALERT, kfk->log, 0,
								  "[kafka] no enough memory");
                    /* TODO may be a bug */
					ngx_http_kafka_destroy_broker(broker);
					return;
				}
			}

			ngx_http_kafka_upstream_finalize(broker, broker->upstream);

			ngx_memcpy(p, host->data, host->len);

			broker->host.data = p;
			broker->host.len = host->len;

			ngx_http_kafka_upstream_init(broker);
		}

		return;
	}

	broker = ngx_http_kafka_new_broker(kfk, host, port, id);

	if (broker == NULL) {
		ngx_log_error(NGX_LOG_ALERT, kfk->log, 0, "[kafka] no enough memory");
		return;
	}

	ngx_http_kafka_upstream_init(broker);
}

static ngx_kfk_buf_t*
ngx_http_kafka_broker_waitrsp_find(ngx_kfk_broker_t *broker, int32_t corr_id)
{
	ngx_kfk_buf_t	*buf, **prev;
	int32_t			 id;

	buf = broker->wait;
	prev = &broker->wait;

	while (buf != NULL) {
		id = (int32_t)ntohl((uint32_t)ngx_kfk_header_corr_id(buf));

        if (id == corr_id) {
        	*prev = buf->next;
            
            if (buf->next == NULL) {
                broker->last_wait = prev;
            }

#if (NGX_KFK_STATUS)
        	if (corr_id != 0 && broker->kfk->cf->status) {
        		(void)ngx_atomic_fetch_add(ngx_kfk_wait_buf, -1);
        	}
#endif
        	return buf;

        } 
        /* this may lead uncorrect handle toppar_mov_to_valid function
          else if (id > corr_id) {

        	return NULL;
        }
        */
        prev = &buf->next;
        buf = buf->next;
    }

	return NULL;
}
