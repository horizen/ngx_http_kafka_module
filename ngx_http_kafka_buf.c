/*
 * ngx_http_kafka_buf.c
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */


#include "ngx_http_kafka_buf.h"


#define ngx_http_kafka_backup_msg(kfk, topic, msg)

static ngx_int_t ngx_http_kafka_init_buf(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t *buf);
static void ngx_http_kafka_free_buf(ngx_kfk_buf_t *buf, ngx_uint_t free_pool);
static void ngx_http_kafka_recycle_buf(ngx_pool_t *pool, ngx_kfk_buf_t *buf);

ngx_kfk_buf_t *
ngx_http_kafka_get_buf(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t **free)
{
	ngx_kfk_buf_t *buf;

	if (*free) {
		buf = *free;
		*free = buf->next;
		buf->next = NULL;
		buf->cnt = -1;
		buf->chain = NULL;
		buf->last_chain = &buf->chain;

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
			(void)ngx_atomic_fetch_add(ngx_kfk_free_buf, -1);
		}
#endif

		return buf;
	}

	if (kfk->allocated + 1 <= kfk->cf->free_bufs.num) {
		buf = ngx_pcalloc(kfk->pool, sizeof(ngx_kfk_buf_t));
		if (buf == NULL) {
			return NULL;
		}

		kfk->allocated++;

		/* set by calloc
		 * buf->chain = NULL
		 * buf->next = NULL
		 */

		if (ngx_http_kafka_init_buf(kfk, buf) != NGX_OK) {
			ngx_http_kafka_recycle_buf(kfk->pool, buf);
			return NULL;
		}

		return buf;
	}

    ngx_log_error(NGX_LOG_ERR, kfk->log, 0, "[kafka] no buf");

	return NULL;
}

ngx_chain_t *
ngx_http_kafka_get_free_chain(ngx_kfk_ctx_t *kfk, ngx_chain_t **free,
	ngx_buf_tag_t tag)
{
	ngx_pool_t	 *p = kfk->pool;
    ngx_chain_t  *cl;
    ngx_buf_t    *b;

    if (*free) {
        cl = *free;
        *free = cl->next;
        cl->next = NULL;

        b = cl->buf;

        b->pos = b->start;
        b->last = b->start;
        b->tag = tag;

#if (NGX_KFK_STATUS)
        if (kfk->cf->status) {
        	(void)ngx_atomic_fetch_add(ngx_kfk_free_chain, -1);
        }
#endif

        return cl;
    }

    cl = ngx_alloc_chain_link(p);
    if (cl == NULL) {
        return NULL;
    }

    b = ngx_calloc_buf(p);
    if (b == NULL) {
        ngx_free_chain(p, cl);
        return NULL;
    }

    b->temporary = 1;
    b->tag = tag;

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


ngx_int_t
ngx_http_kafka_init_chain(ngx_kfk_buf_t *buf, ngx_http_kafka_ctx_t *ctx)
{
	ngx_kfk_ctx_t	*kfk = buf->kfk;
	ngx_chain_t		*cl[4];
	ngx_uint_t		 i;
	int32_t			*size;

	/* we set req header when the first msg in */

	/* size */
	size = ngx_kfk_header_size_ptr(buf);
	*size = 0;

	/* topic len */
	ngx_kfk_header_topic_len(buf) = (int16_t)htons(ctx->topic->name.len);

	/* partition */
	ngx_kfk_header_partition(buf) = (int32_t)htonl(ctx->toppar->partition);

	/* msgset size */
	ngx_kfk_header_msgset_size(buf) = 0;

	for (i = 0; i < 4; i++) {
		cl[i] = ngx_http_kafka_get_free_chain(kfk, &kfk->free_chain,
				(ngx_buf_tag_t)&ngx_http_kafka_module);

		if (cl[i] == NULL) {
			if (i > 0) {
				cl[i - 1]->next = kfk->free_chain;
				kfk->free_chain = cl[0];
			}

#if (NGX_KFK_STATUS)
            if (kfk->cf->status) {
                (void)ngx_atomic_fetch_add(ngx_kfk_free_chain, i);
            }
#endif

			return NGX_ERROR;
		}

		if (i > 0) {
			cl[i - 1]->next = cl[i];
		}
	}

	/* header */
	cl[0]->buf->pos = buf->header.data;
	cl[0]->buf->last = cl[0]->buf->pos + buf->header.len;

	/* request header part1 */
	cl[1]->buf->pos = ngx_kfk_req_header_part1_ptr(buf);
	cl[1]->buf->last = cl[1]->buf->pos + NGX_KFK_REQ_HEADER_PART1_SIZE;

	/* request header topic string */
	cl[2]->buf->pos = ctx->topic->name.data;
	cl[2]->buf->last = cl[2]->buf->pos + ctx->topic->name.len;

	/* request header part2 */
	cl[3]->buf->pos = ngx_kfk_req_header_part2_ptr(buf);
	cl[3]->buf->last = cl[3]->buf->pos + NGX_KFK_REQ_HEADER_PART2_SIZE;

	buf->chain = cl[0];
	buf->last_chain = &cl[3]->next;

	/* we need decr sizeof(int23_t) for header.size self's size */
	*size -= 4;

	for (i = 0; i < 4; i++) {
		*size += ngx_buf_size(cl[i]->buf);
	}

	buf->cnt = 0;

	/* size, corr_id and req_header.msgset_size undone */

	return NGX_OK;
}

ngx_int_t
ngx_http_kafka_init_buf_pool(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t *buf)
{
	buf->pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, kfk->log);
	if (buf->pool == NULL) {

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
			(void)ngx_atomic_fetch_add(ngx_kfk_free_buf, 1);
		}
#endif

		buf->next = kfk->free;
		kfk->free = buf;
		return NGX_ERROR;
	}

	buf->pool->log = kfk->log;

	buf->no_pool = 0;

	return NGX_OK;
}

void
ngx_http_kafka_destroy_buf(ngx_kfk_buf_t *buf, ngx_int_t err)
{
	ngx_kfk_ctx_t	*kfk;
    ngx_str_t        topic, msg;
	ngx_chain_t		*cl;
	ngx_int_t		 i;

	kfk = buf->kfk;

	kfk->pending_msgs_cnt -= buf->cnt;

#if (NGX_KFK_STATUS)
	if (kfk->cf->status) {
		(void) ngx_atomic_fetch_add(ngx_kfk_pending_msgs, -buf->cnt);
	}
#endif

	if (err == 0) {
#if (NGX_KFK_DEBUG)
		debug(kfk->log, "[kafka] %ui messages send success", buf->cnt);
#endif

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
        	(void) ngx_atomic_fetch_add(ngx_kfk_succ_msgs, buf->cnt);
		}
#endif

	} else {
		ngx_log_error(NGX_LOG_ERR, kfk->log, 0,
					  "[kafka] %ui messages send fail: %s", buf->cnt,
					  ngx_kfk_strerr[err]);

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
		    (void) ngx_atomic_fetch_add(ngx_kfk_fail_msgs, buf->cnt);
		}
#endif

		i = 0;
		/* header */
		cl = buf->chain;

		/* request header part1 */
		cl = cl->next;

		/* topic start */
		cl = cl->next;
		topic.len = cl->buf->last - cl->buf->pos;
		topic.data = cl->buf->pos;

		/* request header part2 */
		cl = cl->next;

		/* message start */
		cl = cl->next;
		for (i = 0; i < buf->cnt; i++) {
            /* TODO */
		 	msg.data = cl->buf->pos;
			msg.len = cl->buf->last - cl->buf->pos;
			 
		    ngx_http_kafka_backup_msg(buf->kfk, &topic, &msg);
			 
			cl = cl->next;
		}
	}

	ngx_http_kafka_free_buf(buf, 0);
}

void
ngx_http_kafka_destroy_msg(ngx_kfk_ctx_t *kfk, ngx_str_t *topic,
	ngx_str_t *msg, ngx_int_t err)
{
	if (err == 0) {
#if (NGX_KFK_DEBUG)
		debug(kfk->log, "[kafka] 1 messages send success");
#endif

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
			(void) ngx_atomic_fetch_add(ngx_kfk_succ_msgs, 1);
		}
#endif

	} else {
		ngx_log_error(NGX_LOG_ERR, kfk->log, 0, "[kafka] 1 message send fail: %s",
					  ngx_kfk_strerr[err]);

#if (NGX_KFK_STATUS)
		if (kfk->cf->status) {
			(void) ngx_atomic_fetch_add(ngx_kfk_fail_msgs, 1);
		}
#endif

		/* TODO */
		ngx_http_kafka_backup_msg(kfk, &topic, &msg);
	}
}

static ngx_int_t
ngx_http_kafka_init_buf(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t *buf)
{
	ngx_http_kafka_main_conf_t	*cf;
	u_char 		*p;

	cf = kfk->cf;

	buf->kfk = kfk;

	buf->pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, kfk->log);
	if (buf->pool == NULL) {
		return NGX_ERROR;
	}

	buf->pool->log = kfk->log;

	buf->last_chain = &buf->chain;

	/* header */
	buf->header.data = buf->data;
	buf->header.len = NGX_KFK_HEADER_SIZE + cf->cli.len;

	p = buf->data;

	/* size(caculate later)*/
	p += sizeof(int32_t);

	/* api key */
	ngx_kfk_header_api_key(buf) = (int16_t)htons(NGX_KFK_PRODUCE_REQ);

	/* api_ver = 0 */
	/* corr id(calculate later) */

	/* client id */
	if(cf->cli.len > 0) {
		ngx_kfk_header_cli_len(buf) = (int16_t)htons(cf->cli.len);
		ngx_memcpy(ngx_kfk_header_cli_data_ptr(buf), cf->cli.data, cf->cli.len);

	} else {
		ngx_kfk_header_cli_len(buf) = (int16_t)htons(-1);
	}

	/* request header */

	/* acks */
	ngx_kfk_header_acks(buf) = (int16_t)htons((uint16_t)cf->acks);

	/* timeout */
	ngx_kfk_header_timeout(buf) = (int32_t)htonl(cf->timeout);

	/* topic cnt */
	ngx_kfk_header_topic_cnt(buf) = (int32_t)htonl(1);

	/* topic len(caculate later) */

	/* partition cnt */
	ngx_kfk_header_part_cnt(buf) = (int32_t)htonl(1);

	/* partition(caculate later) */

	/* msgset size(caculate later) */

	buf->no_pool = 0;
	buf->cnt = -1;

	/* size
	 * corr id,
	 * topic len,
	 * partition,
	 * msgset size uninitilized
	 */

	return NGX_OK;
}

static void
ngx_http_kafka_free_buf(ngx_kfk_buf_t *buf, ngx_uint_t free_pool)
{
	ngx_kfk_ctx_t	*kfk;

	kfk = buf->kfk;

	if (free_pool) {
		ngx_destroy_pool(buf->pool);
		buf->pool = NULL;
		buf->no_pool = 1;

	} else {
		ngx_reset_pool(buf->pool);
	}

	*buf->last_chain = kfk->free_chain;
	kfk->free_chain = buf->chain;

	buf->next = kfk->free;
	kfk->free = buf;

#if (NGX_KFK_STATUS)
	if (kfk->cf->status) {
        /* add 4 because request header have four chain link */
		(void)ngx_atomic_fetch_add(ngx_kfk_free_chain, buf->cnt + 4);
		(void)ngx_atomic_fetch_add(ngx_kfk_free_buf, 1);
	}
#endif
}

static void
ngx_http_kafka_recycle_buf(ngx_pool_t *pool, ngx_kfk_buf_t *buf)
{
#if (NGX_KFK_DEBUG)
	debug(pool->log, "[kafka] recycle buf back to pool");
#endif

	if ((u_char*)buf + sizeof(ngx_kfk_buf_t) == pool->d.last) {
		pool->d.last -= sizeof(ngx_kfk_buf_t);
	}
}
