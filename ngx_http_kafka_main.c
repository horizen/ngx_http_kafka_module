/*
 * ngx_http_kafka_util.c
 *
 *  Created on: 2014年8月11日
 *      Author: yw
 */

#include "ngx_http_kafka_common.h"
#include "ngx_http_kafka_upstream.h"
#include "ngx_http_kafka_topic.h"
#include "ngx_http_kafka_buf.h"


static ngx_int_t ngx_http_kafka_script_run(ngx_http_request_t *r,
    ngx_pool_t *pool, ngx_str_t *msg, void *code_lengths, 
    size_t len, void *code_values);


ngx_int_t
ngx_http_kafka_push(ngx_http_request_t *r)
{
	ngx_http_kafka_loc_conf_t	*klcf;
	ngx_http_kafka_ctx_t		*ctx;
	ngx_kfk_toppar_t			*toppar;
	ngx_kfk_buf_t				*buf;
	ngx_str_t				     msg;
    ngx_int_t                    rc;

	klcf = ngx_http_get_module_loc_conf(r, ngx_http_kafka_module);
    ctx = ngx_http_get_module_ctx(r, ngx_http_kafka_module);

	toppar = ctx->toppar;

	buf = toppar->free;

	if (buf == NULL) {
		buf = ngx_http_kafka_get_buf(ngx_kfk, &ngx_kfk->free);

		if (buf == NULL) {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
		}

		if (buf->no_pool) {
			if (ngx_http_kafka_init_buf_pool(ngx_kfk, buf) != NGX_OK) {
                return NGX_HTTP_SERVICE_UNAVAILABLE;
			}
		}

		toppar->free = buf;
	}

	rc = ngx_http_kafka_script_run(r, buf->pool, &msg, klcf->msg_lengths->elts,
                                   0, klcf->msg_values->elts);

    if (rc == NGX_ERROR) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (rc == NGX_DECLINED) {
        return NGX_HTTP_BAD_REQUEST;
	}

	if (rc == NGX_BUSY) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
	}

    /* NGX_OK */

	if (buf->cnt == -1) {
		if (ngx_http_kafka_init_chain(buf, ctx) != NGX_OK) {
			ngx_http_kafka_recycle_msg(buf->pool, &msg);

            return NGX_HTTP_SERVICE_UNAVAILABLE;
		}
	}

	ctx->msg = &msg;

	return ngx_http_kafka_enq_buf(r, ctx);
}

ngx_int_t
ngx_http_kafka_enq_buf(ngx_http_request_t *r, ngx_http_kafka_ctx_t *ctx)
{
	ngx_http_kafka_main_conf_t	*kmcf;
	ngx_http_kafka_upstream_t	*u;
	ngx_kfk_broker_t			*broker;
	ngx_kfk_buf_t 				*buf;
	ngx_chain_t					*cl;
	ngx_str_t   				*msg;

	buf = ctx->toppar->free;
	msg = ctx->msg;

	cl = ngx_http_kafka_get_free_chain(buf->kfk, &buf->kfk->free_chain,
									   (ngx_buf_tag_t)&ngx_http_kafka_module);

	if (cl == NULL) {
		ngx_http_kafka_recycle_msg(buf->pool, msg);

        return NGX_HTTP_SERVICE_UNAVAILABLE;
	}

	/* TODO compression */

	cl->buf->pos = msg->data;
	cl->buf->last = cl->buf->pos + msg->len;

	/* msgset size */
	ngx_kfk_header_msgset_size(buf) += ngx_buf_size(cl->buf);

	*buf->last_chain = cl;
	buf->last_chain = &cl->next;

	buf->cnt++;
	buf->kfk->pending_msgs_cnt++;

	kmcf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);

#if (NGX_KFK_STATUS)
	if (kmcf->status) {
		(void)ngx_atomic_fetch_add(ngx_kfk_pending_msgs, 1);
	}
#endif

	if (buf->cnt >= kmcf->batch_size) {
		ngx_http_kafka_toppar_free_to_out(ctx->toppar);

		broker = ctx->toppar->broker;
		if (broker != NULL && broker->state == NGX_KFK_BROKER_UP) {
			u = broker->upstream;

			ngx_http_kafka_upstream_enable_write_event(broker, u);

			u->send_request(broker, u);
		}

	} else if (buf->cnt == 1) {
		/* add a timer for each partition to achieve delayed sending */
		ngx_add_timer(&ctx->toppar->send_timer, kmcf->linger);
	}

    return ngx_http_special_response_handler(r, NGX_HTTP_OK);
}

static ngx_int_t
ngx_http_kafka_script_run(ngx_http_request_t *r, ngx_pool_t *pool,
    ngx_str_t *msg, void *code_lengths, size_t len, void *code_values)
{
	ngx_http_kafka_main_conf_t	 *kmcf;
    ngx_http_script_code_pt       code;
    ngx_http_script_len_code_pt   lcode;
    ngx_http_script_engine_t      e;
    ngx_http_core_main_conf_t    *cmcf;
    ngx_uint_t                    i;
    u_char                       *p;
    int32_t                      *crc;

    cmcf = ngx_http_get_module_main_conf(r, ngx_http_core_module);

    for (i = 0; i < cmcf->variables.nelts; i++) {
        if (r->variables[i].no_cacheable) {
            r->variables[i].valid = 0;
            r->variables[i].not_found = 0;
        }
    }

    ngx_memzero(&e, sizeof(ngx_http_script_engine_t));

    e.ip = code_lengths;
    e.request = r;
    e.flushed = 1;

    while (*(uintptr_t *) e.ip) {
        lcode = *(ngx_http_script_len_code_pt *) e.ip;
        len += lcode(&e);
    }

    if (len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "[kafka] message is null, we ignore it");
        return NGX_DECLINED;
    }

    kmcf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);

    if (len > kmcf->msg_max_size) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "[kafka] message is too large, we ignore it");
        return NGX_DECLINED;
    }

    p = ngx_pnalloc(pool, NGX_KFK_MSG_HEADER_SIZE + len);
    if (p == NULL) {
        return NGX_BUSY;
    }

    msg->data = p;
    msg->len = NGX_KFK_MSG_HEADER_SIZE + len;

    /* offset */
    *(int64_t *)p = 0;
    p += sizeof(int64_t);

    /* msg size */
    *(int32_t *)p = (int32_t)htonl(14 + len);
    p += sizeof(int32_t);

    /* crc(later calculate) */
    crc = (int32_t *)p;
    p += sizeof(int32_t);

    /* magic */
    *(int8_t *)p = 0;
    p += sizeof(int8_t);
    /* attr */
    *(int8_t *)p = 0;
    p += sizeof(int8_t);

    /* key len */
	*(int32_t *)p = (int32_t)htonl(-1);
    p += sizeof(int32_t);

	/* value len */
	*(int32_t *)p = (int32_t)htonl(len);
    p += sizeof(int32_t);

    e.ip = code_values;
    e.pos = p;

    while (*(uintptr_t *) e.ip) {
        code = *(ngx_http_script_code_pt *) e.ip;
        code((ngx_http_script_engine_t *) &e);
    }

    if (e.pos == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "[kafka] some unexpected things happened when get request body");
        
		ngx_http_kafka_recycle_msg(pool, msg);

        return NGX_ERROR;
    }

    *crc = (int32_t)htonl(ngx_crc32_long((u_char*)(crc + 1), 10 + len));

    return NGX_OK;
}

