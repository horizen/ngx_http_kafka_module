/*
 * ngx_http_kafka_meta.c
 *
 *  Created on: 2014年9月1日
 *      Author: yw
 */


#include "ngx_http_kafka_common.h"
#include "ngx_http_kafka_broker.h"
#include "ngx_http_kafka_upstream.h"

static ngx_int_t ngx_http_kafka_init_meta_broker(ngx_kfk_ctx_t *kfk);
static ngx_int_t ngx_http_kafka_init_meta_req(ngx_kfk_ctx_t	*kfk);
static ngx_inline void ngx_http_kafka_meta_reinit_req();

typedef struct {
	int32_t 	cnt;
	u_char		data[0];
}ngx_kfk_array_t;

static ngx_http_upstream_resolved_t	resolved;

static ngx_buf_t ngx_meta_buf[2];

static ngx_chain_t ngx_meta_chain[2] = {
	{ &ngx_meta_buf[0], NULL },
	{ &ngx_meta_buf[1], NULL }
};

void
ngx_http_kafka_meta_force_refresh(ngx_event_t *ev)
{
	ngx_http_kafka_upstream_t	*u;
	ngx_kfk_broker_t			*broker;
	ngx_kfk_ctx_t				*kfk;

	kfk = ev->data;

#if (NGX_KFK_DEBUG)
    debug(kfk->log, "[kafka] start a metadata query");
#endif

	if (kfk->meta_broker == NULL) {

		if (ngx_http_kafka_init_meta_req(kfk) == NGX_ERROR) {
			ngx_log_error(NGX_LOG_ALERT, kfk->log, 0,
						  "[kafka] [%V:%ui] metadata query error while init metadata req",
						  &kfk->meta_broker->host, kfk->meta_broker->port);

            if (!ngx_exiting) {
			    ngx_add_timer(ev, kfk->cf->meta_refresh);
            }
			return;
		}

		if (ngx_http_kafka_init_meta_broker(kfk) == NGX_ERROR) {
			ngx_log_error(NGX_LOG_ALERT, kfk->log, 0,
						  "[kafka] [%V:%ui] metadata query error while init metadata broker",
						  &kfk->meta_broker->host, kfk->meta_broker->port);

            if (!ngx_exiting) {
			    ngx_add_timer(ev, kfk->cf->meta_refresh);
            }
			return;
		}

		ngx_http_kafka_upstream_init(kfk->meta_broker);

	} else {
		broker = kfk->meta_broker;
		if (broker->meta_query) {
#if (NGX_KFK_DEBUG)
			debug(kfk->log, "[kafka] [V:%ui] metadata query is on flight",
				  &broker->host, broker->port);
#endif
			return;
		}

        ngx_http_kafka_meta_reinit_req();

		broker->meta_query = 1;
       
		u = broker->upstream;
        if (broker->state == NGX_KFK_BROKER_DOWN) {
            ngx_http_kafka_upstream_reconn(&broker->reconn);

        } else {
			ngx_http_kafka_upstream_enable_write_event(broker, u);
		    u->send_request(broker, u);
        }
	}

    if (!ngx_exiting) {
	    ngx_add_timer(ev, kfk->cf->meta_refresh);
    }
}

static ngx_int_t
ngx_http_kafka_init_meta_broker(ngx_kfk_ctx_t *kfk)
{
	ngx_http_kafka_main_conf_t	*cf;
	ngx_kfk_broker_t			*broker;
	ngx_url_t					*url;
	ngx_uint_t	 				 i, n;

	cf = kfk->cf;
	url = cf->meta_brokers->elts;
	n = cf->meta_brokers->nelts;

#if (NGX_KFK_DEBUG)
    for (i = 0; i < n; i++) {
        debug(kfk->log, "host:%V, port:%ui, port_text:%V, default_port:%ui, "
              "naddrs:%ui", &url[i].host, url[i].port,
              &url[i].port_text, url[i].default_port, url[i].naddrs);
    }
#endif

	if (n > 1) {
		n = ngx_random() % n;
	} else {
        n = 0;
    }

	broker = ngx_http_kafka_new_broker(kfk, &url[n].host, url[n].port, -1);

	if (broker == NULL) {
		return NGX_ERROR;
	}

	broker->meta_query = 1;

	broker->upstream->resolved = &resolved;

	if (url[n].addrs && url[n].addrs[0].sockaddr) {
		resolved.sockaddr = url[n].addrs[0].sockaddr;
		resolved.socklen = url[n].addrs[0].socklen;
		resolved.host = url[n].host;

	} else {
		resolved.host = url[n].host;
		resolved.port = url[n].port;
	}

	kfk->meta_broker = broker;

	return NGX_OK;
}

static ngx_int_t
ngx_http_kafka_init_meta_req(ngx_kfk_ctx_t	*kfk)
{
	ngx_http_kafka_main_conf_t	*cf;
	ngx_kfk_buf_t		*buf;
	ngx_chain_t			*cl;
	ngx_uint_t			 i, n;
	u_char				*p, *q;
	size_t				 len;
	ngx_str_t			*topics;

	cf = kfk->cf;

	buf = &kfk->meta_buf;

	ngx_memzero(buf, sizeof(ngx_kfk_buf_t));

	buf->last_chain = &buf->chain;

	buf->kfk = kfk;

	/* header */

	ngx_kfk_header_api_key(buf) = (int16_t)htons(NGX_KFK_META_REQ);
	/*
	 * api_ver = 0;
	 * corr_id = 0;
	 */
	if (cf->cli.len > 0) {
		ngx_kfk_header_cli_len(buf) = (int16_t)htons(cf->cli.len);
		ngx_memcpy(ngx_kfk_header_cli_data_ptr(buf), cf->cli.data, cf->cli.len);

	} else {
		ngx_kfk_header_cli_len(buf) = (int16_t)htons(-1);
	}

	cl = &ngx_meta_chain[0];
    cl->buf->temporary = 1;
    cl->buf->tag = (ngx_buf_tag_t)&ngx_http_kafka_module;
	cl->buf->pos = buf->data;
	cl->buf->last = cl->buf->pos + NGX_KFK_HEADER_SIZE + cf->cli.len;
    cl->buf->start = cl->buf->pos;
    cl->buf->end = cl->buf->last;

	/* body */
	topics = cf->topics->elts;
	n = cf->topics->nelts;

	for (i = 0, len = 0; i < n; i++) {
		len += topics[i].len;
	}

	len += sizeof(int32_t) + n * sizeof(int16_t);

	p = ngx_palloc(kfk->pool, len);
	if (p == NULL) {
		return NGX_ERROR;
	}
    q = p;

    /* topic cnt */
	*(int32_t *)q = (int32_t)htonl(n);
    q += sizeof(int32_t);
    
	for (i = 0; i < n; i++) {
        *(int16_t *)q = (int16_t)htons(topics[i].len);
        q += sizeof(int16_t);
		q = ngx_cpymem(q, topics[i].data, topics[i].len);
	}

    cl->next = &ngx_meta_chain[1];

	cl = &ngx_meta_chain[1];

    cl->buf->temporary = 1;
    cl->buf->tag = (ngx_buf_tag_t)&ngx_http_kafka_module;
	cl->buf->pos = p;
	cl->buf->last = p + len;
    cl->buf->start = cl->buf->pos;
    cl->buf->end = cl->buf->last;

    ngx_kfk_header_size(buf) = (int32_t)htonl(NGX_KFK_HEADER_SIZE - sizeof(int32_t)
							 	 	  + cf->cli.len + len);

	buf->chain = &ngx_meta_chain[0];
	buf->last_chain = &ngx_meta_chain[1].next;

#if (NGX_KFK_DEBUG)
	{
    u_char temp[512];
	cl = buf->chain;
	while (cl) {
        for (p = cl->buf->pos, q = temp; p < cl->buf->last; p++) {
            q = ngx_snprintf(q, sizeof(temp) - (q - temp), "%Xd ", *p);
        }

		debug(kfk->log, "[kafka] meta req chain: %*s", q - temp, temp);

		cl = cl->next;
	}
	}
#endif
	return NGX_OK;
}

static ngx_inline void
ngx_http_kafka_meta_reinit_req()
{
    ngx_chain_t *cl;

	cl = &ngx_meta_chain[0];
	cl->buf->pos = cl->buf->start;
	cl->buf->last = cl->buf->end;

	cl = &ngx_meta_chain[1];
	cl->buf->pos = cl->buf->start;
	cl->buf->last = cl->buf->end;
}
