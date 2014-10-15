
#include "ngx_http_kafka_broker.h"
#include "ngx_http_kafka_buf.h"
#include "ngx_http_kafka_upstream.h"


static void ngx_http_kafka_upstream_resolve(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);
static void ngx_http_kafka_upstream_resolve_handler(ngx_resolver_ctx_t *ctx);
static void ngx_http_kafka_upstream_connect(ngx_kfk_broker_t *broker,
    ngx_http_kafka_upstream_t *u);
static void ngx_http_kafka_upstream_connected_handler(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);
static void ngx_http_kafka_upstream_handler(ngx_event_t *ev);
static void ngx_http_kafka_upstream_send_request(ngx_kfk_broker_t *broker,
    ngx_http_kafka_upstream_t *u);
static void ngx_http_kafka_upstream_process_response(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);
static ngx_int_t ngx_http_kafka_upstream_test_connect(ngx_connection_t *c);
static void ngx_http_kafka_upstream_error(ngx_kfk_broker_t *broker,
    ngx_http_kafka_upstream_t *u, ngx_int_t rc);
static ngx_int_t ngx_http_kafka_upstream_peer_get(ngx_peer_connection_t *pc,
	void *data);
static void ngx_http_kafka_upstream_dummy_handler(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);



ngx_http_kafka_upstream_t*
ngx_http_kafka_upstream_create(ngx_kfk_broker_t *broker)
{
    ngx_http_kafka_upstream_t  *u;

    u = broker->upstream;

    if (u == NULL) {
    	u = ngx_pcalloc(broker->pool, sizeof(ngx_http_kafka_upstream_t));

    } else {
    	ngx_memzero(u, sizeof(ngx_http_kafka_upstream_t));
    }

    if (u == NULL) {
        return NULL;
    }

    u->peer.log = broker->log;
    u->peer.log_error = NGX_ERROR_ERR;

    u->send_request = ngx_http_kafka_upstream_send_request;

    return u;
}

void
ngx_http_kafka_upstream_init(ngx_kfk_broker_t *broker)
{
	ngx_http_kafka_upstream_t *u;

	u = broker->upstream;

	u->output.tag = (ngx_buf_tag_t) &ngx_http_kafka_module;
    u->output.output_filter = ngx_chain_writer;
    u->output.filter_ctx = &u->writer;

    ngx_http_kafka_upstream_resolve(broker, u);
}

void
ngx_http_kafka_upstream_reconn(ngx_event_t *ev)
{
	ngx_kfk_broker_t *broker;

	broker = ev->data;

#if (NGX_KFK_DEBUG)
	debug(broker->log, "[kafka] [%V:%ui] upstream reconnect", &broker->host, broker->port);
#endif

	ngx_http_kafka_upstream_resolve(broker, broker->upstream);
}

void
ngx_http_kafka_upstream_enable_write_event(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
	ngx_connection_t	*c;

	c = u->peer.connection;

    u->write_event_handler = ngx_http_kafka_upstream_send_request;

	if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
		u->socket_errno = ngx_socket_errno;
    	ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_SOCKET_ERROR);
	}
}

static void
ngx_http_kafka_upstream_resolve(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
	ngx_http_kafka_main_conf_t		*cf;
	ngx_str_t						*host;
    ngx_resolver_ctx_t              *ctx, temp;

	host = &broker->host;

    if (u->resolved == NULL) {
    	u->resolved = ngx_pcalloc(broker->pool,
    							  sizeof(ngx_http_upstream_resolved_t));
    }

    if (u->resolved == NULL) {
    	ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_MEMORY);
    	return;
    }

    if (u->resolved->sockaddr) {
    	ngx_http_kafka_upstream_connect(broker, u);
    	return;
    }

	u->resolved->host = *host;
	u->resolved->port = (in_port_t)broker->port;

	cf = broker->kfk->cf;

	temp.name = *host;

	ctx = ngx_resolve_start(cf->resolver, &temp);
	if (ctx == NULL) {
		ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_MEMORY);
		return;
	}

	if (ctx == NGX_NO_RESOLVER) {
		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] no resolver defined to resolve %V", host);

		ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_RESOLVER);
		return;
	}

	ctx->name = *host;
	ctx->handler = ngx_http_kafka_upstream_resolve_handler;
	ctx->data = broker;
	ctx->timeout = cf->resolver_timeout;

	u->resolved->ctx = ctx;

	if (ngx_resolve_name(ctx) != NGX_OK) {
		u->resolved->ctx = NULL;
		ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_RESOLVER_ERROR);
	}
}

static void
ngx_http_kafka_upstream_resolve_handler(ngx_resolver_ctx_t *ctx)
{
    ngx_http_kafka_upstream_t     	*u;
    ngx_http_upstream_resolved_t  	*ur;
    ngx_kfk_broker_t			  	*broker;
    ngx_int_t					   	 i;
    u_char							*p;
    size_t							 len;
    struct sockaddr					*sockaddr;
    socklen_t				 		 socklen;

    broker = ctx->data;

    u = broker->upstream;
    ur = u->resolved;

    if (ctx->state) {
        ngx_log_error(NGX_LOG_ERR, broker->log, 0,
                      "[kafka] %V could not be resolved (%i: %s)",
                      &ctx->name, ctx->state,
                      ngx_resolver_strerror(ctx->state));

        ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_RESOLVER_ERROR);
        return;
    }

    ur->naddrs = ctx->naddrs;
    ur->addrs = ctx->addrs;

#if (NGX_DEBUG)
    {
    u_char      text[NGX_SOCKADDR_STRLEN];
    ngx_str_t   addr;
    ngx_uint_t  i;

    addr.data = text;

    for (i = 0; i < ctx->naddrs; i++) {
        addr.len = ngx_sock_ntop(ur->addrs[i].sockaddr, ur->addrs[i].socklen,
                                 text, NGX_SOCKADDR_STRLEN, 0);

        debug(broker->log, "[kafka] %V name was resolved to %V", &ctx->name, &addr);
    }
    }
#endif

    if (ur->naddrs <= 0) {
    	ngx_resolve_name_done(ctx);
    	ur->ctx = NULL;

        ngx_log_error(NGX_LOG_ERR, broker->log, 0, "[kafka] %V can not be resolved",
        			  &ctx->name);

        ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_RESOLVER_ERROR);
        return;
    }

    if (ur->naddrs == 1) {
    	i = 0;
    } else {
    	i = ngx_random() % ur->naddrs;
    }

    socklen = ur->addrs[i].socklen;
    sockaddr = ngx_palloc(broker->pool, socklen);
    if (sockaddr == NULL) {
    	goto nomem;
    }

    ngx_memcpy(sockaddr, ur->addrs[i].sockaddr, socklen);

    switch (sockaddr->sa_family) {
    #if (NGX_HAVE_INET6)
        case AF_INET6:
            ((struct sockaddr_in6 *) sockaddr)->sin6_port = htons(ur->port);
            break;
    #endif
        default: /* AF_INET */
            ((struct sockaddr_in *) sockaddr)->sin_port = htons(ur->port);
    }

    ur->sockaddr = sockaddr;
    ur->socklen = socklen;
    ur->naddrs = 1;

    p = ngx_palloc(broker->pool, NGX_SOCKADDR_STRLEN);
    if (p == NULL) {
        goto nomem;
    }

    len = ngx_sock_ntop(sockaddr, socklen, p, NGX_SOCKADDR_STRLEN, 1);

    ur->host.data = p;
    ur->host.len = len;

    ngx_resolve_name_done(ctx);
    ur->ctx = NULL;

    ngx_http_kafka_upstream_connect(broker, broker->upstream);
    return;

nomem:
	ngx_resolve_name_done(ctx);
    ur->ctx = NULL;

    ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_MEMORY);
}

static void
ngx_http_kafka_upstream_connect(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
    ngx_int_t          rc;
    ngx_connection_t  *c;

    if (u->resolved->sockaddr) {
    	u->peer.sockaddr = u->resolved->sockaddr;
    	u->peer.socklen = u->resolved->socklen;
    	u->peer.name = &u->resolved->host;

    } else {
    	ngx_log_error(NGX_LOG_ERR, broker->log, 0, "[kafka] unexpected resolve error");

    	ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_RESOLVER_ERROR);
        return;
    }


    u->peer.get = ngx_http_kafka_upstream_peer_get;
    rc = ngx_event_connect_peer(&u->peer);

#if (NGX_KFK_DEBUG)
    debug(broker->log, "[kafka] [%V:%ui] upstream connect, rc: %i",
    	  &broker->host, broker->port, rc);
#endif

    if (rc == NGX_ERROR || rc == NGX_DECLINED) {
    	u->socket_errno = ngx_socket_errno;
        ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_SOCKET_ERROR);
        return;
    }

    /* rc == NGX_OK || rc == NGX_AGAIN || rc == NGX_DONE */

    c = u->peer.connection;

    c->data = broker;

    c->write->handler = ngx_http_kafka_upstream_handler;
    c->read->handler = ngx_http_kafka_upstream_handler;

    u->write_event_handler = ngx_http_kafka_upstream_connected_handler;
    u->read_event_handler = ngx_http_kafka_upstream_connected_handler;

    c->log = broker->log;
    c->read->log = c->log;
    c->write->log = c->log;

    if (rc == NGX_AGAIN) {
        ngx_add_timer(c->write, broker->kfk->cf->timeout);
        return;
    }

    ngx_http_kafka_upstream_connected_handler(broker, u);
}

static void
ngx_http_kafka_upstream_handler(ngx_event_t *ev)
{
    ngx_connection_t     *c;
    ngx_kfk_broker_t   *broker;
    ngx_http_kafka_upstream_t  *u;

    c = ev->data;
    broker = c->data;

    u = broker->upstream;

#if (NGX_KFK_DEBUG)
    debug(c->log, "[kafka] [%V:%ui] upstream event, write: %ui, ready: %ui",
    	  &broker->host, broker->port, ev->write, ev->ready);
#endif

    if (ev->write) {
        u->write_event_handler(broker, u);

    } else {
        u->read_event_handler(broker, u);
    }
}

static void
ngx_http_kafka_upstream_connected_handler(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
	ngx_connection_t	*c;
    ngx_int_t            rc;

	c = u->peer.connection;

	if (c->write->timedout) {
		ngx_log_error(NGX_LOG_ERR, broker->log, 0,
					  "[kafka] [%V:%ui] upstream connect timeout",
					  &broker->host, broker->port);

		ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_SOCKET_TIMEOUT);
		return;
	}

	if (c->write->timer_set) {
		ngx_del_timer(c->write);
	}

	if (broker->reconn.timer_set) {
		ngx_del_timer(&broker->reconn);
	}

	rc = ngx_http_kafka_upstream_test_connect(c);
    if (rc != NGX_OK) {
    	u->socket_errno = rc;
    	ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_SOCKET_ERROR);
    	return;
	}

#if (NGX_KFK_DEBUG)
    debug(broker->log, "[kafka] [%V:%ui] upstream connected",
          &broker->host, broker->port);
#endif

	broker->rsp_size = 0;
	broker->state = NGX_KFK_BROKER_UP;

    u->write_event_handler = ngx_http_kafka_upstream_send_request;
    u->read_event_handler = ngx_http_kafka_upstream_process_response;

    /* init or reinit the ngx_output_chain() and ngx_chain_writer() contexts */
    if (u->pool == NULL) {
        u->pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, broker->log);
        if (u->pool == NULL) {
        	ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_MEMORY);
        }

        u->pool->log = broker->log;
    }

	u->output.pool = u->pool;
	u->writer.pool = u->pool;

    u->writer.out = NULL;
    u->writer.last = &u->writer.out;
    u->writer.connection = c;
    u->writer.limit = 0;

    u->request_all_sent = 1;

    ngx_http_kafka_upstream_send_request(broker, u);
}

static void
ngx_http_kafka_upstream_send_request(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
    ngx_int_t          rc;
    ngx_connection_t  *c;

    c = u->peer.connection;

#if (NGX_KFK_DEBUG)
    debug(broker->log, "[kafka] [%V:%ui] upstream send request",
    	  &broker->host, broker->port);
#endif

    c->log->action = "sending request to kafka";

    do {
		u->create_request(broker);

#if (NGX_KFK_DEBUG)
        debug(broker->log, "[kafka] [%V:%ui], meta query: %ud, "
        	  "request bufs: %p, all_send: %ud", &broker->host,
        	  broker->port, broker->meta_query, u->request_bufs,
        	  u->request_all_sent);
#endif

		if (u->request_all_sent) {
			u->request_all_sent = 0;

			/* we must delete event because level event may lead to hot spin */
			if (u->request_bufs == NULL) {

#if (NGX_KFK_DEBUG)
                debug(broker->log, "[kafka] [%V:%ui] idle, to dummy handler",
                      &broker->host, broker->port);
#endif

                u->request_all_sent = 1;

				if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
					u->socket_errno = ngx_socket_errno;
					ngx_http_kafka_upstream_error(broker, u,
												  NGX_KAFKA_SOCKET_ERROR);
				}

				u->write_event_handler = ngx_http_kafka_upstream_dummy_handler;

				return;
			}
		}

		rc = ngx_output_chain(&u->output, u->request_bufs);

		u->request_bufs = NULL;

		if (rc == NGX_ERROR) {
			u->socket_errno = ngx_socket_errno;
			ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_SOCKET_ERROR);
			return;
		}

		if (rc == NGX_AGAIN) {

			if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
				u->socket_errno = ngx_socket_errno;
				ngx_http_kafka_upstream_error(broker, u,
											  NGX_KAFKA_SOCKET_ERROR);
				return;
			}

			return;
		}

		/* NGX_OK */
		u->request_all_sent = 1;

		if (broker == broker->kfk->meta_broker) {
			break;
		}

    } while (rc == NGX_OK);

    if (c->read->ready) {
        ngx_http_kafka_upstream_process_response(broker, u);
    }
}

static void
ngx_http_kafka_upstream_process_response(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
	ngx_http_kafka_main_conf_t	*cf;
    ssize_t            n;
    ngx_int_t          rc;
    ngx_connection_t  *c;

    cf = broker->kfk->cf;

    c = u->peer.connection;

    c->log->action = "reading response from kafka";

    if (u->buffer.start == NULL) {
        u->buffer.start = ngx_palloc(broker->pool, cf->rsp_buffer_size);
        if (u->buffer.start == NULL) {
            ngx_http_kafka_upstream_error(broker, u, NGX_KAFKA_NO_MEMORY);
            return;
        }

        u->buffer.pos = u->buffer.start;
        u->buffer.last = u->buffer.start;
        u->buffer.end = u->buffer.start + cf->rsp_buffer_size;
        u->buffer.temporary = 1;

        u->buffer.tag = u->output.tag;
    }

    for ( ;; ) {
        n = c->recv(c, u->buffer.last, u->buffer.end - u->buffer.last);

#if (NGX_KFK_DEBUG)
        debug(broker->log, "[kafka] [%V:%ui] response rc: %i", 
              &broker->host, broker->port, n);
#endif

        if (n == NGX_AGAIN) {

            if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            	u->socket_errno = ngx_socket_errno;
                ngx_http_kafka_upstream_error(broker, u,
                                              NGX_KAFKA_SOCKET_ERROR);
                return;
            }

            return;
        }

        if (n == 0) {
            ngx_http_kafka_upstream_error(broker, u,
                                          NGX_KAFKA_SOCKET_CLOSED);
            return;
        }

        if (n == NGX_ERROR) {
        	u->socket_errno = ngx_socket_errno;
            ngx_http_kafka_upstream_error(broker, u,
                                          NGX_KAFKA_SOCKET_ERROR);
            return;
        }
       
        u->buffer.last += n;

        rc = u->process_response(broker);

        if (rc == NGX_AGAIN) {

            if (u->buffer.last == u->buffer.end) {
                ngx_log_error(NGX_LOG_ERR, c->log, 0,
                              "[kafka] [%V:%ui] sent too big response",
                              &broker->host, broker->port);

                ngx_http_kafka_upstream_error(broker, u,
                                              NGX_KAFKA_NO_BUFFER);
                return;
            }
        }

        if (rc != NGX_OK) {
            ngx_http_kafka_upstream_error(broker, u, rc);
            return;
        }
    }
}


static ngx_int_t
ngx_http_kafka_upstream_test_connect(ngx_connection_t *c)
{
    int        err;
    socklen_t  len;

#if (NGX_HAVE_KQUEUE)

    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT)  {
        if (c->write->pending_eof || c->read->pending_eof) {
            if (c->write->pending_eof) {
                err = c->write->kq_errno;

            } else {
                err = c->read->kq_errno;
            }

            c->log->action = "connecting to upstream";
            (void) ngx_connection_error(c, err,
                                    "kevent() reported that connect() failed");
            return err;
        }

    } else
#endif
    {
        err = 0;
        len = sizeof(int);

        /*
         * BSDs and Linux return 0 and set a pending error in err
         * Solaris returns -1 and sets errno
         */

        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
            == -1)
        {
            err = ngx_socket_errno;
        }

        if (err) {
            c->log->action = "connecting to upstream";
            (void) ngx_connection_error(c, err, "connect() failed");
            return err;
        }
    }

    return NGX_OK;
}

static void
ngx_http_kafka_upstream_error(ngx_kfk_broker_t *broker,
    ngx_http_kafka_upstream_t *u, ngx_int_t rc)
{
	ngx_kfk_ctx_t		*kfk;
	ngx_kfk_buf_t		*buf, *next;
    u_char           	*p, errstr[NGX_MAX_ERROR_STR];
	ngx_int_t			 n = 0;

	if (rc == NGX_KAFKA_NO_MEMORY ) {

		ngx_log_error(NGX_LOG_ALERT, broker->log, 0, "[kafka] [%V:%ui] %s",
					  &broker->host, broker->port, ngx_kfk_strerr[rc]);

	} else {
        if (u->socket_errno) {
            p = ngx_strerror(u->socket_errno, errstr, sizeof(errstr));

            ngx_log_error(NGX_LOG_ERR, broker->log, 0, "[kafka] [%V:%ui] %*s",
            			  &broker->host, broker->port, p - errstr, errstr);

        } else {
    		ngx_log_error(NGX_LOG_ALERT, broker->log, 0, "[kafka] [%V:%ui] %s",
    					  &broker->host, broker->port, ngx_kfk_strerr[rc]);
        }
    }

	u->socket_errno = 0;

	kfk = broker->kfk;

	/* metadata broker, reconnect whatever */
	if (broker == kfk->meta_broker) {

		ngx_log_error(NGX_LOG_ERR, broker->log, 0, "[kafka] metadata query "
					  "error while handle upstream connection");

	    broker->meta_query = 0;
	    broker->wait = NULL;
	    broker->last_wait = &broker->wait;

	    ngx_http_kafka_upstream_finalize(broker, u);
	    return;
	}

	/* destroy all wait buf */
	buf = broker->wait;
	while (buf != NULL) {
		next = buf->next;
		ngx_http_kafka_destroy_buf(buf, rc);
        buf = next;
        n++;
    }

#if (NGX_KFK_STATUS)
	(void)ngx_atomic_fetch_add(ngx_kfk_wait_buf, -n);
#endif

	broker->wait = NULL;
	broker->last_wait = &broker->wait;

	/* to here, connection may encount some unexpected error, close it and reconnect */

    ngx_http_kafka_upstream_finalize(broker, u);
    
    if (!ngx_exiting) {
        ngx_add_timer(&broker->reconn, kfk->cf->reconn_backoff);
    }

    if (rc == NGX_KAFKA_SOCKET_ERROR
    	|| rc == NGX_KAFKA_RESOLVER_ERROR
    	|| rc == NGX_KAFKA_SOCKET_TIMEOUT
    	|| rc == NGX_KAFKA_SOCKET_CLOSED)
    {
        if (kfk->meta_broker->state == NGX_KFK_BROKER_UP) {
            /* when kafka down, zookeeper may not be updated immediately */
            ngx_add_timer(&kfk->meta_ev, 2000);
        }
    }
}

void
ngx_http_kafka_upstream_finalize(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
#if (NGX_KFK_DEBUG)
    debug(broker->log, "[kafka] [%V:%ui] finalize kafka upstream",
          &broker->host, broker->port);
#endif

    if (u->resolved && u->resolved->ctx) {
        ngx_resolve_name_done(u->resolved->ctx);
        u->resolved->ctx = NULL;
    }

    if (u->peer.free && u->peer.sockaddr) {
        u->peer.free(&u->peer, u->peer.data, 0);
        u->peer.sockaddr = NULL;
    }

    if (u->peer.connection) {

#if (NGX_KFK_DEBUG)
        debug(broker->log, "[kafka] [%V:%ui] close upstream connection: %d",
        	  &broker->host, broker->port, u->peer.connection->fd);
#endif

        if (u->peer.connection->pool) {
            ngx_destroy_pool(u->peer.connection->pool);
        }

        ngx_close_connection(u->peer.connection);
    }

    u->peer.connection = NULL;

    if (u->buffer.start) {
    	u->buffer.pos = u->buffer.start;
    	u->buffer.last = u->buffer.pos;
    }

    if (u->pool) {
    	ngx_destroy_pool(u->pool);
    	u->pool = NULL;
    }

    broker->state = NGX_KFK_BROKER_DOWN;
}

static ngx_int_t
ngx_http_kafka_upstream_peer_get(ngx_peer_connection_t *pc, void *data)
{
	/* empty */
	return NGX_OK;
}

static void
ngx_http_kafka_upstream_dummy_handler(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u)
{
#if (NGX_KFK_DEBUG)
    debug(broker->log, "[kafka] [%V:%ui] upstream dummy handler",
          &broker->host, broker->port);
#endif
}
