/*
 * ngx_http_kafka_upstream.h
 *
 *  Created on: 2014年9月18日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_UPSTREAM_H_
#define NGX_HTTP_KAFKA_UPSTREAM_H_

#include "ngx_http_kafka_common.h"

ngx_http_kafka_upstream_t* ngx_http_kafka_upstream_create(ngx_kfk_broker_t *broker);
void ngx_http_kafka_upstream_init(ngx_kfk_broker_t *broker);
void ngx_http_kafka_upstream_reconn(ngx_event_t *ev);
void ngx_http_kafka_upstream_enable_write_event(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);
void ngx_http_kafka_upstream_finalize(ngx_kfk_broker_t *broker,
	ngx_http_kafka_upstream_t *u);

typedef void (*ngx_http_kafka_upstream_handler_pt) (ngx_kfk_broker_t *broker, 
    ngx_http_kafka_upstream_t *u);

struct ngx_http_kafka_upstream_s {
    ngx_http_kafka_upstream_handler_pt     read_event_handler;
    ngx_http_kafka_upstream_handler_pt     write_event_handler;

    ngx_peer_connection_t            peer;

    ngx_chain_t                     *request_bufs;
    ngx_buf_t						 buffer;

    ngx_pool_t						*pool;
    ngx_output_chain_ctx_t           output;
    ngx_chain_writer_ctx_t           writer;

    ngx_err_t                        socket_errno;

    void            (*create_request)(ngx_kfk_broker_t *broker);

    void			(*send_request)(ngx_kfk_broker_t *broker, ngx_http_kafka_upstream_t *u);

    ngx_int_t       (*process_response)(ngx_kfk_broker_t *broker);

    ngx_http_upstream_resolved_t    *resolved;

    unsigned                         request_all_sent:1;
};




#endif /* NGX_HTTP_KAFKA_UPSTREAM_H_ */
