/*
 * ngx_http_kafka_broker.h
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_BROKER_H_
#define NGX_HTTP_KAFKA_BROKER_H_

#include "ngx_http_kafka_common.h"

ngx_kfk_broker_t *ngx_http_kafka_new_broker(ngx_kfk_ctx_t *kfk,
	ngx_str_t *host, ngx_uint_t port, ngx_int_t id);
ngx_kfk_broker_t* ngx_http_kafka_broker_find(ngx_kfk_ctx_t *kfk, ngx_int_t id);
void ngx_http_kafka_broker_waitrsp_timeout_scan(ngx_kfk_broker_t *broker);
void ngx_http_kafka_destroy_broker(ngx_kfk_broker_t *broker);

#endif /* NGX_HTTP_KAFKA_BROKER_H_ */
