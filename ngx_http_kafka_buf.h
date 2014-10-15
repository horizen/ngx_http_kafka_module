/*
 * ngx_http_kafka_buf.h
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_BUF_H_
#define NGX_HTTP_KAFKA_BUF_H_

#include "ngx_http_kafka_common.h"


ngx_kfk_buf_t *ngx_http_kafka_get_buf(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t **free);
ngx_chain_t *ngx_http_kafka_get_free_chain(ngx_kfk_ctx_t *kfk, ngx_chain_t **free,
	ngx_buf_tag_t tag);
ngx_int_t ngx_http_kafka_init_chain(ngx_kfk_buf_t *buf, ngx_http_kafka_ctx_t *ctx);
ngx_int_t ngx_http_kafka_init_buf_pool(ngx_kfk_ctx_t *kfk, ngx_kfk_buf_t *buf);
void ngx_http_kafka_destroy_buf(ngx_kfk_buf_t *buf, ngx_int_t err);
void ngx_http_kafka_destroy_msg(ngx_kfk_ctx_t *kfk, ngx_str_t *topic,
	ngx_str_t *msg, ngx_int_t err);

#endif /* NGX_HTTP_KAFKA_BUF_H_ */
