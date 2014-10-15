/*
 * ngx_http_kafka_topic.h
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_TOPIC_H_
#define NGX_HTTP_KAFKA_TOPIC_H_

#include "ngx_http_kafka_common.h"

void ngx_http_kafka_toppar_free_to_out(ngx_kfk_toppar_t *toppar);
ngx_kfk_toppar_t* ngx_http_kafka_toppar_get_valid(ngx_kfk_topic_t *topic,
	ngx_uint_t partition);
ngx_kfk_topic_t* ngx_http_kafka_topic_find(ngx_kfk_ctx_t *kfk,
	u_char *data, size_t len);
void ngx_http_kafka_topic_update(ngx_kfk_ctx_t *kfk, ngx_kfk_top_meta_t *tm);
void ngx_http_kafka_destroy_topic(ngx_kfk_topic_t *topic, ngx_int_t err);


static ngx_inline ngx_kfk_toppar_t*
ngx_http_kafka_toppar_get(ngx_kfk_topic_t *topic, ngx_uint_t partition)
{
	ngx_kfk_toppar_t	*toppars;

	toppars = topic->toppars->elts;

	if (partition >= topic->toppars->nelts) {
		return NULL;
	}

	return &toppars[partition];
}

#endif /* NGX_HTTP_KAFKA_TOPIC_H_ */
