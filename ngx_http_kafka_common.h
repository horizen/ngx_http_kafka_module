/*
 * ngx_http_kafka_common.h
 *
 *  Created on: 2014年9月23日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_COMMON_H_
#define NGX_HTTP_KAFKA_COMMON_H_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

#if (NGX_KFK_LUA)
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include "ngx_http_lua_api.h"
#endif

typedef struct ngx_kfk_buf_s ngx_kfk_buf_t;
typedef struct ngx_kfk_ctx_s ngx_kfk_ctx_t;
typedef struct ngx_kfk_toppar_s ngx_kfk_toppar_t;
typedef struct ngx_kfk_topic_s ngx_kfk_topic_t;
typedef struct ngx_kfk_broker_s ngx_kfk_broker_t;

typedef struct ngx_http_kafka_upstream_s ngx_http_kafka_upstream_t;
typedef struct ngx_http_kafka_main_conf_s ngx_http_kafka_main_conf_t;

#define NGX_KFK_PRODUCE_REQ 0
#define NGX_KFK_META_REQ  3

#define NGX_KFK_MAX_CLIENT_ID_LEN 18
#define NGX_KFK_HEADER_SIZE 14
#define NGX_KFK_MSG_HEADER_SIZE 26
#define NGX_KFK_REQ_HEADER_PART1_SIZE 12
#define NGX_KFK_REQ_HEADER_PART2_SIZE 12

#define ngx_kfk_header_size(b)	*(int32_t *)((b)->data)
#define ngx_kfk_header_size_ptr(b)	(int32_t *)((b)->data)
#define ngx_kfk_header_api_key(b)	*(int16_t *)((b)->data + 4)
#define ngx_kfk_header_api_ver(b)	*(int16_t *)((b)->data + 6)
#define ngx_kfk_header_corr_id(b)	*(int32_t *)((b)->data + 8)
#define ngx_kfk_header_cli_len(b)	*(int16_t *)((b)->data + 12)
#define ngx_kfk_header_cli_data_ptr(b)	 ((b)->data + 14)

#define ngx_kfk_req_header_part1_ptr(b) \
	((b)->data + NGX_KFK_HEADER_SIZE + NGX_KFK_MAX_CLIENT_ID_LEN)

#define ngx_kfk_req_header_part2_ptr(b) \
	(ngx_kfk_req_header_part1_ptr(b) + NGX_KFK_REQ_HEADER_PART1_SIZE)

#define ngx_kfk_header_acks(b)	\
	*(int16_t *)ngx_kfk_req_header_part1_ptr(b)

#define ngx_kfk_header_timeout(b)	\
	*(int32_t *)(ngx_kfk_req_header_part1_ptr(b) + 2)

#define ngx_kfk_header_topic_cnt(b)	\
	*(int32_t *)(ngx_kfk_req_header_part1_ptr(b) + 6)

#define ngx_kfk_header_topic_len(b)	\
	*(int16_t *)(ngx_kfk_req_header_part1_ptr(b) + 10)

#define ngx_kfk_header_part_cnt(b)	\
	*(int32_t *)(ngx_kfk_req_header_part2_ptr(b))

#define ngx_kfk_header_partition(b)	\
	*(int32_t *)(ngx_kfk_req_header_part2_ptr(b) + 4)

#define ngx_kfk_header_msgset_size(b)	\
	*(int32_t *)(ngx_kfk_req_header_part2_ptr(b) + 8)

#define ngx_kfk_header_msgset_size_ptr(b)	\
	(int32_t *)(ngx_kfk_req_header_part2_ptr(b) + 8)

#include "debug.h"
#include "ngx_http_kafka_types.h"
#include "ngx_http_kafka_util.h"
#include "ngx_http_kafka_module.h"

int ngx_http_kafka_lua_inject_api(lua_State *L);
ngx_int_t ngx_http_kafka_push(ngx_http_request_t *r);
ngx_int_t ngx_http_kafka_enq_buf(ngx_http_request_t *r,
	ngx_http_kafka_ctx_t *ctx);
void ngx_http_kafka_meta_force_refresh(ngx_event_t *ev);

static ngx_inline void ngx_http_kafka_post_read_body(ngx_http_request_t *r) 
{
    ngx_http_finalize_request(r, ngx_http_kafka_push(r));
}

static ngx_inline void ngx_http_kafka_recycle_msg(ngx_pool_t *pool, 
    ngx_str_t *msg)
{ 
#if (NGX_KFK_DEBUG)
	debug(pool->log, "[kafka] recycle message back to pool");
#endif

	if  (msg->data + msg->len == pool->d.last) {
		pool->d.last -= msg->len;
	}
}

#endif /* NGX_HTTP_KAFKA_COMMON_H_ */
