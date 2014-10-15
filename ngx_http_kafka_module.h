/*
 * ngx_http_kafka_module.h
 *
 *  Created on: 2014年8月10日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_MODULE_H_
#define NGX_HTTP_KAFKA_MODULE_H_

#include "ngx_http_kafka_common.h"

#define NGX_HTTP_KAFKA_COMPRESSION_NONE 0
#define NGX_HTTP_KAFKA_COMPRESSION_GZIP 1
#define NGX_HTTP_KAFKA_COMPRESSION_SNAPPY 2

#define NGX_HTTP_KAFKA_FULL_BLOCK 0
#define NGX_HTTP_KAFKA_FULL_DISCARD 1
#define NGX_HTTP_KAFKA_FULL_BACKUP 2

struct ngx_http_kafka_main_conf_s {
	ngx_array_t			*meta_brokers;
	ngx_int_t			 acks;
	ngx_msec_t			 timeout;
	ngx_msec_t			 msg_timeout;
	size_t				 msg_max_size;
	ngx_uint_t			 compress_type;
	ngx_array_t			*compress_topics;
	ngx_int_t			 retries;
	ngx_msec_t			 retry_backoff;
	ngx_msec_t			 meta_refresh;
	ngx_msec_t			 linger;
	size_t			 	 buffering_max_msg;
	ngx_uint_t			 on_buffer_full;
	ngx_int_t		 	 batch_size;
	size_t			 	 send_buffer;
	ngx_str_t			 cli;
	size_t				 rsp_buffer_size;

	ngx_array_t			*topics;
	ngx_path_t			*backpath;

	ngx_msec_t			 reconn_backoff;
	ngx_bufs_t			 free_bufs;

	ngx_resolver_t		*resolver;
	ngx_msec_t			 resolver_timeout;

	ngx_flag_t			 status;

	ngx_log_t			*log;
};

typedef struct {
	ngx_uint_t		 topic_index;
	ngx_uint_t		 key_index;

	ngx_flag_t		 force_read_body;
	ngx_array_t		*msg_lengths;
	ngx_array_t		*msg_values;
}ngx_http_kafka_loc_conf_t;

typedef struct {
	ngx_kfk_topic_t		*topic;
	ngx_kfk_toppar_t	*toppar;
	ngx_str_t      		*msg;
}ngx_http_kafka_ctx_t;


#define ngx_http_kafka_partition(p, len, n) (ngx_crc32_short(p, len) % n)

#if (NGX_KFK_STATUS)
extern ngx_atomic_t *ngx_kfk_succ_msgs;
extern ngx_atomic_t *ngx_kfk_fail_msgs;
extern ngx_atomic_t *ngx_kfk_pending_msgs;
extern ngx_atomic_t *ngx_kfk_wait_buf;
extern ngx_atomic_t *ngx_kfk_out_buf;
extern ngx_atomic_t *ngx_kfk_free_buf;
extern ngx_atomic_t *ngx_kfk_free_chain;
#endif

extern ngx_kfk_ctx_t	*ngx_kfk;
extern ngx_module_t  	 ngx_http_kafka_module;

#endif /* NGX_HTTP_KAFKA_MODULE_H_ */
