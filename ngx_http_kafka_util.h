/*
 * ngx_http_kafka_util.h
 *
 *  Created on: 2014年9月22日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_UTIL_H_
#define NGX_HTTP_KAFKA_UTIL_H_

#include "ngx_http_kafka_common.h"

#define _check_len(b, len) do {					\
		ngx_int_t _len = (ngx_int_t)(len);		\
		if (_len > (b)->last - (b)->pos) {			\
			goto again;						\
		}										\
	} while(0)

#define _skip(b, len) do {			\
		_check_len(b, len);			\
		(b)->pos += (len);			\
	} while(0)

#define _read_i16(b, v)  do {		\
		_check_len(b, 2);			\
		*(v) = (int16_t)ngx_kfk_atoi((b)->pos, 2); 	\
		(b)->pos += 2;				\
	} while(0)

#define _read_i32(b, v)  do {		\
		_check_len(b, 4);			\
		*(v) = (int32_t)ngx_kfk_atoi((b)->pos, 4); 	\
		(b)->pos += 4;				\
	} while(0)

#define _read_i64(b, v)  do {		\
		_check_len(b, 8);			\
		*(v) = (int64_t)ngx_kfk_atoi((b)->pos, 8); 	\
		(b)->pos += 8;				\
	} while(0)

#define _read_str(b, v) do { 		\
		_read_i16(b, &((v)->len));		\
		(v)->data = (b)->pos;			\
		(b)->pos += (v)->len;			\
	} while(0)

#define _read_byte(b, v) do { 		\
		_read_i32(b, &((v)->len));		\
		(v)->data = (b)->pos;			\
		(b)->pos += (v)->len;			\
	} while(0)


#define KAFKA_ERR_UNKNOWN -1
#define KAFKA_OFFSET_OUT_OF_RANGE 1
#define KAFKA_INVALID_MESSAGE 2
#define KAFKA_UNKNOWN_TOPIC_OR_PARTITION 3
#define KAFKA_INVALID_MESSAGE_SIZE 4
#define KAFKA_LEADER_NOT_AVAILABLE 5
#define KAFKA_REQUEST_TIMED_OUT 7
#define KAFKA_MESSAGE_SIZE_TOO_LARGE 10
#define KAFKA_OFFSET_METADATA_TOO_LARGE_CODE 12
#define KAFKA_OFFSETS_LOAD_IN_PROGRESS_CODE 14
#define KAFKA_NOT_COORDINATOR_FOR_CONSUMER_CODE 15
#define KAFKA_CONSUMERE_COORDINATOR_NOT_AVAILABLE_CODE 16

#define KAFKA_LAST_ERR 16

#define NGX_KAFKA_INTERNAL_SERVER_ERROR KAFKA_LAST_ERR + 1
#define NGX_KAFKA_RESOLVER_ERROR KAFKA_LAST_ERR + 2
#define NGX_KAFKA_NO_RESOLVER KAFKA_LAST_ERR + 3
#define NGX_KAFKA_NO_MEMORY KAFKA_LAST_ERR + 4
#define NGX_KAFKA_NO_BUFFER KAFKA_LAST_ERR + 5
#define NGX_KAFKA_SOCKET_ERROR KAFKA_LAST_ERR + 6
#define NGX_KAFKA_SOCKET_TIMEOUT KAFKA_LAST_ERR + 7
#define NGX_KAFKA_SOCKET_CLOSED KAFKA_LAST_ERR + 8

#define NGX_KAFKA_MESSAGE_TIMED_OUT KAFKA_LAST_ERR + 9
#define NGX_KAFKA_INVALID_TOPIC_CNT KAFKA_LAST_ERR + 10
#define NGX_KAFKA_INVALID_PARTITION_CNT KAFKA_LAST_ERR + 11
#define NGX_KAFKA_INVALID_MSG_SIZE KAFKA_LAST_ERR + 12
#define NGX_KAFKA_INVALID_ERROR KAFKA_LAST_ERR + 13
#define NGX_KAFKA_INVALID_RESPONSE KAFKA_LAST_ERR + 14
#define NGX_KAFKA_BROKER_DOWN KAFKA_LAST_ERR + 15
#define NGX_KAFKA_NGINX_DOWN KAFKA_LAST_ERR + 16

extern char *ngx_kfk_strerr[];

int64_t ngx_kfk_atoi(u_char *line, size_t n);

#endif /* NGX_HTTP_KAFKA_UTIL_H_ */
