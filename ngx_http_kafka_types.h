/*
 * ngx_http_kafka_types.h
 *
 *  Created on: 2014年8月11日
 *      Author: yw
 */

#ifndef NGX_HTTP_KAFKA_TYPES_H_
#define NGX_HTTP_KAFKA_TYPES_H_

#include "ngx_http_kafka_common.h"



typedef struct {
	int16_t		err;
	int32_t		id;
	int32_t		leader;
}ngx_kfk_part_meta_t;

typedef struct {
	int16_t				 err;
	ngx_str_t			 topic;
	int32_t				 part_cnt;
	ngx_kfk_part_meta_t *part;
}ngx_kfk_top_meta_t;

struct ngx_kfk_toppar_s {
	ngx_kfk_ctx_t			*kfk;
	ngx_queue_t			 	 queue;

	ngx_kfk_topic_t			*topic;
	ngx_kfk_broker_t		*broker;

	ngx_kfk_buf_t			*out;
	ngx_kfk_buf_t			**last_out;

	ngx_kfk_buf_t			*free;

	ngx_uint_t			 	 partition;

	ngx_event_t				 send_timer;
};

typedef enum {
	NGX_KFK_TOPIC_INIT,
	NGX_KFK_TOPIC_UNKNOWN,
	NGX_KFK_TOPIC_EXIST
}ngx_kfk_topic_state_t;

struct ngx_kfk_topic_s {
	ngx_kfk_ctx_t			*kfk;
	ngx_str_t				 name;
	ngx_kfk_topic_state_t	 state;
	ngx_array_t				*toppars;
};

typedef struct {
	int32_t		len;
	u_char		data[0];
}ngx_kfk_byte_t;

typedef struct {
	int16_t		len;
	u_char 		data[0];
}ngx_kfk_str_t;

/*
typedef struct {
	int64_t		offset;
	int32_t		msg_size;
	int32_t		crc;
	int8_t		magic;
	int8_t		attr;
	int32_t		key_len;	// key bytes len, we all set to -1, so we set key to null
	int32_t		value_len;
    u_char      data[0];
}ngx_kfk_msg_t;
*/

struct ngx_kfk_buf_s {
	ngx_kfk_ctx_t	*kfk;
	ngx_pool_t		*pool;
	ngx_int_t		 cnt;
	ngx_msec_t		 timeout;
	ngx_chain_t		*chain;
	ngx_chain_t	   **last_chain;
	ngx_kfk_buf_t	*next;

	ngx_uint_t		 no_pool:1;
    
    ngx_str_t        header;

    u_char           data[NGX_KFK_HEADER_SIZE + NGX_KFK_MAX_CLIENT_ID_LEN
						  + NGX_KFK_REQ_HEADER_PART1_SIZE
						  + NGX_KFK_REQ_HEADER_PART2_SIZE];

    /* header comment
	struct {
		int32_t			size;
		int16_t			api_key;
		int16_t			api_ver;
		int32_t			corr_id;
		int16_t			cli_len;
		u_char			data[NGX_KFK_MAX_CLIENT_ID_LEN];
	}header;

	struct {
		// part1
		int16_t acks;
		int32_t timeout;
		int32_t topic_cnt; // always set 1
		int16_t topic_len; // topic string len

		// topic string insert here

		// part2
		int32_t part_cnt;	// alway set 1
		int32_t partition;
		int32_t msgset_size;
	}req_header;
	*/
};

typedef enum {
	NGX_KFK_BROKER_DOWN,
	NGX_KFK_BROKER_UP
}ngx_kfk_broker_state_t;

struct ngx_kfk_broker_s {
	ngx_http_kafka_upstream_t	*upstream;
	ngx_kfk_ctx_t	*kfk;

	ngx_str_t		host;
	ngx_uint_t		port;
	ngx_int_t		id;

	ngx_queue_t		queue;

	ngx_event_t		reconn;

	int32_t			 rsp_size;

	ngx_pool_t		*pool;
	ngx_log_t		*log;

	ngx_kfk_buf_t	*wait;
	ngx_kfk_buf_t	**last_wait;

	ngx_queue_t		toppars;

	ngx_kfk_broker_state_t	state;

	unsigned		meta_query:1;
};

struct ngx_kfk_ctx_s {
	ngx_http_kafka_main_conf_t		*cf;
	ngx_int_t						 allocated;
	ngx_uint_t						 corr_id;
	size_t							 pending_msgs_cnt;

	ngx_pool_t						*pool;
	ngx_log_t						*log;

	ngx_kfk_buf_t					*free;
	ngx_chain_t						*free_chain;

	ngx_array_t						*topics;
	ngx_queue_t						 brokers;

	ngx_kfk_buf_t					 meta_buf;
	ngx_event_t						 meta_ev;
	ngx_kfk_broker_t				*meta_broker;
};


#endif /* NGX_HTTP_KAFKA_TYPES_H_ */
