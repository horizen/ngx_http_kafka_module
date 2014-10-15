/*
 * ngx_http_kafka_module.c
 *
 *  Created on: 2014年8月10日
 *      Author: yw
 */


#include "ngx_http_kafka_common.h"
#include "ngx_http_kafka_broker.h"
#include "ngx_http_kafka_topic.h"


#define NGX_HTTP_KAFKA_BACKUP_PATH "backup"
#define NGX_KAFKA_CONF_ACKS_UNSET -2

static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_kafka_status_handler(ngx_http_request_t *r);
static void* ngx_http_kafka_create_main_conf(ngx_conf_t *cf);
static char* ngx_http_kafka_init_main_conf(ngx_conf_t *cf, void *conf);
static void* ngx_http_kafka_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_kafka_merge_loc_conf(ngx_conf_t *cf,
	void *parent, void *child);
static ngx_int_t ngx_http_kafka_module_init(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_kafka_process_init(ngx_cycle_t *cycle);
static void ngx_http_kafka_process_exit(ngx_cycle_t *cycle);
static char* ngx_http_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_kafka_request_acks(ngx_conf_t *cf, ngx_command_t *cmd,
	void *conf);
static char* ngx_http_kafka_log(ngx_conf_t *cf, ngx_command_t *cmd,
	void *conf);
static char* ngx_http_kafka_meta_broker(ngx_conf_t *cf, ngx_command_t *cmd,
	void *conf);
static char* ngx_http_kafka_set_status(ngx_conf_t *cf, ngx_command_t *cmd,
	void *conf);
static ngx_int_t ngx_http_kafka_status_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_http_kafka_init_topic(ngx_cycle_t *cycle,
	ngx_kfk_ctx_t *kfk);
static ngx_int_t ngx_http_kafka_add_variables(ngx_conf_t *cf);
static ngx_int_t ngx_http_kafka_post_init(ngx_conf_t *cf);


static ngx_http_variable_t  ngx_http_kafka_vars[] = {
#if (NGX_KFK_STATUS)
    { ngx_string("pending_msg"), NULL, ngx_http_kafka_status_variable,
      0, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("succ_msg"), NULL, ngx_http_kafka_status_variable,
      1, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("fail_msg"), NULL, ngx_http_kafka_status_variable,
      2, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("wait_buf"), NULL, ngx_http_kafka_status_variable,
      3, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("out_buf"), NULL, ngx_http_kafka_status_variable,
      4, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("free_buf"), NULL, ngx_http_kafka_status_variable,
      5, NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("free_chain"), NULL, ngx_http_kafka_status_variable,
      6, NGX_HTTP_VAR_NOCACHEABLE, 0 },
#endif
    { ngx_null_string, NULL, NULL, 0, 0, 0 }
};

static ngx_path_init_t  ngx_http_kafka_backup_path = {
    ngx_string(NGX_HTTP_KAFKA_BACKUP_PATH), { 0, 0, 0 }
};

static ngx_conf_num_bounds_t  ngx_http_kafka_acks_bounds = {
    ngx_conf_check_num_bounds, -1, 1
};

static ngx_conf_num_bounds_t  ngx_http_kafka_non_negative_bounds = {
    ngx_conf_check_num_bounds, 0, -1
};

static ngx_conf_enum_t  ngx_http_kafka_compression_type[] = {
    { ngx_string("none"), NGX_HTTP_KAFKA_COMPRESSION_NONE },
    { ngx_string("gzip"), NGX_HTTP_KAFKA_COMPRESSION_GZIP },
    { ngx_string("snappy"), NGX_HTTP_KAFKA_COMPRESSION_SNAPPY },
    { ngx_null_string, 0 }
};

static ngx_conf_enum_t ngx_http_kafka_on_buffer_full[] = {
	{ ngx_string("block"), NGX_HTTP_KAFKA_FULL_BLOCK },
	{ ngx_string("discard"), NGX_HTTP_KAFKA_FULL_DISCARD },
	{ ngx_string("backup"), NGX_HTTP_KAFKA_FULL_BACKUP },
	{ ngx_null_string, 0 }
};


static ngx_str_t ngx_http_kafka_topic = ngx_string("kafka_topic");
static ngx_str_t ngx_http_kafka_key = ngx_string("kafka_key");

static ngx_command_t  ngx_http_kafka_commands[] = {
	/* TODO self define*/
	{ ngx_string("kfk.backpath"),
	  NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1234,
	  ngx_conf_set_path_slot,
	  NGX_HTTP_MAIN_CONF_OFFSET,
	  offsetof(ngx_http_kafka_main_conf_t, backpath),
	  NULL },

    /* 0.8.0 */
    { ngx_string("kfk.metadata.broker.list"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_http_kafka_meta_broker,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, meta_brokers),
      NULL },

    /* 0.8.1 */
    { ngx_string("kfk.bootstrap.servers"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_http_kafka_meta_broker,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, meta_brokers),
      NULL },

    /* 0.8.0 */
    { ngx_string("kfk.request.required.acks"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_http_kafka_request_acks,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, acks),
      &ngx_http_kafka_acks_bounds },

    /* 0.8.1 */
    { ngx_string("kfk.acks"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_http_kafka_request_acks,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, acks),
      &ngx_http_kafka_acks_bounds },

    /* 0.8.0 */
    { ngx_string("kfk.request.timeout"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, timeout),
      NULL },

    /* 0.8.1 */
    { ngx_string("kfk.timeout"), 
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, timeout),
      NULL },

    /* TODO self define*/ 
    { ngx_string("kfk.message.timeout"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, msg_timeout),
      NULL }, 

    /* TODO self define */
    { ngx_string("kfk.message.max.size"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, msg_max_size),
      NULL },

      /* TODO 0.8.0 */
    { ngx_string("kfk.compression.codec"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, compress_type),
      &ngx_http_kafka_compression_type },

    /* TODO 0.8.1 */
    { ngx_string("kfk.compression.type"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, compress_type),
      &ngx_http_kafka_compression_type },

      /* TODO 0.8.0 */
    { ngx_string("kfk.compressed.topics" ),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_conf_set_str_array_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, compress_topics),
      NULL },

      /* TODO 0.8.0 */
    { ngx_string("kfk.message.send.max.retries"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, retries),
      &ngx_http_kafka_non_negative_bounds },

    /* TODO 0.8.1 */
    { ngx_string("kfk.retries"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, retries),
      &ngx_http_kafka_non_negative_bounds },

      /* TODO */
    { ngx_string("kfk.retry.backoff"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, retry_backoff),
      NULL },

    /* 0.8.0 */
    { ngx_string("kfk.topic.metadata.refresh.interval"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, meta_refresh),
      NULL },

    /* 0.8.1 */
    { ngx_string("kfk.metadata.max.age"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, meta_refresh),
      NULL },

    /* 0.8.1 */
    { ngx_string("kfk.linger"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, linger),
      NULL },

    /* 0.8.0 */
    { ngx_string("kfk.queue.buffering.max.messages"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, buffering_max_msg),
      NULL },

    /* TODO 0.8.1 */
    { ngx_string("kfk.block.on.buffer.full"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, on_buffer_full),
      &ngx_http_kafka_on_buffer_full },

    /* 0.8.0 */
    { ngx_string("kfk.batch.num.messages"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, batch_size),
      &ngx_http_kafka_non_negative_bounds },

    /* TODO 0.8.1 */
    { ngx_string("kfk.send.buffer.bytes"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, send_buffer),
      NULL },

    { ngx_string("kfk.client.id"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, cli),
      NULL },

    /* self define */
    { ngx_string("kfk.topics"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_conf_set_str_array_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, topics),
      NULL },

    /* 0.8.1 */
    { ngx_string("kfk.reconnect.backoff"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, reconn_backoff),
      NULL },

    /* self define */
    { ngx_string("kfk.log"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_http_kafka_log,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

    /* self define */
    { ngx_string("kfk.rsp.max.size"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, rsp_buffer_size),
      NULL },

    /* self define */
    { ngx_string("kfk.buffers"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE2,
      ngx_conf_set_bufs_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, free_bufs),
      NULL },

    /* self define */
    { ngx_string("kfk.status"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_kafka_main_conf_t, status),
      NULL },

#if (NGX_KFK_STATUS)
    { ngx_string("kafka_stub_status"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_FLAG,
      ngx_http_kafka_set_status,
      0,
      0,
      NULL },
#endif

    { ngx_string("kafka_read_body"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_kafka_loc_conf_t, force_read_body),
      NULL },

    { ngx_string("kafka"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_kafka,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};

static ngx_http_module_t  ngx_http_kafka_module_ctx = {
	ngx_http_kafka_add_variables,	        /* preconfiguration */
    ngx_http_kafka_post_init,				/* postconfiguration */

    ngx_http_kafka_create_main_conf,        /* create main configuration */
    ngx_http_kafka_init_main_conf,          /* init main configuration */

    NULL,									/* create server configuration */
    NULL,						        	/* merge server configuration */

    ngx_http_kafka_create_loc_conf,         /* create location configuration */
    ngx_http_kafka_merge_loc_conf           /* merge location configuration */
};


ngx_module_t  ngx_http_kafka_module = {
    NGX_MODULE_V1,
    &ngx_http_kafka_module_ctx,            /* module context */
    ngx_http_kafka_commands,               /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    ngx_http_kafka_module_init,            /* init module */
    ngx_http_kafka_process_init,           /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_http_kafka_process_exit,           /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

ngx_atomic_t *ngx_kfk_succ_msgs;
ngx_atomic_t *ngx_kfk_fail_msgs;
ngx_atomic_t *ngx_kfk_pending_msgs;
ngx_atomic_t *ngx_kfk_wait_buf;
ngx_atomic_t *ngx_kfk_out_buf;
ngx_atomic_t *ngx_kfk_free_buf;
ngx_atomic_t *ngx_kfk_free_chain;

ngx_kfk_ctx_t	*ngx_kfk;

static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *r)
{
	ngx_http_kafka_main_conf_t	*kmcf;
	ngx_http_kafka_loc_conf_t	*klcf;
	ngx_http_kafka_ctx_t		*ctx;
	ngx_http_variable_value_t	*vv;
	ngx_kfk_topic_t				*topic;
	ngx_kfk_toppar_t			*toppar;
	ngx_uint_t					 partition;
	ngx_int_t					 rc;
	ngx_kfk_ctx_t				*kfk;

	kfk = ngx_kfk;

	if (kfk == NULL) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					  "kafka not start");
		return NGX_HTTP_SERVICE_UNAVAILABLE;
	}

	klcf = ngx_http_get_module_loc_conf(r, ngx_http_kafka_module);
	kmcf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);

	if (kfk->pending_msgs_cnt + 1 > kmcf->buffering_max_msg) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					  "buffering queue is full, try later");
		return NGX_HTTP_SERVICE_UNAVAILABLE;
	}

    vv = ngx_http_get_indexed_variable(r, klcf->topic_index);

    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "the \"$kafka_topic\" variable is not set");
        return NGX_ERROR;
    }

    topic = ngx_http_kafka_topic_find(kfk, vv->data, vv->len);

    if (topic == NULL || topic->state == NGX_KFK_TOPIC_UNKNOWN) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "unknown topic %v", vv);

    	return NGX_HTTP_BAD_REQUEST;
    }

    if (topic->toppars == NULL || topic->toppars->nelts == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "unknown topic(partition uninitilized) %v", vv);

    	return NGX_HTTP_SERVICE_UNAVAILABLE;
    }

    vv = ngx_http_get_indexed_variable(r, klcf->key_index);

    if (vv == NULL || vv->not_found || vv->len == 0) {
        partition = (ngx_uint_t)ngx_random() % topic->toppars->nelts;

    } else {
    	partition = ngx_http_kafka_partition(vv->data, vv->len,
    										 topic->toppars->nelts);
    }

    toppar = ngx_http_kafka_toppar_get_valid(topic, partition);
    if (toppar == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "broker not available");

    	return NGX_HTTP_SERVICE_UNAVAILABLE;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_kafka_module);

    if (ctx == NULL) {
    	ctx = ngx_palloc(r->pool, sizeof(ngx_http_kafka_ctx_t));

    	if (ctx == NULL) {
    		return NGX_HTTP_INTERNAL_SERVER_ERROR;
    	}

    	ctx->topic = topic;
    	ctx->toppar = toppar;
    }

    ngx_http_set_ctx(r, ctx, ngx_http_kafka_module);

    if (klcf->force_read_body) {

    	/* let body in single buf to save the number of copy operations */
        r->request_body_in_single_buf = 1;
        r->request_body_in_persistent_file = 1;
        r->request_body_in_clean_file = 1;

        rc = ngx_http_read_client_request_body(r,
                                       ngx_http_kafka_post_read_body);

        if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
        	return rc;
        }

        return NGX_DONE;
    }

    return ngx_http_kafka_push(r);
}

static ngx_int_t
ngx_http_kafka_status_handler(ngx_http_request_t *r)
{
    size_t             size;
    ngx_int_t          rc;
    ngx_buf_t         *b;
    ngx_chain_t        out;
    ngx_atomic_int_t   pm, sm, fm, wb, ob, fb, fc;

    if (r->method != NGX_HTTP_GET && r->method != NGX_HTTP_HEAD) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    r->headers_out.content_type_len = sizeof("text/plain") - 1;
    ngx_str_set(&r->headers_out.content_type, "text/plain");
    r->headers_out.content_type_lowcase = NULL;

    if (r->method == NGX_HTTP_HEAD) {
        r->headers_out.status = NGX_HTTP_OK;

        rc = ngx_http_send_header(r);

        if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
            return rc;
        }
    }

    size = sizeof("Pending messages: \n"
    			  "Success messages: \n"
    			  "Fail messages: \n"
    			  "Wait bufs: \n"
    			  "Out bufs: \n"
    			  "Free bufs: \n"
    			  "Free chains: \n") + 7 * NGX_ATOMIC_T_LEN;

    b = ngx_create_temp_buf(r->pool, size);
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    out.buf = b;
    out.next = NULL;

    pm = *ngx_kfk_pending_msgs;
    sm = *ngx_kfk_succ_msgs;
    fm = *ngx_kfk_fail_msgs;
    wb = *ngx_kfk_wait_buf;
    ob = *ngx_kfk_out_buf;
    fb = *ngx_kfk_free_buf;
    fc = *ngx_kfk_free_chain;

    b->last = ngx_sprintf(b->last, "Pending messages: %uA\n"
    			  	  	  "Success messages: %uA\n"
    			  	  	  "Fail messages: %uA\n"
    			  	  	  "Wait bufs: %uA\n"
    			  	  	  "Out bufs: %uA\n"
    			  	  	  "Free bufs: %uA\n"
    			  	  	  "Free chains: %uA\n",
                          pm, sm, fm, wb, ob, fb, fc);

    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = b->last - b->pos;

    b->last_buf = (r == r->main) ? 1 : 0;
    b->last_in_chain = 1;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    return ngx_http_output_filter(r, &out);
}

static void*
ngx_http_kafka_create_main_conf(ngx_conf_t *cf)
{
	ngx_http_kafka_main_conf_t 	*kmcf;

	kmcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_main_conf_t));
	if (kmcf == NULL) {
	    return NULL;
	}

	/*
	 * kmcf->cli = {0, NULL}
	 * kmcf->backpath = NULL
	 * kmcf->free_bufs.num = {0, 0}
	 * kmcf->resolver = NULL
	 * kmcf->log = NULL
	 */
    kmcf->meta_brokers = NGX_CONF_UNSET_PTR;
	kmcf->acks = NGX_KAFKA_CONF_ACKS_UNSET;
	kmcf->timeout = NGX_CONF_UNSET_MSEC;
	kmcf->msg_timeout = NGX_CONF_UNSET_MSEC;
	kmcf->msg_max_size = NGX_CONF_UNSET_SIZE;
	kmcf->compress_type = NGX_CONF_UNSET_UINT;
	kmcf->compress_topics = NGX_CONF_UNSET_PTR;
	kmcf->retries = NGX_CONF_UNSET;
	kmcf->rsp_buffer_size = NGX_CONF_UNSET_SIZE;
	kmcf->retry_backoff = NGX_CONF_UNSET_MSEC;
	kmcf->meta_refresh = NGX_CONF_UNSET_MSEC;
	kmcf->linger = NGX_CONF_UNSET_MSEC;
	kmcf->buffering_max_msg = NGX_CONF_UNSET_SIZE;
	kmcf->on_buffer_full = NGX_CONF_UNSET_UINT;
	kmcf->batch_size = NGX_CONF_UNSET;
	kmcf->send_buffer = NGX_CONF_UNSET_SIZE;
    kmcf->topics = NGX_CONF_UNSET_PTR;
	kmcf->status = NGX_CONF_UNSET;
	kmcf->reconn_backoff = NGX_CONF_UNSET_MSEC;
	kmcf->resolver_timeout = NGX_CONF_UNSET_MSEC;

	return kmcf;
}

static char* ngx_http_kafka_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_kafka_main_conf_t  *kmcf = conf;
    ngx_http_core_loc_conf_t	*clcf;

    if (kmcf->meta_brokers == NGX_CONF_UNSET_PTR) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "the metadata.broker.list must be set");
        return NGX_CONF_ERROR;
    }

    if (kmcf->topics == NGX_CONF_UNSET_PTR) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "the topics must be set");
        return NGX_CONF_ERROR;
    }

    if (kmcf->cli.len > NGX_KFK_MAX_CLIENT_ID_LEN) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "client id string len must less than 10");
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (clcf->resolver == NULL || clcf->resolver->udp_connections.nelts == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "no resolver found for kafka");
    	return NGX_CONF_ERROR;
    }

    kmcf->resolver = clcf->resolver;

    if (clcf->resolver_timeout == NGX_CONF_UNSET_MSEC) {
    	kmcf->resolver_timeout = 5000;

    } else {
    	kmcf->resolver_timeout = clcf->resolver_timeout;
    }

    ngx_conf_init_msec_value(kmcf->timeout, 5000);
    ngx_conf_init_msec_value(kmcf->msg_timeout, 10000);
    ngx_conf_init_size_value(kmcf->msg_max_size, 4096);
    ngx_conf_init_uint_value(kmcf->compress_type,
    						 NGX_HTTP_KAFKA_COMPRESSION_NONE);
    ngx_conf_init_value(kmcf->retries, 1);
    ngx_conf_init_msec_value(kmcf->retry_backoff, 100);
    ngx_conf_init_msec_value(kmcf->meta_refresh, 120000);
    ngx_conf_init_msec_value(kmcf->linger, 2000);
    ngx_conf_init_size_value(kmcf->rsp_buffer_size, 4096);
    ngx_conf_init_size_value(kmcf->buffering_max_msg, 10000);
    ngx_conf_init_uint_value(kmcf->on_buffer_full,
    						 NGX_HTTP_KAFKA_FULL_DISCARD);
    ngx_conf_init_value(kmcf->batch_size, 500);
    ngx_conf_init_size_value(kmcf->send_buffer, 102400);
    ngx_conf_init_value(kmcf->status, 0);
    ngx_conf_init_msec_value(kmcf->reconn_backoff, 60000);

    if (ngx_conf_merge_path_value(cf, &kmcf->backpath, NULL,
                                  &ngx_http_kafka_backup_path)
        != NGX_CONF_OK)
    {
        return NGX_CONF_ERROR;
    }

    if (kmcf->log == NULL) {
    	kmcf->log = &cf->cycle->new_log;
    }

    if (kmcf->free_bufs.num == 0) {
        kmcf->free_bufs.num = 100;
        kmcf->free_bufs.size = 4096;
    }

    return NGX_CONF_OK;
}

static void*
ngx_http_kafka_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_kafka_loc_conf_t	*klcf;

	klcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_loc_conf_t));
	if (klcf == NULL) {
		return NULL;
	}

	/*
	 * klcf->topic_index = 0
	 * klcf->key_index = 0
	 * klcf->msg_lengths = NULL
	 * klcf->msg_values = NULL
	 */

	klcf->force_read_body = NGX_CONF_UNSET;

	return klcf;
}

static char *
ngx_http_kafka_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_kafka_loc_conf_t	*prev = parent;
	ngx_http_kafka_loc_conf_t	*conf = child;

	ngx_conf_merge_value(conf->force_read_body, prev->force_read_body, 0);

	return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_kafka_module_init(ngx_cycle_t *cycle)
{
    ngx_http_kafka_main_conf_t  *kmcf;
	ngx_shm_t					 shm;
	u_char 	    				*shared;
	size_t						 cl, size;

#if (NGX_KFK_STATUS)
    /* cl should be equal to or greater than cache line size */
    cl = 128;

    size = cl            /* succ_msgs */
           + cl          /* fail_msgs */
           + cl          /* pending_msgs */
    	   + cl          /* wait_buf */
           + cl          /* out_buf */
           + cl          /* free_buf */
    	   + cl;		 /* free_chain */

    shm.size = size;
    shm.name.len = sizeof("ngx_kafka_shared");
    shm.name.data = (u_char *) "ngx_kafka_shared";
    shm.log = cycle->log;

    if (ngx_shm_alloc(&shm) != NGX_OK) {
        return NGX_ERROR;
    }

    shared = shm.addr;

    ngx_kfk_succ_msgs = (ngx_atomic_t *) (shared);
    ngx_kfk_fail_msgs = (ngx_atomic_t *) (shared + 1 * cl);
    ngx_kfk_pending_msgs = (ngx_atomic_t *) (shared + 2 * cl);
    ngx_kfk_wait_buf = (ngx_atomic_t *) (shared + 3 * cl);
    ngx_kfk_out_buf= (ngx_atomic_t *) (shared + 4 * cl);
    ngx_kfk_free_buf = (ngx_atomic_t *) (shared + 5 * cl);
    ngx_kfk_free_chain = (ngx_atomic_t *) (shared + 6 * cl);
#endif

	kmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

#if (NGX_KFK_DEBUG)
    debug(kmcf->log, "[kafka] config, acks: %i, timeout: %M, msg_timeout: %M, "
    	  "msg_max_size: %uz, retries: %i, retry_backoff: %M, "
    	  "meta_refresh: %M, linger: %M, buffering-max_msg: %uz, "
    	  "batch_size: %i, cli: %V, rsp_buffer_size: %uz, "
    	  "reconn_backoff: %M, free_bufs: %i, status: %i",
    	  kmcf->acks, kmcf->timeout, kmcf->msg_timeout, kmcf->msg_max_size,
    	  kmcf->retries, kmcf->retry_backoff, kmcf->meta_refresh, kmcf->linger,
    	  kmcf->buffering_max_msg, kmcf->batch_size, &kmcf->cli,
    	  kmcf->rsp_buffer_size, kmcf->reconn_backoff, kmcf->free_bufs.num,
    	  kmcf->status);
#endif

	return NGX_OK;
}

static ngx_int_t
ngx_http_kafka_process_init(ngx_cycle_t *cycle)
{
	ngx_http_kafka_main_conf_t	*kmcf;
	ngx_kfk_ctx_t 				*kfk;

	kmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

	kfk = ngx_pcalloc(cycle->pool, sizeof(ngx_kfk_ctx_t));
	if (kfk == NULL) {
		return NGX_ERROR;
	}

	/*
	 * kfk->pending_msgs_cnt = 0
	 * kfk->free = NULL
	 * kfk->free_chain = NULL
	 * kfk->allocated = 0;
	 * kfk->meta_buf = {0, ...}
	 * kfk->meta_body = {0, NULL}
	 * kfk->meta_broker = NULL
	 */

	kfk->cf = kmcf;
	kfk->corr_id = 1;

	kfk->pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, kfk->log);
	if (kfk->pool == NULL) {
		return NGX_ERROR;
	}

	kfk->pool->log = kfk->log;
	kfk->log = kmcf->log;

	if (ngx_http_kafka_init_topic(cycle, kfk) != NGX_OK) {
		ngx_destroy_pool(kfk->pool);
		return NGX_ERROR;
	}

	ngx_queue_init(&kfk->brokers);

	kfk->meta_ev.handler = ngx_http_kafka_meta_force_refresh;
	kfk->meta_ev.log = kfk->log;
	kfk->meta_ev.data = kfk;

	ngx_kfk = kfk;

	ngx_add_timer(&kfk->meta_ev, 0);

	return NGX_OK;
}

static void
ngx_http_kafka_process_exit(ngx_cycle_t *cycle)
{
	ngx_kfk_ctx_t		*kfk = ngx_kfk;
	ngx_kfk_broker_t	*broker;
	ngx_kfk_topic_t		*topics;
	ngx_queue_t			*q;
	ngx_uint_t			 i;

	if (kfk == NULL) {
		return;
	}

	if (kfk->meta_ev.timer_set) {
		ngx_del_timer(&kfk->meta_ev);
	}

	while (!ngx_queue_empty(&kfk->brokers)) {
		q = ngx_queue_head(&kfk->brokers);

		broker = ngx_queue_data(q, ngx_kfk_broker_t, queue);

		/* in function, the node is removed */
		ngx_http_kafka_destroy_broker(broker);
	}

	topics = kfk->topics->elts;

	for (i = 0; i < kfk->topics->nelts; i++) {
		ngx_http_kafka_destroy_topic(&topics[i], NGX_KAFKA_NGINX_DOWN);
	}

	ngx_destroy_pool(kfk->pool);
}

static char*
ngx_http_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  	*clcf;
    ngx_http_kafka_loc_conf_t	*klcf = conf;
    ngx_http_script_compile_t	 sc;
    ngx_str_t					*values;
    ngx_int_t                    index;
    ngx_uint_t					 n;

	if (klcf->msg_lengths) {
		return "is duplicate";
	}

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_kafka_handler;

    index = ngx_http_get_variable_index(cf, &ngx_http_kafka_topic);

	if (index == NGX_ERROR) {
		return NGX_CONF_ERROR;
	}
    klcf->topic_index = index;

	index = ngx_http_get_variable_index(cf, &ngx_http_kafka_key);

	if (index == NGX_ERROR) {
		return NGX_CONF_ERROR;
	}
    klcf->key_index = index;

	values = cf->args->elts;

	n = ngx_http_script_variables_count(&values[1]);
	if (n == 0) {
		ngx_log_error(NGX_LOG_ERR, cf->cycle->log, 0,
				      "kafka message must not be a const string");
		return NGX_CONF_ERROR;
	}

	ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

	sc.cf = cf;
	sc.source = &values[1];
	sc.lengths = &klcf->msg_lengths;
	sc.values = &klcf->msg_values;
	sc.variables = n;
	sc.complete_lengths = 1;
	sc.complete_values = 1;

	if (ngx_http_script_compile(&sc) != NGX_OK) {
		return NGX_CONF_ERROR ;
	}

	return NGX_CONF_OK;
}

static char *
ngx_http_kafka_request_acks(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    ngx_int_t        *np;
    ngx_str_t        *value;
    ngx_conf_post_t  *post;


    np = (ngx_int_t *) (p + cmd->offset);

    if (*np != NGX_KAFKA_CONF_ACKS_UNSET) {
        return "is duplicate";
    }

    value = cf->args->elts;
    *np = ngx_atoi(value[1].data, value[1].len);
    if (*np == NGX_ERROR) {
        return "invalid number";
    }

    if (cmd->post) {
        post = cmd->post;
        return post->post_handler(cf, post, np);
    }

    return NGX_CONF_OK;
}

static char*
ngx_http_kafka_meta_broker(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    ngx_str_t         *value;
    ngx_url_t		  *url;
    ngx_array_t      **a;
    ngx_uint_t		   i;

    a = (ngx_array_t **) (p + cmd->offset);

    if (*a == NGX_CONF_UNSET_PTR) {
        *a = ngx_array_create(cf->pool, 4, sizeof(ngx_url_t));

        if (*a == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    for (i = 1; i < cf->args->nelts; i++) {
        url = ngx_array_push(*a);
        if (url == NULL) {
            return NGX_CONF_ERROR;
        }

        value = cf->args->elts;

        ngx_memzero(url, sizeof(ngx_url_t));

		url->url = value[i];
		url->default_port = (in_port_t)9092;

		if (ngx_parse_url(cf->pool, url) != NGX_OK) {
			ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
						  	   "invalid metadata.broker.list %V: %s",
						  	   &value[i], url->err ? url->err: "");
			return NGX_CONF_ERROR;
		}
    }

    return NGX_CONF_OK;
}

static char*
ngx_http_kafka_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_kafka_main_conf_t	*kmcf;

	kmcf = conf;

    return ngx_log_set_log(cf, &kmcf->log);
}

static char*
ngx_http_kafka_set_status(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_kafka_status_handler;

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_kafka_status_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    u_char            *p;
    ngx_atomic_int_t   value;

    p = ngx_pnalloc(r->pool, NGX_ATOMIC_T_LEN);
    if (p == NULL) {
        return NGX_ERROR;
    }

    switch (data) {
    case 0:
        value = *ngx_kfk_pending_msgs;
        break;

    case 1:
        value = *ngx_kfk_succ_msgs;
        break;

    case 2:
        value = *ngx_kfk_fail_msgs;
        break;

    case 3:
        value = *ngx_kfk_wait_buf;
        break;

    case 4:
        value = *ngx_kfk_out_buf;
        break;

    case 5:
        value = *ngx_kfk_free_buf;
        break;

    case 6:
        value = *ngx_kfk_free_chain;
        break;

    /* suppress warning */
    default:
        value = 0;
        break;
    }

    v->len = ngx_sprintf(p, "%uA", value) - p;
    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->data = p;

    return NGX_OK;
}

static ngx_int_t
ngx_http_kafka_init_topic(ngx_cycle_t *cycle, ngx_kfk_ctx_t *kfk)
{
	ngx_http_kafka_main_conf_t	*kmcf;
	ngx_kfk_topic_t		*topic;
    ngx_str_t           *topics;
	ngx_uint_t		 	 i;

	kmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

	kfk->topics = ngx_array_create(kfk->pool, kmcf->topics->nelts,
								   sizeof(ngx_kfk_topic_t));
	if (kfk->topics == NULL) {
		return NGX_ERROR;
	}

    topics = kmcf->topics->elts;

	for (i = 0; i < kmcf->topics->nelts; i++) {
		if (topics[i].len == 0) {
			continue;
		}

        topic = ngx_array_push(kfk->topics);
        if (topic == NULL) {
            return NGX_ERROR;
        }

		topic->kfk = kfk;
		topic->name = topics[i];
		topic->state = NGX_KFK_TOPIC_UNKNOWN;
		topic->toppars = NULL;
	}

	return NGX_OK;
}


static ngx_int_t
ngx_http_kafka_add_variables(ngx_conf_t *cf)
{
    ngx_http_variable_t  *var, *v;

    for (v = ngx_http_kafka_vars; v->name.len; v++) {
        var = ngx_http_add_variable(cf, &v->name, v->flags);
        if (var == NULL) {
            return NGX_ERROR;
        }

        var->get_handler = v->get_handler;
        var->data = v->data;
    }

    return NGX_OK;
}

static
ngx_int_t ngx_http_kafka_post_init(ngx_conf_t *cf)
{
#if (NGX_KFK_LUA)
	if (ngx_http_lua_add_package_preload(cf, "kfk",
	                                     ngx_http_kafka_lua_inject_api)
	    != NGX_OK)
	{
	    return NGX_ERROR;
	}
#endif

	return NGX_OK;
}
