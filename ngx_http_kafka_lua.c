/*
 * ngx_http_kafka_lua.c
 *
 *  Created on: 2014年10月15日
 *      Author: yw
 */

#include "ngx_http_kafka_common.h"
#include "ngx_http_kafka_topic.h"
#include "ngx_http_kafka_buf.h"

static int ngx_http_kafka_lua_log(lua_State *L);
static ngx_int_t ngx_http_kafka_lua_push(lua_State *L);

int
ngx_http_kafka_lua_inject_api(lua_State *L)
{
	lua_createtable(L, 0, 1);

    lua_pushcfunction(L, ngx_http_kafka_lua_log);
    lua_setfield(L, -2, "log");

    return 1;
}

static int
ngx_http_kafka_lua_log(lua_State *L)
{
    ngx_kfk_ctx_t			*kfk = ngx_kfk;
    ngx_http_request_t      *r;
    ngx_http_kafka_ctx_t	*ctx;
    ngx_kfk_topic_t			*topic;
    ngx_kfk_toppar_t		*toppar;
    ngx_uint_t				 partition;
    u_char              	*p;
    size_t               	 len;
    int                  	 n, type;

    n = lua_gettop(L);
    if (n < 3) {
        return luaL_error(L, "kfk.log: expecting at least 3 arguments but got %d", n);
    }

    if (kfk == NULL) {
    	lua_pushnil(L);
    	lua_pushliteral(L, "kafka not start");
    	return 2;
    }

	if (kfk->pending_msgs_cnt + 1 > kfk->cf->buffering_max_msg) {
    	lua_pushnil(L);
    	lua_pushliteral(L, "buffering queue is full, try later");
		return 2;
	}

    p = (u_char *) luaL_checklstring(L, 1, &len);
    if (len == 0) {
    	lua_pushnil(L);
    	lua_pushliteral(L, "unknown topic");
    	return 2;
    }

    topic = ngx_http_kafka_topic_find(kfk, p, len);
    if (topic == NULL || topic->state == NGX_KFK_TOPIC_UNKNOWN) {
        lua_pushnil(L);
        lua_pushliteral(L, "unknown topic");
        return 2;
    }

    if (topic->toppars == NULL || topic->toppars->nelts == 0) {
    	lua_pushnil(L);
        lua_pushliteral(L, "unknown topic(partition uninitilized)");
    	return 2;
    }

    type = lua_type(L, 2);

    switch (type) {
    	case LUA_TNIL:
    		partition = (ngx_uint_t)ngx_random() % topic->toppars->nelts;
    		break;

    	case LUA_TSTRING:
    		p = (u_char *) lua_tolstring(L, 2, &len);
    		partition = ngx_http_kafka_partition(p, len, topic->toppars->nelts);
    		break;

    	default:
            return luaL_argerror(L, 2, "expect string or nil");
    }

    toppar = ngx_http_kafka_toppar_get_valid(topic, partition);
    if (toppar == NULL) {
    	lua_pushnil(L);
        lua_pushliteral(L, "broker not available");
        return 2;
    }

    r = ngx_http_lua_get_request(L);
    ctx = ngx_http_get_module_ctx(r, ngx_http_kafka_module);

    if (ctx == NULL) {
    	ctx = ngx_palloc(r->pool, sizeof(ngx_http_kafka_ctx_t));

    	if (ctx == NULL) {
    		lua_pushnil(L);
    		lua_pushliteral(L, "no enough memory");
    		return 2;
    	}

    	ctx->topic = topic;
    	ctx->toppar = toppar;
    }

    ngx_http_set_ctx(r, ctx, ngx_http_kafka_module);

    return ngx_http_kafka_lua_push(L);
}

static ngx_int_t
ngx_http_kafka_lua_push(lua_State *L)
{
	ngx_http_request_t			*r;
	ngx_http_kafka_main_conf_t	*kmcf;
	ngx_http_kafka_ctx_t		*ctx;
	ngx_kfk_toppar_t			*toppar;
	ngx_kfk_buf_t				*buf;
	ngx_str_t				     msg;
    ngx_int_t                    rc;
    size_t						 size, len;
    int32_t                     *crc;
    u_char						*p, *q;
    const char					*err;
    int 						 i, n, type;

    n = lua_gettop(L);

	size = 0;

	for (i = 3; i <= n; i++) {
		type = lua_type(L, i);
		switch (type) {
		case LUA_TNUMBER:
		case LUA_TSTRING:
			lua_tolstring(L, i, &len);
			size += len;
			break;

		case LUA_TNIL:
			size += sizeof("nil") - 1;
			break;

		case LUA_TBOOLEAN:
			if (lua_toboolean(L, i)) {
				size += sizeof("true") - 1;

			} else {
				size += sizeof("false") - 1;
			}

			break;

		case LUA_TLIGHTUSERDATA:
			if (lua_touserdata(L, i) == NULL) {
				size += sizeof("null") - 1;
				break;
			}

			continue;

		default:
			err = lua_pushfstring(L, "string, number, boolean, or nil "
					"expected, got %s", lua_typename(L, type));
			return luaL_argerror(L, i, err);
		}
	}

    r = ngx_http_lua_get_request(L);

 	kmcf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);

    if (size > kmcf->msg_max_size) {
		lua_pushnil(L);
		lua_pushfstring(L, "message is too long, at most %d bytes",
						(int)(kmcf->msg_max_size));
		return 2;
	}

    ctx = ngx_http_get_module_ctx(r, ngx_http_kafka_module);

 	toppar = ctx->toppar;

 	buf = toppar->free;

 	if (buf == NULL) {
 		buf = ngx_http_kafka_get_buf(ngx_kfk, &ngx_kfk->free);

 		if (buf == NULL) {
			lua_pushnil(L);
			lua_pushliteral(L, "no bufs");
			return 2;
 		}

 		if (buf->no_pool) {
 			if (ngx_http_kafka_init_buf_pool(ngx_kfk, buf) != NGX_OK) {
 				lua_pushnil(L);
 				lua_pushliteral(L, "no enough memory");
 				return 2;
 			}
 		}

 		toppar->free = buf;
 	}

    p = ngx_pnalloc(buf->pool, NGX_KFK_MSG_HEADER_SIZE + size);
    if (p == NULL) {
        lua_pushnil(L);
        lua_pushliteral(L, "no enough memory");
        return 2;
    }

    msg.data = p;
    msg.len = NGX_KFK_MSG_HEADER_SIZE + size;

    /* offset */
    *(int64_t *)p = 0;
    p += sizeof(int64_t);

    /* msg size */
    *(int32_t *)p = (int32_t)htonl(14 + len);
    p += sizeof(int32_t);

    /* crc(later calculate) */
    crc = (int32_t *)p;
    p += sizeof(int32_t);

    /* magic */
    *(int8_t *)p = 0;
    p += sizeof(int8_t);

    /* attr */
    *(int8_t *)p = 0;
    p += sizeof(int8_t);

    /* key len */
    *(int32_t *)p = (int32_t)htonl(-1);
    p += sizeof(int32_t);

    /* value len */
    *(int32_t *)p = (int32_t)htonl(len);
    p += sizeof(int32_t);

	for (i = 3; i <= n; i++) {
		type = lua_type(L, i);
		switch (type) {
		case LUA_TNUMBER:
		case LUA_TSTRING:
			q = (u_char *) lua_tolstring(L, i, &len);
			p = ngx_copy(p, q, len);
			break;

		case LUA_TNIL:
			*p++ = 'n';
			*p++ = 'i';
			*p++ = 'l';
			break;

		case LUA_TBOOLEAN:
			if (lua_toboolean(L, i)) {
				*p++ = 't';
				*p++ = 'r';
				*p++ = 'u';
				*p++ = 'e';

			} else {
				*p++ = 'f';
				*p++ = 'a';
				*p++ = 'l';
				*p++ = 's';
				*p++ = 'e';
			}

			break;

		case LUA_TLIGHTUSERDATA:
			*p++ = 'n';
			*p++ = 'u';
			*p++ = 'l';
			*p++ = 'l';

			break;

		default:
			ngx_http_kafka_recycle_msg(buf->pool, &msg);
			return luaL_error(L, "impossible to reach here");
		}
	}

	if (p - msg.data > (off_t)(NGX_KFK_MSG_HEADER_SIZE + size)) {
	    ngx_http_kafka_recycle_msg(buf->pool, &msg);
	    return luaL_error(L, "buffer error: %d > %d", (int) (p - msg.data),
	                      (int) size);
	}

    *crc = (int32_t)htonl(ngx_crc32_long((u_char*)(crc + 1), 10 + size));

	if (buf->cnt == -1) {
		if (ngx_http_kafka_init_chain(buf, ctx) != NGX_OK) {
			ngx_http_kafka_recycle_msg(buf->pool, &msg);

			lua_pushnil(L);
			lua_pushliteral(L, "no enough memory");
			return 2;
		}
	}

	ctx->msg = &msg;

	rc = ngx_http_kafka_enq_buf(r, ctx);

	if (rc != NGX_OK) {
		lua_pushnil(L);
		lua_pushliteral(L, "internal error");
		return 2;
	}

	lua_pushinteger(L, 1);
	return 1;
}
