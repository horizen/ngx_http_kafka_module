/*
 * ngx_http_kafka_util.c
 *
 *  Created on: 2014年9月22日
 *      Author: yw
 */

#include "ngx_http_kafka_util.h"

char *ngx_kfk_strerr[] = {
	/* kafka define err code */
	NULL,
	"Offset out of range",
	"Invalid message",
	"Unknown topic or partition",
	"Invalid message size",
	"Leader not available",
	"Not leader for partition",
	"Request timed out",
	"Broker not available",
	"Replica not available",
	"Message size too large",
	"Stale controller epoch code",
	"Offset metadata too large code",
	NULL,
	"Offsets load in progress code",
	"Consumer coordinator not available code",
	"Not coordinator for consumer code",

	/* self define err code */
	"Internal error",
	"Resolver error",
	"No resolver",
	"No enough memory",
	"No buffer",
	"Socket error",
	"Socket timedout",
	"Socket closed",
	"Message Timed out",
	"Invalid topic number",
	"Invalid partition number",
	"Invalid response message size",
	"Invalid error number",
	"Invalid response",
	"Broker down",
    "Nginx down"
};

int64_t
ngx_kfk_atoi(u_char *line, size_t n)
{
    int64_t  value;

    for (value = 0; n--; line++) {
        value = (value << 8) + *line ;
    }

    return value;
}
