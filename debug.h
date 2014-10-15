/*
 * debug.h
 *
 *  Created on: 2014年10月10日
 *      Author: yw
 */

#ifndef DEBUG_H_
#define DEBUG_H_

#include <ngx_config.h>
#include <ngx_core.h>

/*********************************/

#if (NGX_HAVE_C99_VARIADIC_MACROS)

#define NGX_HAVE_VARIADIC_MACROS  1

#define debug(log, ...)                                        \
    if ((log)->log_level >= NGX_LOG_DEBUG)					   \
		ngx_log_error_core(NGX_LOG_DEBUG, log, 0, __VA_ARGS__)

/*********************************/

#elif (NGX_HAVE_GCC_VARIADIC_MACROS)

#define NGX_HAVE_VARIADIC_MACROS  1

#define debug(log, args...)                                    \
    if ((log)->log_level >= NGX_LOG_DEBUG) 						\
		ngx_log_error_core(NGX_LOG_DEBUG, log, 0, args)

/*********************************/

#else /* NO VARIADIC MACROS */

#define NGX_HAVE_VARIADIC_MACROS  0

void debug(ngx_log_t *log, const char *fmt, ...);


#endif /* VARIADIC MACROS */


#endif /* DEBUG_H_ */
