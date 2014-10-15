/*
 * debug.c
 *
 *  Created on: 2014年10月10日
 *      Author: yw
 */

#include "debug.h"

#if (!NGX_HAVE_VARIADIC_MACROS)

void
debug(ngx_log_t *log, const char *fmt, ...)
{
    va_list  args;

    if (log->log_level >= NGX_LOG_DEBUG) {
        va_start(args, fmt);
        ngx_log_error_core(NGX_LOG_DEBUG, log, 0, fmt, args);
        va_end(args);
    }
}

#endif
