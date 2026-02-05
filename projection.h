#ifndef __PROJECTION_OPERATOR__
#define __PROJECTION_OPERATOR__
#include "header.h"
#include "defines.h"
#include <inttypes.h>
#include <stdarg.h>


Table_t * projection(int64_t *indexes, int32_t num_columns, ...);
#endif
