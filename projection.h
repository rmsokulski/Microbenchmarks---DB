#ifndef __PROJECTION_OPERATOR__
#define __PROJECTION_OPERATOR__
#include "header.h"
#include "defines.h"
#include <inttypes.h>
#include <stdarg.h>


Table_t * projection(uint64_t *indexes, uint64_t num_columns, ...);
#endif
