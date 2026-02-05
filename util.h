#ifndef __UTIL__
#define __UTIL__
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "defines.h"

int64_t *generate_full_table_indexes(Table_t *table);
void print_indexes(int64_t * indexes);
int64_t *copy_indexes(int64_t * indexes);

#endif
