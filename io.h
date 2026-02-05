#ifndef __IO__
#define __IO__
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "defines.h"

Table_t * read_table(FILE *);
void print_table(Table_t *);

#endif
