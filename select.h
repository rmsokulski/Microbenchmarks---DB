#include "header.h"
#ifndef __SELECT_OPERATOR__
#define __SELECT_OPERATOR__

void select_gt_float(float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_ge_float(float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_lt_float(float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_le_float(float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
#endif 
