#include <header.h>
#ifndef __SELECT_OPERATOR__
#define __SELECT_OPERATOR__

void select_gt_float(Column_Float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_ge_float(Column_Float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_lt_float(Column_Float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
void select_le_float(Column_Float *column, uint64_t *indexes, uint64_t * new_indexes, float comparison);
#endif 
