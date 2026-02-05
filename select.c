#include "select.h"


void select_gt_float(float *column, int64_t *indexes, int64_t * new_indexes, float comparison){
  int64_t i   = 0;
  int64_t idx = 0;
  new_indexes[0]  = 0;

  for (i=1; i <= indexes[0]; ++i) {
    float value = column[indexes[i]];
    if (value > comparison) {
      new_indexes[0]++;
      new_indexes[new_indexes[0]] = indexes[i];
    } 
  } 
}

void select_ge_float(float *column, int64_t *indexes, int64_t * new_indexes, float comparison) {
  int64_t i   = 0;
  int64_t idx = 0;
  new_indexes[0]  = 0;

  for (i=1; i <= indexes[0]; ++i) {
    float value = column[indexes[i]];
    if (value >= comparison) {
      new_indexes[0]++;
      new_indexes[new_indexes[0]] = indexes[i];
    } 
  } 
}
void select_lt_float(float *column, int64_t *indexes, int64_t * new_indexes, float comparison) {
  int64_t i   = 0;
  int64_t idx = 0;
  new_indexes[0]  = 0;

  for (i=1; i <= indexes[0]; ++i) {
    float value = column[indexes[i]];
    if (value < comparison) {
      new_indexes[0]++;
      new_indexes[new_indexes[0]] = indexes[i];
    } 
  } 
}
void select_le_float(float *column, int64_t *indexes, int64_t * new_indexes, float comparison) {
  int64_t i   = 0;
  int64_t idx = 0;
  new_indexes[0]  = 0;

  for (i=1; i <= indexes[0]; ++i) {
    float value = column[indexes[i]];
    if (value <= comparison) {
      new_indexes[0]++;
      new_indexes[new_indexes[0]] = indexes[i];
    } 
  } 
}
