#include "util.h"


int64_t *generate_full_table_indexes(Table_t *table) {

  int64_t *indexes = (int64_t * ) malloc(sizeof(int64_t) * table->num_lines + 1);

  indexes[0] = table->num_lines;
  for (int64_t i=0; i < table->num_lines; ++i) {
    indexes[i + 1] = i;
  }
  
  return indexes;
}

void print_indexes(int64_t * indexes) {
  printf("Index with %ld elements:\n", indexes[0]);
  for (int64_t i=1; i <= indexes[0]; ++i) {
    printf("Index[%ld]: %ld\n", i, indexes[i]);
  }
}


int64_t *copy_indexes(int64_t * indexes) {

  int64_t *indexes_copy = (int64_t * ) malloc(sizeof(int64_t) * indexes[0] + 1);

  for (int64_t i=0; i <= indexes[0]; ++i) {
    indexes_copy[i] = indexes[i];
  }
  
  return indexes_copy;
}
