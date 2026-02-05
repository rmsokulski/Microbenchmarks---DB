#include "projection.h"
#define DEBUG 0


Table_t * projection(int64_t *indexes, int32_t num_columns, ...){
  va_list args;
  va_start(args, num_columns);

  // Build the column data structure
  
  // Build the table
  Table_t *materialized_table = (Table_t *) malloc(sizeof(Table_t));
  materialized_table->num_lines = indexes[0];
  materialized_table->num_columns = num_columns;
  materialized_table->columns = (void **) malloc(sizeof(void *) * num_columns);
  

  materialized_table->column_type = (DataType_t *) malloc(sizeof(DataType_t) * num_columns);

  #if DEBUG == 1
  printf("Structures allocated\n");
  #endif

  for (int32_t col_idx=0; col_idx < num_columns; ++col_idx) {
    DataType_t type = va_arg(args, DataType_t);
    void * column_pointer = va_arg(args, void *);

    // Alocate materialization
    void * mat_column = NULL;
    switch (type) {
      case INT_32:
        mat_column = (int32_t *) malloc(sizeof(int32_t)*indexes[0]);
        break;

      case FLOAT:
        mat_column = (float *) malloc(sizeof(float)*indexes[0]);
        break;

      case CHAR_ARRAY:
        mat_column = (char **) malloc(sizeof(char *)*indexes[0]);
        break;

    }
    #if DEBUG == 1
    printf("Materialization allocated\n");
    #endif
    // Save column type
    materialized_table->column_type[col_idx] = type;

    // Fill materialized table
    for (int64_t ind_idx=1; ind_idx <= indexes[0]; ++ind_idx) {
      int64_t old_index = indexes[ind_idx];
      int64_t new_index = ind_idx-1;
       
      switch (type) {
        case INT_32:
          ((int32_t *) mat_column)[new_index] = ((int32_t *) column_pointer)[old_index];
          break;

        case FLOAT:

          ((float *) mat_column)[new_index] = ((float *) column_pointer)[old_index];
          #if DEBUG == 1
          printf("Value %f [from %p vector] copied into: %p (%f)\n",((float *) column_pointer)[old_index], (void *)column_pointer, &((float *) mat_column)[new_index],((float *) mat_column)[new_index]);
          #endif
          break;

        case CHAR_ARRAY:

          ((char **) mat_column)[new_index] = ((char **) column_pointer)[old_index];
          break;

      }
      #if DEBUG == 1
      printf("Moving data from (%ld; %d) => (%ld; %d)\n", old_index, col_idx, new_index,col_idx);
      #endif

    }

    // Link with the structure of the materialized table
    materialized_table->columns[col_idx] = mat_column;
    
  }

  #if DEBUG == 1
  printf("Results:");
  printf("[0,0]: %f\n", ((float **)materialized_table->columns)[0][0]);
  #endif

  // Return the materialized results
  return materialized_table;
}
