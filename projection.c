#include "projection.h"


Table_t * projection(uint64_t *indexes, uint64_t num_columns, ...){
  va_list args;
  va_start(args, num_columns);

  // Build the column data structure
  void **materialized_column_pointers = (void **) malloc(sizeof(void *) * num_columns);
  
  // Build the table
  Table_t *materialized_table = (Table_t *) malloc(sizeof(Table_t));
  materialized_table->num_lines = indexes[0];
  materialized_table->num_columns = num_columns;
  

  materialized_table->column_type = (DataType_t *) malloc(sizeof(DataType_t) * num_columns);

  for (int col_idx=0; col_idx < num_columns; ++col_idx) {
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
    
    // Save column type
    materialized_table->column_type[col_idx] = type;

    // Fill materialized table
    for (uint64_t ind_idx=1; ind_idx <= indexes[0]; ++ind_idx) {
      uint64_t index = indexes[ind_idx];
       
      switch (type) {
        case INT_32:
          ((int32_t *) mat_column)[ind_idx] = ((int32_t *) column_pointer)[index];
          break;

        case FLOAT:

          ((float *) mat_column)[ind_idx] = ((float *) column_pointer)[index];
          break;

        case CHAR_ARRAY:

          ((char **) mat_column)[ind_idx] = ((char **) column_pointer)[index];
          break;

      }

    }

    // Link with the structure of the materialized table
    materialized_column_pointers[col_idx] = mat_column;
    
  }
  materialized_table->columns = materialized_column_pointers;


  // Return the materialized results
  return materialized_table;
}
