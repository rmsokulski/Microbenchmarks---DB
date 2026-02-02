#ifndef __PROJECTION_OPERATOR__
#define __PROJECTION_OPERATOR__
#include <header.h>
#include <inttypes.h>
#include <stdarg.h>

typedef enum DataTypes {
  INT_32 = 0,
  FLOAT,
  CHAR_ARRAY
} DataTypes;

void projection(uint64_t *indexes, uint64_t num_columns, ...){
  va_list args;
  va_start(args, num_columns);

  for (int col_idx=0; col_idx < num_columns; ++col_idx) {
    DataTypes type = va_arg(args, DataTypes);
    void * column_pointer = va_arg(args, void *);

    // Alocate materialization
    void * mat_column = NULL;
    switch (type) {
      case INT_32:
        mat_column (int32_t *) malloc(sizeof(int32_t)*indexes[0]);
        break;

      case FLOAT:
        mat_column (float *) malloc(sizeof(float)*indexes[0]);
        break;

      case CHAR_ARRAY:
        mat_column (char **) malloc(sizeof(char *)*indexes[0]);
        break;

    }

    // Fill materialized table
    for (uint64_t ind_idx=1; ind_idx <= indexes[0]; ++ind_idx) {
      uint64_t index = indexes[ind_idx];
       
      switch (type) {
        case INT:
          (int32_t *) mat_column = (int32_t) column_pointer[index];
          break;

        case FLOAT:

          (float *) mat_column = (float) column_pointer[index];
          break;

        case CHAR_ARRAY:

          (char **) mat_column = (char *) column_pointer[index];
          break;

      }

    }
    switch (type) {
      case INT:
        
        break;

      case FLOAT:

        break;

      case CHAR_ARRAY:

        break;

    }
  }
}

#endif
