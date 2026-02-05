#ifndef __DEFINES__
#define __DEFINES__

typedef enum DataType_t {
  INT_32 = 0,
  FLOAT,
  CHAR_ARRAY
} DataType_t;



typedef struct Table_t {
  int64_t num_lines;
  int32_t num_columns;
  DataType_t *column_type;
  void **columns;
} Table_t;




#endif
