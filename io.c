#include "io.h"


Table_t * read_table(FILE *table_file){

  Table_t *result_table = (Table_t *) malloc(sizeof(Table_t));


  if (table_file == 0x0) {
    printf("io:read_table Empty table file!\n");
    exit(1);
  }

  char line[1024];
  int64_t num_lines = 0;
  int32_t num_columns = 0;

  // Number of lines and columns
  fgets(line, sizeof(line), table_file);
  sscanf(line, "%ld %d", &num_lines, &num_columns);

  // Alocate column pointers
  result_table->num_lines = num_lines; 
  result_table->num_columns = num_columns;
  result_table->column_type = (DataType_t * ) malloc(sizeof(DataType_t) * num_columns);
  result_table->columns = (void **) malloc(sizeof(void *) * num_columns);



  
  // Column types
  fgets(line, sizeof(line), table_file);
  char * value = strtok(line, "|");
  int32_t column_id = 0;

  while(value != NULL) {

    int type = atoi(value);
    result_table->column_type[column_id] = type;


    switch(type) {
      case INT_32:
      result_table->columns[column_id] = (void *) malloc(sizeof(int32_t) * num_lines);
        break;

      case FLOAT:
        result_table->columns[column_id] = (void *) malloc(sizeof(float) * num_lines);
        break;

      case CHAR_ARRAY:
        result_table->columns[column_id] = (void *) malloc(sizeof(char *) * num_lines);
        break;

    }


    value = strtok(NULL, "|");
    column_id++;
  }
  


  // Values
  while(fgets(line, sizeof(line), table_file) != NULL) {
    column_id = 0;
    value = strtok(line, "|");

    while(value != NULL) {

      switch(result_table->column_type[column_id]) {
        case INT_32:
          ((int32_t *)result_table->columns)[column_id] = atoi(value); 
          break;

        case FLOAT:
          ((float *)result_table->columns)[column_id] = atof(value);
          break;

        case CHAR_ARRAY:
          ((char **)result_table->columns)[column_id] = (char *) malloc(sizeof(char) * (strlen(value) + 1));
          strcpy(((char **)result_table->columns)[column_id], value);
          break;

      }

      
      column_id++;
      value = strtok(NULL, "|");
    }
  }
 

  return result_table;
}

void print_table(Table_t *table_pointer){
  int64_t num_lines = table_pointer->num_lines;
  int32_t num_columns = table_pointer->num_columns;
  
  for (int64_t l=0; l < num_lines; ++l) {
    for (int32_t c=0; c < num_columns; ++c) {
      switch(table_pointer->column_type[num_columns]) {
        case INT_32:
          printf("%d ", ((int32_t **)table_pointer->columns)[c][l]);
          break;

        case FLOAT:
          printf("%f ", ((float **)table_pointer->columns)[c][l]);

          break;

        case CHAR_ARRAY:
          printf("%s ", ((char ***)table_pointer->columns)[c][l]);

          break;
      }

      printf("|");


    }
    printf("\n");

  } 
}
