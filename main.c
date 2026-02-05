#include "operators.h"
#include "defines.h"
#include "io.h"
#include "util.h"
int main() {
  Table_t *data_table;

  FILE * input_table_file = fopen("input.tbl", "r");
  data_table = read_table(input_table_file);
  fclose(input_table_file);

  print_table(data_table);

  printf("Applying the select operator:\n");
  printf("    Generating indexes:\n");
  int64_t *indexes = generate_full_table_indexes(data_table);
  print_indexes(indexes);

  printf("    Alocating result index set\n");
  int64_t *result_indexes = copy_indexes(indexes);
  print_indexes(result_indexes);


  printf("   Selection (2º column greater than 15)\n");
  select_gt_float(data_table->columns[1], indexes, result_indexes, 15.0);

  printf("    Indexes after selection\n");
  print_indexes(result_indexes);

  printf("Applying the projection operator:\n");
  Table_t * result_table = projection(result_indexes, 2, FLOAT, data_table->columns[1], CHAR_ARRAY, data_table->columns[2]);
  printf("    Resulting table:\n");
  print_table(result_table);


  return 0;
}
