#include "operators.h"
#include "defines.h"
#include "io.h"
int main() {
  Table_t *data_table;

  FILE * input_table_file = fopen("input.tbl", "r");
  data_table = read_table(input_table_file);
  fclose(input_table_file);

  print_table(data_table);

  return 0;
}
