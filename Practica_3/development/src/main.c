/******************************************************************************

*******************************************************************************/
#include <stdio.h>
#include <string.h>
#include "import.h"
#include "cmds.h"
#include "table.h"
#include "type.h"

/*
Simple test: receives a command line such as

3 INT STR int

Creates a table with thiose columns and types, opens it,
and prints some information about it.
*/
void simple_test(int argc, char **argv) {
  int cols = atoi(argv[0]);
  if (cols <=0) {
    printf("Number of columns must be positive\n");
    exit(1);
  }
  if (argc-1 != cols) {
    printf("Error: %d columns specified, %d types given\n", cols, argc-1);
    return;
  }
  type_t *types = (type_t *) malloc(cols*sizeof(type_t));
  for (int i=0; i<cols; i++) {
    types[i] = type_parse(argv[i+1]);
  }
  table_create("test_tab.dat", cols, types);
  table_t *t = table_open("test_tab.dat");
  printf("Testing the basic functions of a table\n");
  printf("Number of columns: %d\n", table_ncols(t));
  printf("Types: ");
  type_t *tps = table_types(t);
  for (int i=0; i<table_ncols(t); i++) {
    printf("%s  ", type_to_str(tps[i]));
  }
  printf("\n");
  printf("First position: %ld (%02XH)\n", table_first_pos(t), (unsigned int) table_first_pos(t));
  printf("Last position: %ld (%02XH)\n", table_last_pos(t), (unsigned int) table_last_pos(t));
  table_close(t);
}

void test2() {
  table_t *table = NULL;

  table = table_open("test_tab.dat");

  int ncols = table_ncols(table);
  type_t *tp = table_types(table);
  long pos = table_first_pos(table);
  int i, n = 7;
  void *val = NULL;
  void *values[2];

  values[0] = (void*)&n;
  values[1] = (void*)&n;

  /*printf("%d, %d\n", *(int*)values[0], *(int*)values[1]);
  table_close(table);
  return;*/

  if(table_insert_record(table, values) == 0) {
    printf("%d, %d\n", *(int*)values[0], *(int*)values[1]);
    table_close(table);
    return;
  }
  while (pos > 0) {
    pos = table_read_record(table, pos);
    if(pos < 0) break;
    printf("%ld (POS)\n", pos);
    for (i = 0; i < table_ncols(table); i++){
      val = table_get_col(table, i);
      print_value(stdout, tp[i], val);
      printf("  ");
    }
    printf("\n");
  }
  table_close(table);
}


int main(int argc, char **argv)
{
  if (argc>1) {
    simple_test(argc-1, argv+1);
    test2();
  }
  else {
    cmdstatus *cs = c_create();

    c_key_init(cs);
    c_print_status(cs);
    while(1) {
      char *cmd = c_cmd_get(40);
      printf("\n");
      if (!strncmp(cmd, "quit", strlen("quit"))) {
        c_key_restore(cs);
        printf("bye!\n");
        exit(0);
      }

      c_execute(cs, cmd);
      c_print_status(cs);
    }
  }
  return 0;
}
