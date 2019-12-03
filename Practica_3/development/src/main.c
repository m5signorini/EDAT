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

void test_type() {
  type_t types[4] = {INT, STR, LLONG, DBL};
  char tstr[4][8] = {"INT", "STR", "LLONG", "DBL"};
  int sizes[4] = {sizeof(int), sizeof(char)*5, sizeof(long long), sizeof(double)};
  int a = 7;
  char b[5] = "HOLA";
  long long c = 1234567890;
  double d = 3.1416;
  int i;
  char lit[3] = "77";
  void *value = NULL;

  void* values[4] = {&a, &b, &c, &d};

  for(i = 0; i < 4; i++){
    if(value_length(types[i], values[i]) != sizes[i]) {
      printf("Error en value_length\n");
    }
    print_value(stdout, types[i], values[i]);
    printf("\n");

    if(value_cmp(types[i], values[i], values[i]) != 0) {
      printf("Error en value_cmp\n");
    }
    /**/
    if(type_parse(tstr[i]) != types[i]) {
        printf("Error en type_parse\n");
    }
    if(strcmp(type_to_str(types[i]), tstr[i]) != 0){
        printf("Error en type_to_str\n");
    }
    value = value_parse(types[i], lit);
    if(value == NULL) {
      printf("Error en value_parse\n");
    }
    switch(types[i]) {
      case INT:
        if(*(int*)value != 77) {
          printf("Error en value_parse\n");
        }
        break;
      case LLONG:
        if(*(long long*)value != 77) {
          printf("Error en value_parse\n");
        }
        break;
      case DBL:
        if(*(double*)value != 77) {
          printf("Error en value_parse\n");
        }
        break;
      case STR:
      if(strcmp(value, "77") != 0){
        printf("Error en value_parse\n");
      }
      break;
    }
  }
}


int main(int argc, char **argv)
{
  if (argc>1) {
    /*simple_test(argc-1, argv+1);*/
    test_type();
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
