#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "type.h"

size_t
value_length(type_t type, void* value) {
  switch(type) {
    case INT:
      return sizeof(int);
    case STR:
      return (strlen((char*) value) + 1) * sizeof(char);
    case LLONG:
      return sizeof(long long);
    case DBL:
      return sizeof(double);
    default:
      return 0;
  }
}

void
print_value(FILE* f, type_t type, void* value) {
  switch(type) {
    case INT:
      fprintf(f, "%d", *((int*) value));
      break;
    case STR:
      fprintf(f, "%s", (char*) value);
      break;
    case LLONG:
      fprintf(f, "%lld", *((long long*) value));
      break;
    case DBL:
      fprintf(f, "%lf", *((double*) value));
      break;
  }
}


char *
type_to_str(type_t type) {
  switch(type) {
      case INT:
      return strdup("INT");
    case STR:
      return strdup("STR");
    case LLONG:
      return strdup("LLONG");
    case DBL:
      return strdup("DBL");
    default:
      return strdup("Invalid");
  }
}


int
value_cmp(type_t type, void* value1, void* value2) {
  switch(type) {
    case INT:
      return *((int*) value1) - *((int*) value2);
    case STR:
      return strcmp((char*) value1, (char*) value2);
    case LLONG:
      return *((long long*) value1) - *((long long*) value2);
    case DBL:
      return *((double*) value1) - *((double*) value2);
    default:
      return 0;
  }
}

type_t type_parse(char* type_name) {
  if (strcmp(type_name, "INT") == 0) {
    return INT;
  } else if (strcmp(type_name, "STR") == 0) {
    return STR;
  } else if (strcmp(type_name, "LLONG") == 0) {
    return LLONG;
  } else if (strcmp(type_name, "DBL") == 0) {
    return DBL;
  } else {
    return -1;
  }
}

void* value_parse(type_t type, char* literal) {
  void* value;

  switch(type) {
    case INT:
      value = malloc(sizeof(int));
      *((int*) value) = atoi(literal);
      break;
    case STR:
      value = malloc((strlen(literal) + 1) * sizeof(char));
      strcpy(value, literal);
      break;
    case LLONG:
      value = malloc(sizeof(long long));
      *((long long*) value) = atoi(literal);
      break;
    case DBL:
      value = malloc(sizeof(double));
      *((double*) value) = atoi(literal);
      break;
    default:
      value = NULL;
      break;
  }

  return value;
}


void* value_set(type_t type, char* buffer) {
  void* value;

  switch(type) {
    case INT:
      value = malloc(sizeof(int));
      *((int*) value) = *(int*)buffer;
      break;
    case STR:
      value = malloc((strlen(buffer) + 1) * sizeof(char));
      strcpy(value, buffer);
      break;
    case LLONG:
      value = malloc(sizeof(long long));
      *((long long*) value) = *(long long*)buffer;
      break;
    case DBL:
      value = malloc(sizeof(double));
      *((double*) value) = *(double*)buffer;
      break;
    default:
      value = NULL;
      break;
  }

  return value;
}
