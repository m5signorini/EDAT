/*
Project: EDAT Lab 3 test program
File:    table.h
Author:  Simone Santini
Rev.     1.0
Date:    10/13/2019  (check it out: it's Sunday!
and I am here, working for you.
Praise to the commmitted professor.)

File in which YOU (Yes: you!) have to implement the functions defined in
the file table.h. The functions are defined in this file, but they are
elft empty or return dummy values. It is up to you to implement them
to guarantee the functionality expressed in table.h
*/
#include "table.h"
#include "type.h"


/*
This is the structure that contains the data relative to a table. You
have to implement it. Keep in mind that all the information about the table
that the functions have is in this structure, so you must put in there
all that is needed for the correct work of the functions
*/

struct table_ {
  int ncols;
  type_t* types;
  FILE* file;
  long first_pos;
  long last_pos;
  void** values;
};


/*
void table_create(char* path, int ncols, type_t* types);

Stores an empty table in a newly created file.

Note that this function does not return any value nor does it do anything
in memory. It creates a new file, stores in it a header that indicates
the number of columns, the types of these columns, and that the table
has 0 records. Then closes the file and returns.

Parameters:
path:    path name (referred to the current directory) of the file
where the table is to be stored.
ncols:   number of columns of the table.
types:   array of ncols elements of type type_t with the type of each
one of the columns (see type.h and type.c for details on the
types recognized by the table).

Returns:
1: table created
0: error in creation

WARNING: if the file specified in path already exists, this function
erases it and creates a new one. That is, all the data contained in the
file will be lost.
*/
int table_create(char* path, int ncols, type_t* types) {
  if(path == NULL || ncols <= 0 || types == NULL) return 0;
  FILE* pf = NULL;

  pf = fopen(path, "wb");
  if(pf == NULL) {
    return 0;
  }

  if(fwrite(&ncols, sizeof(int), 1, pf) != 1) {
    fclose(pf);
    return 0;
  }
  if(fwrite(types, sizeof(type_t), ncols, pf) != ncols) {
    fclose(pf);
    return 0;
  }

  fclose(pf);
  return 1;
}

/*
table_t* table_open(char* path)

Opens a table and returns the structure necessary to use it. The file
<path> must exist for this function to succeeds. This functions
allocates a table_t structure and fills in the necessary fields so that
the other functions defined here can operate on the table.

Parameters:
path:   path name (referred to the current directory) of the file
where the table is stored. The file must exist.

Returns:
A pointer to a newly allocated table_t structure with the information
necessary to operate on the table (the table is NOT read in memory), or
NULL is the file <path> does not exist.

NOTE: The calling program should not release the structure returned
by this function. Use table_close instead.
*/
table_t* table_open(char* path) {
  if(path == NULL) return NULL;

  table_t* tb = NULL;
  int i;

  tb = (table_t*)malloc(sizeof(table_t));
  if(tb == NULL) {
    return NULL;
  }

  tb->file = fopen(path, "rb+");
  if(tb->file == NULL) {
    free(tb);
    return NULL;
  }

  fseek(tb->file, 0, SEEK_SET);

  /* Get ncols from the file */
  /*Vamos a guardar en tb->cols 1 cosa de tamaño int del file tb->file*/
  if(fread(&tb->ncols, sizeof(int), 1, tb->file) != 1){
    fclose(tb->file);
    free(tb);
    return NULL;
  }

  /* Buffer set to null */
  tb->values = malloc(sizeof(void*)*tb->ncols);
  if(tb->values == NULL){
    fclose(tb->file);
    free(tb);
    return NULL;
  }

  for(i = 0; i < tb->ncols; i++){
    tb->values[i] = NULL;
  }

  /* Set types array based on ncols */
  tb->types = (type_t*)malloc(sizeof(type_t)*tb->ncols);
  if(tb->types == NULL) {
    fclose(tb->file);
    free(tb->values);
    free(tb);
    return NULL;
  }

  if(fread(tb->types, sizeof(type_t), tb->ncols, tb->file) != tb->ncols){
    fclose(tb->file);
    free(tb->types);
    free(tb->values);
    free(tb);
  }

  /* Set first pos */
  tb->first_pos = ftell(tb->file);

  /* Set last_pos */
  fseek(tb->file, 0, SEEK_END);
  tb->last_pos = ftell(tb->file);
  fseek(tb->file, 0, SEEK_SET);

  return tb;
}

/*
void table_close(table_t* table);

Closes a table freeing all the resources allocated. This function must
leave the whole system in the state it was before the table was opened:
all files closed, all memory released.

Note that after calling this function, it will no longer be possible
to operate on the table <table>.

Parameters:
table:  The table that we eant to close.

Returns:
Nothing
*/
void table_close(table_t* table) {
  if(table == NULL) return;

  int i;

  for(i = 0; i < table->ncols; i++){
    if(table->values[i] != NULL){
      free(table->values[i]);
    }
  }
  free(table->values);

  /* Free types */
  if(table->types != NULL) {
    free(table->types);
  }
  /* Close file */
  if(table->file != NULL) {
    fclose(table->file);
  }

  free(table);
  return;
}

/*
int table_ncols(table_t* table);

Returns the number of columns of the table <table>

Parameters:
table:  The table on which we want to operate.

Returns:
n>0:    The table has n columns
n<0:    Incorrect parameter
*/
int table_ncols(table_t* table) {
  if(table == NULL) return -1;
  return table->ncols;
}


/*
type_t *table_types(table_t* table);

Returns an array containing the types of the columns of the table
<table>.

Parameters:
table:  The table on which we want to operate.

Returns:
An array of table_ncols(table) element. Each element is of type type_t,
and contains the type of the corresponding column. For the definition
of type_t, see the file type.h/type.c. Returns NULL if the parameter
is invalid.

WARNING: The array that is returned is not a copy of the one used
internally by these functions, but a pointer to the same array. The
caller should not free the pointer returned by this function nor
should it modify it in any way.
*/
type_t *table_types(table_t* table) {
  if(table == NULL) return NULL;
  return table->types;
}

/*
long table_first_pos(table_t* table);

Returns the position of the file where the first record begin. Calling
table_read_record with this value as position will result in reading
the first retypecord of the table (see the example at the beginning of this
file.

Parameters:
table:  The table on which we want to operate.

Returns:
n>0:    the first record begins at position n in the file
n<0:    error in the parameter
*/
long table_first_pos(table_t* table) {
  if(table == NULL) return -1L;
  return table->first_pos;
}

/*
long table_last_pos(table_t* table);

Returns the last position of the file, that is, the position where a new
record will be inserted upon calling table_insert_record. Note that
table_insert_record does not use this function, which is used simply to
give information to the calling program.

Parameters:
table:  The table on which we want to operate.

Returns:
n>0:    the new record begins at position n in the file
n<=0:   error in the parameter
*/
long table_last_pos(table_t* table) {
  if(table == NULL) return -1L;
  return table->last_pos;
}

/*
long table_read_record(table_t* table, long pos);

Reads a record that begins at a given position in the table file.

Parameters:
table:  The table on which we want to operate.
pos:    Position in the file where the record begins. The pos-th byte
in the file must be the beginning of a record; if it is not, the
result of the call will be unpredictable.

Returns:
n>0:     The next record in the file begins at position n
n<0:     No record read, we had already reached the end of the file

Note: this function reads the record, but it returns no data from that
record. Use the function table_get_col to read the data of the record
after it has been read.
*/
long table_read_record(table_t* t, long pos) {
  if(t == NULL || pos < t->first_pos || t->file == NULL) return -1L;

  FILE* pf = t->file;
  char* buf = NULL;
  char* aux = NULL;
  int tam = 0;
  int i;

  /* Set beginning of file's regs */
  fseek(pf, pos, SEEK_SET);

  /* Get reg tam */
  if(fread(&tam, sizeof(int), 1, pf) != 1) {
    /* Couldn't read or EOF reached */
    return -1L;
  }

  buf = (char*)malloc(sizeof(char)*tam);
  if(buf == NULL) {
    return -1L;
  }
  /* Read to buffer */
  if(fread(buf, sizeof(char), tam, pf) != tam) {
    free(buf);
    return -1L;
  }
  /* Free previous values (record) */
  /* Save buffer in values*/
  aux = buf;
  for(i = 0; i < t->ncols; i++) {
    if(t->values[i] != NULL) {
      free(t->values[i]);
    }
    t->values[i] = value_set(t->types[i], aux);
    aux += value_length(t->types[i], t->values[i]);
  }

  free(buf);

  return ftell(pf);
}


/*
void *table_get_col(table_t* table, int col)

Returns the pointer to the data contained in the col-th column of the
record currently in memory. The record must have been previpusly read
using table_read_record. If no record was read in memory, the result
will be unpredictable.

Parameters:
table:  The table on which we want to operate.
col:    The column that we want to read, 0<=col<ncol,

Returns:
A pointer to the data that is contained in the column, or NULL if the
column number is out of range. The way the data are interpreted
depends on the type of the column, as specified by the col-th element
of the array returned by table_types (see the example at the beginning
of the file).
*/
void *table_get_col(table_t* table, int col) {
  if(table == NULL || col >= table->ncols || col < 0 || table->values == NULL) return NULL;
  return table->values[col];
}

/*  void table_insert_record(table_t* table, void** values);

Inserts a record at the end of the file given the pointers to the
values of each column.

Parameters:
table:  The table on which we want to operate.
values: Array of ncol pointers to the data that are to be stored in the
record. The element values[i] must be a pointer to a datum of the
same type as the i-th column of the file. If this constraint is
not respected, the results will be unpredictable.

Returns:
1: inserted OK
0: error
*/
int table_insert_record(table_t* t, void** values) {
  if(t == NULL || values == NULL || t->file == NULL){
    return 0;
  }

  FILE* pf = t->file;
  int tam = 0;
  int i;
  void* value = NULL;

  /*Gets the size of the record*/
  for(i = 0; i < t->ncols; i++){
    tam += value_length(t->types[i], values[i]);
  }

  fseek(pf, t->last_pos, SEEK_SET);

  /*Write the size of the record*/
  if(fwrite(&tam, sizeof(int), 1, pf) != 1){
    return 0;
  }

  /*Writes the record itself*/
  for(i = 0; i < t->ncols; i++){
    /*Writes data from the array pointed to, by values[i] to pf*/
    if (fwrite(values[i], value_length(t->types[i], values[i]), 1, pf) != 1){
      return 0;
    }
  }

  t->last_pos = ftell(pf);

  return 1;
}
