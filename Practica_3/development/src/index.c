#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "index.h"

typedef struct record_ {
  int key;
  int n_regs;  /* Number of registers associated to this key */
  long* registers;
} record;

struct index_ {
  FILE* file;
  type_t type;
  int n_keys;
  record* entries;
};




/*
   Function: int index_create(char *path, int type)

   Creates a file for saving an empty index. The index is initialized
   to be of the specific tpe (in the basic version this is always INT)
   and to contain 0 entries.

   Parameters:
   path:  the file where the index is to be created
   type:  the type of the index (always INT in this version)

   Returns:  return NULL;
   1:   index created
   0:   parameter error or file creation problem. Index not created.
 */
int index_create(char *path, type_t type) {
  if(path == NULL || type != INT) return 0;
  FILE* pf = NULL;
  int n_entries = 0;

  pf = fopen(path, "wb");
  if(pf == NULL) {
    return 0;
  }

  if(fwrite(&type, sizeof(type_t), 1, pf) != 1) {
    fclose(pf);
    return 0;
  }
  if(fwrite(&n_entries, sizeof(int), 1, pf) != 1) {
    fclose(pf);
    return 0;
  }

  fclose(pf);
  return 1;
}



/*
   Opens a previously created index: reads the contents of the index
   in an index_t structure that it allocates, and returns a pointer to
   it (or NULL if the files doesn't exist or there is an error).

   NOTE: the index is stored in memory, so you can open and close the
   file in this function. However, when you are asked to save the
   index, you will not be given the path name again, so you must store
   in the structure either the FILE * (and in this case you must keep
   the file open) or the path (and in this case you will open the file
   again).

   Parameters:
   path:  the file where the index is

   Returns:
   pt:   index opened
   NULL: parameter error or file opening problem. Index not opened.

 */
index_t* index_open(char* path) {
  if(path == NULL) return NULL;
  if(path == NULL) return NULL;

  index_t* index = NULL;
  int i;
  int size;

  index = (index_t*)malloc(sizeof(index_t));
  if(index == NULL) {
    return NULL;
  }

  index->file = fopen(path, "rb+");
  if(index->file == NULL) {
    free(index);
    return NULL;
  }

  fseek(index->file, 0, SEEK_SET);

  /* Get type from the file */
  if(fread(&index->type, sizeof(type_t), 1, index->file) != 1){
    fclose(index->file);
    free(index);
    return NULL;
  }
  /* Get n_keys from the file */
  if(fread(&index->n_keys, sizeof(int), 1, index->file) != 1){
    fclose(index->file);
    free(index);
    return NULL;
  }

  /* READ ENTRIES */
  if(index->n_keys == 0) {
    index->entries = NULL;
    return index;
  }

  index->entries = (record*)malloc(index->n_keys*sizeof(record));
  if(index->entries == NULL) {
    fclose(index->file);
    free(index);
    return NULL;
  }

  /* Set to null for easier erasing */
  for(i = 0; i < index->n_keys; i++) {
    index->entries[i].registers = NULL;
  }

  /* Add the entries */
  for(i = 0; i < index->n_keys; i++) {
    /* Read key */
    if(fread(&(index->entries[i].key), sizeof(int), 1, index->file) != 1) {
      index_close(index);
      return NULL;
    }
    /* Read n_regs */
    if(fread(&(index->entries[i].n_regs), sizeof(int), 1, index->file) != 1) {
      index_close(index);
      return NULL;
    }
    /* Read registers */
    size = index->entries[i].n_regs;
    index->entries[i].registers = (long*)malloc(size*sizeof(long));
    if(index->entries[i].registers == NULL) {
      index_close(index);
      return NULL;
    }
    if(fread(index->entries[i].registers, sizeof(long), size, index->file) != size) {
      index_close(index);
      return NULL;
    }
  }

  return index;
}


/*
   int index_save(index_t* index);

   Saves the current state of index in the file it came from. Note
   that the name of the file in which the index is to be saved is not
   given.  See the NOTE to index_open.

   Parameters:
   index:  the index the function operates upon

   Returns:
   1:  index saved
   0:  error saving the index (cound not open file) [][m][*][m+1][T][L]

*/
int index_save(index_t* idx) {
  if(idx == NULL) return 0;
  int i;

  fseek(idx->file, 0, SEEK_SET);

  if(fwrite(&idx->type, sizeof(type_t), 1, idx->file) != 1) {
    index_close(idx);
    return 0;
  }
  if(fwrite(&idx->n_keys, sizeof(int), 1, idx->file) != 1) {
    index_close(idx);
    return 0;
  }

  /* Add the entries */
  for(i = 0; i < idx->n_keys; i++) {
    /* Write key */
    if(fwrite(&(idx->entries[i].key), sizeof(int), 1, idx->file) != 1) {
      index_close(idx);
      return 0;
    }
    /* Write n_regs */
    if(fwrite(&(idx->entries[i].n_regs), sizeof(int), 1, idx->file) != 1) {
      index_close(idx);
      return 0;
    }
    /* Write registers */
    if(fwrite(idx->entries[i].registers, sizeof(long), idx->entries[i].n_regs, idx->file) != idx->entries[i].n_regs) {
      index_close(idx);
      return 0;
    }
  }

  return 1;
}


/*
   Function: int index_put(index_t *index, int key, long pos);

   Puts a pair key-position in the index. Note that the key may be
   present in the index or not... you must manage both situation. Also
   remember that the index must be kept ordered at all times.

   Parameters:
   index:  the index the function operates upon
   key: the key of the record to be indexed (may or may not be already
        present in the index)
   pos: the position of the corresponding record in the table
        file. This is the datum that we will want to recover when we
        search for the key.

   Return:
   n>0:  after insertion the file now contains n unique keys
   0:    error inserting the key

*/
int index_put(index_t *idx, int key, long pos) {
  if (idx == NULL || pos < 0) return 0;

  int P = 0;
  int U = idx->n_keys - 1;
  int m = 0, i;
  int size = 0;

  while (P <= U) {
    m = (P + U)/2;
    /*Key found*/
    if (idx->entries[m].key == key){
      size = idx->entries[m].n_regs;
      /*Search pos*/
      for (i = 0; i < size; i++){
        /*Register already added*/
        if (idx->entries[m].registers[i] == pos){
          return idx->n_keys;
        }
      }
      /*Register not added previosly, so we add it*/
      idx->entries[m].n_regs++;
      idx->entries[m].registers = (long*)realloc(idx->entries[m].registers, idx->entries[m].n_regs*sizeof(long));
      if (idx->entries[m].registers == NULL){
        idx->entries[m].n_regs--;
        return 0;
      }
      idx->entries[m].registers[idx->entries[m].n_regs-1] = pos;
      return idx->n_keys;
    }
    else if (key < idx->entries[m].key){
      U = m - 1;
    }
    else {
      P = m + 1;
    }
  }
  m = P;

  /*Key not found, we add the key*/
  idx->n_keys++;
  idx->entries = (record*)realloc(idx->entries, idx->n_keys*sizeof(record));
  if (idx->entries == NULL){
    idx->n_keys--;
    return 0;
  }
  /*We relocate all the keys, m is new key position*/
  for(i = idx->n_keys - 1; i > m; i--){
    idx->entries[i] = idx->entries[i-1];
    printf("%ld\n", idx->entries[i].registers[0]);
  }
  idx->entries[i].registers = (long*)malloc(sizeof(long));
  if(idx->entries[i].registers == NULL){
    return 0;
  }
  idx->entries[i].key = key;
  idx->entries[i].n_regs = 1;
  idx->entries[i].registers[0] = pos;

  return idx->n_keys;
}


/*
   Function: long *index_get(index_t *index, int key, int* nposs);

   Retrieves all the positions associated with the key in the index.

   Parameters:
   index:  the index the function operates upon
   key: the key of the record to be searched
   nposs: output paramters: the number of positions associated to this key

   Returns:

   pos: an array of *nposs long integers with the positions associated
        to this key
   NULL: the key was not found

   NOTE: the parameter nposs is not an array of integers: it is
   actually an integer variable that is passed by reference. In it you
   must store the number of elements in the array that you return,
   that is, the number of positions associated to the key. The call
   will be something like this:

   int n
   long **poss = index_get(index, key, &n);

   for (int i=0; i<n; i++) {
       Do something with poss[i]
   }

   ANOTHER NOTE: remember that the search for the key MUST BE DONE
   using binary search.

   FURTHER NOTE: the pointer returned belongs to this module. The
   caller guarantees that the values returned will not be changed.

*/
long *index_get(index_t *idx, int key, int* nposs) {
  if(idx == NULL || nposs == NULL) return NULL;

  int P = 0;
  int U = idx->n_keys-1;
  int m;
  /* Binary Search */
  while (P <= U) {
    m = (P + U)/2;
    /* Key found */
    if (idx->entries[m].key == key){
      *nposs = idx->entries[m].n_regs;
      return idx->entries[m].registers;
    }
    /* Continue search */
    else if (key < idx->entries[m].key){
      U = m - 1;
    }
    else {
      P = m + 1;
    }
  }
  *nposs = 0;
  return NULL;
}


/*
   Closes the index by freeing the allocated resources. No operation
   on the index will be possible after calling this function.

   Parameters:
   index:  the index the function operates upon

   Returns:
   Nothing

   NOTE: This function does NOT save the index on the file: you will
   have to call the function index_save for this.
*/
void index_close(index_t *idx) {
  if(idx == NULL) return;
  int i;

  for(i = 0; i < idx->n_keys; i++) {
    if(idx->entries[i].registers != NULL) {
      free(idx->entries[i].registers);
    }
  }
  free(idx->entries);
  fclose(idx->file);
  free(idx);
  return;
}


/*
  Function: long *index_get_order(index_t *index, int n, int* nposs);

  Function useful for debugging but that should not be used otherwise:
  returns the nth record in the index. DO NOT USE EXCEPT FOR
  DEBUGGING. The test program uses it.

   Parameters:
   index:  the index the function operates upon
   n: number of the record to be returned
   key: output parameter: the key of the record
   nposs: output parameter: the number of positions associated to this key

   Returns:

   pos: an array of *nposs long integers with the positions associated
        to this key
   NULL: the key was not found


   See index_get for explanation on nposs and pos: they are the same stuff
*/
long *index_get_order(index_t *index, int n, int *key, int* nposs) {
  if(index == NULL || n < 0 || n >= index->n_keys || key == NULL || nposs == NULL) return NULL;
  *key = index->entries[n].key;
  *nposs = index->entries[n].n_regs;
  return index->entries[n].registers;
}
