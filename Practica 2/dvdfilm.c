#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"


int param_error();
int remove_film(char* f_id);

int main(int argc, char** argv) {

  /* CHECK COMMAND LINE */
  if(argc < 2) {
    fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
    fprintf(stderr, "./dvdfilm remove <Film_id>\n");

    return EXIT_FAILURE;
  }

  /* CASO REMOVE */
  if(strcmp(argv[1], "remove") == 0) {
    if(argc < 3) return param_error();
   /* QUERY USING PARAMETERS */
   return remove_film(argv[2]);
  }

  else {
    return param_error();
  }

  return EXIT_FAILURE;
}


int remove_film(char* f_id){

  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[1024];

  SQLCHAR film_id[512];

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)){
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  /* First we check that the film is in the database */
  sprintf(query, "SELECT film.film_id "
                 "FROM film "
                 "WHERE film.film_id = %s;", f_id);

  SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

  SQLBindCol(stmt, 1, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
  ret = SQLFetch(stmt);

  /* If the film does not exist in the database we cannot remove the film */
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stdout, "ERROR: film not found.\n");
    SQLCloseCursor(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    ret = odbc_disconnect(env, dbc);
    if(!SQL_SUCCEEDED(ret)) {
      return EXIT_FAILURE;
    }
    return EXIT_FAILURE;
  }

    SQLCloseCursor(stmt);

    /* Now we remove the film from the film_actor table */
    sprintf(query, "DELETE FROM film_actor "
                   "WHERE film_actor.film_id = %s;", f_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: film could not be removed from film_actor table.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* Now we remove the film from the film_category table */
    sprintf(query, "DELETE FROM film_category "
                   "WHERE film_category.film_id = %s;" f_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: film could not be removed from film_category table.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* Now we remove the film from the inventory table */
    sprintf(query, "DELETE FROM inventory "
                   "WHERE inventory.film_id = %s;", f_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: film could not be removed from inventory table.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* Now we remove the film from the film table */
    sprintf(query, "DELETE FROM film "
                   "WHERE film.film_id = %s;" f_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: film could not be removed from film table.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);
    /* Free up statement handle */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* DISCONNECT */
    ret = odbc_disconnect(env, dbc);
    if(!SQL_SUCCEEDED(ret)) {
      return EXIT_FAILURE;
    }

    fprintf(stdout, "Film deleted correctly\n");
    return EXIT_SUCCESS;
 }

 int param_error() {
   fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
   fprintf(stderr, "./dvdfilm remove <Film_id>\n");
   return EXIT_FAILURE;
 }
