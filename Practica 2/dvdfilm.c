#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"


int param_error();
int err_disconnect(SQLHENV* env, SQLHDBC* dbc, SQLHSTMT* stmt);
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
                 "WHERE film.film_id = ?");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, f_id, 0, NULL);

  ret = SQLExecute(stmt);

  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetros <%s> no válidos\n", f_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
  ret = SQLFetch(stmt);

  /* If the film does not exist in the database we cannot remove the film */
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stdout, "ERROR: Película no encontrada.\n");
    return err_disconnect(&env, &dbc, &stmt);
  }

    SQLCloseCursor(stmt);


  /**/
       sprintf(query, "DELETE FROM payment "
                      "WHERE payment.rental_id IN (SELECT payment.rental_id "
                                                  "FROM payment, rental, inventory "
                                                  "WHERE payment.rental_id = rental.rental_id and "
                                                       "rental.inventory_id = inventory.inventory_id and "
                                                       "inventory.film_id = ?)");

       SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
       SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
       ret = SQLExecute(stmt);

       if(!SQL_SUCCEEDED(ret)){
         fprintf(stdout, "ERROR: Pago no eliminado.\n");
         return err_disconnect(&env, &dbc, &stmt);
       }



    SQLCloseCursor(stmt);


    sprintf(query, "DELETE FROM rental "
                   "WHERE rental.rental_id IN (SELECT rental.rental_id "
                                              "FROM rental, inventory "
                                              "WHERE rental.inventory_id = inventory.inventory_id and "
                                                    "inventory.film_id = ?)");
    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
    ret = SQLExecute(stmt);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: Alquiler no eliminado.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }

    SQLCloseCursor(stmt);

    sprintf(query, "DELETE FROM inventory "
                   "WHERE inventory.inventory_id IN (SELECT inventory.inventory_id "
                                              "FROM inventory "
                                              "WHERE inventory.film_id = ?)");

    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
    ret = SQLExecute(stmt);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: Película no eliminada de la tabla inventory.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }

    SQLCloseCursor(stmt);


    sprintf(query, "DELETE FROM film_actor "
                   "WHERE film_actor.film_id = ? ");

    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
    ret = SQLExecute(stmt);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: Película no eliminada de la tabla film_actor.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }

    SQLCloseCursor(stmt);


    sprintf(query, "DELETE FROM film_category "
                   "WHERE film_category.film_id = ? ");

    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
    ret = SQLExecute(stmt);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: Película no eliminada de la tabla film_category.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }

    SQLCloseCursor(stmt);


    sprintf(query, "DELETE FROM film "
                   "WHERE film.film_id = ? ");

    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id, 0, NULL);
    ret = SQLExecute(stmt);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: Película no eliminada de la tabla film.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }

    SQLCloseCursor(stmt);

    /* Free up statement handle */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* DISCONNECT */
    ret = odbc_disconnect(env, dbc);
    if(!SQL_SUCCEEDED(ret)) {
      return EXIT_FAILURE;
    }

    fprintf(stdout, "Película eliminada exitosamente\n");
    return EXIT_SUCCESS;
 }

 int err_disconnect(SQLHENV* env, SQLHDBC* dbc, SQLHSTMT* stmt) {
  SQLRETURN ret;

  if(stmt != NULL) {
    SQLCloseCursor(*stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, *stmt);
  }
  if(env != NULL) {
    ret = odbc_disconnect(*env, *dbc);
    if(!SQL_SUCCEEDED(ret)) {
      fprintf(stderr, "ERROR: La desconexión ha fallado.\n");
      return EXIT_FAILURE;
    }
  }

  return EXIT_FAILURE;
}

 int param_error() {
   fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
   fprintf(stderr, "./dvdfilm remove <Film_id>\n");
   return EXIT_FAILURE;
 }
