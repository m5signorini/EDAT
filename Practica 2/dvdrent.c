#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"

int err_disconnect(SQLHENV* env, SQLHDBC* dbc, SQLHSTMT* stmt);
int param_error();
int add_rent(char* c_id, char* f_id, char* s_id, char* st_id, int am);
int remove_rent(char* r_id);

int main(int argc, char** argv) {

  /* CHECK COMMAND LINE */
  if(argc < 2) {
    param_error();
    return EXIT_FAILURE;
  }

  /* CASO NEW */
  else if(strcmp(argv[1], "new") == 0) {
    if(argc < 7) return param_error();
    return add_rent(argv[2], argv[3], argv[4], argv[5], atoi(argv[6]));
  }

  /* CASO REMOVE */
  else if(strcmp(argv[1], "remove") == 0) {
    if(argc < 3) return param_error();
    return remove_rent(argv[2]);
  }

  /**/
  else {
    return param_error();
  }

  return EXIT_FAILURE;
}

int add_rent(char* c_id, char* f_id, char* s_id, char* st_id, int am){
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[1024];

  SQLCHAR customer_id[512];
  SQLCHAR film_id[512];
  SQLCHAR inventory_id[512];
  SQLCHAR staff_id[512];
  SQLCHAR store_id[512];
  SQLCHAR rental_id[512];

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)){
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  /* First we check that the customer is registered */
  sprintf(query, "SELECT customer.customer_id "
  "FROM customer "
  "WHERE customer.customer_id = ?");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, c_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la búsqueda del cliente\n" );
    fprintf(stderr, "Parámetro <%s> no válido\n", c_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, customer_id, sizeof(customer_id), NULL);
  ret = SQLFetch(stmt);

  /* If the customer does not exist we cannot add the rent */
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: cliente no encontrado.\n");
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We check if the film exists and if it is available for rent */
  sprintf(query, "SELECT inventory.film_id, inventory.inventory_id "
  "FROM inventory "
  "WHERE inventory.store_id = ? and "
  "inventory.film_id = ? and "
  "inventory.inventory_id <> ALL  "
  "(SELECT rental.inventory_id "
  "FROM rental "
  "WHERE rental.return_date > CURRENT_TIMESTAMP)");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, f_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, st_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la búsqueda de la película\n" );
    fprintf(stderr, "Parámetros <%s>, <%s> no válidos\n", f_id, st_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
  SQLBindCol(stmt, 2, SQL_C_CHAR, inventory_id, sizeof(inventory_id), NULL);

  ret = SQLFetch(stmt);

  if (!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: Pelicula con id %s no encontrado o no disponible en alquiler en la tienda de id %s.\n", f_id, st_id);
    SQLCloseCursor(stmt);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We check if the staff matchs with the store in which you want to rent */
  sprintf(query, "SELECT staff.staff_id "
  "FROM store, staff "
  "WHERE staff.staff_id = ? and "
  "store.store_id = ? and "
  "staff.store_id = store.store_id");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, s_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, st_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la búsqueda del empleado\n" );
    fprintf(stderr, "Parámetros <%s>, <%s> no válidos\n", s_id, st_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, staff_id, sizeof(staff_id), NULL);
  ret = SQLFetch(stmt);

  if (!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: store_id %s no coincide con el empleado de id %s.\n", st_id, s_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We check if the customer can rent in the store he wants to rent */
  sprintf(query, "SELECT store.store_id "
  "FROM store, customer "
  "WHERE store.store_id = ? and "
  "customer.customer_id = ? and "
  "customer.store_id = store.store_id");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, st_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, c_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la comprobación del cliente y la tienda\n" );
    fprintf(stderr, "Parámetros <%s>, <%s> no válidos\n", st_id, c_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, store_id, sizeof(store_id), NULL);
  ret = SQLFetch(stmt);

  if (!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: Tienda de id %s no coincide con el cliente con id %s.\n", st_id, c_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We add the rent */
  sprintf(query, "INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) "
  "VALUES (DEFAULT, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP + INTERVAL '1 month', ?, DEFAULT) "
  "RETURNING rental.rental_id");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, inventory_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, staff_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: El alquiler no pudo ser añadido.\n");
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, rental_id, sizeof(rental_id), NULL);
  ret = SQLFetch(stmt);
  if (!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: Id no insertada correctamente.\n");
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We add the payment */
  sprintf(query, "INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date) "
  "VALUES (DEFAULT, ?, ?, ?, ?, CURRENT_TIMESTAMP)");
  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, staff_id, 0, NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, rental_id, 0, NULL);
  SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &am, 0, NULL);

  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "ERROR: Pago no procesado.\n");
    SQLCloseCursor(stmt);

    sprintf(query, "DELETE FROM rental WHERE rental.rental_id = ?");
    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, rental_id, 0, NULL);
    ret = SQLExecute(stmt);
    if(!SQL_SUCCEEDED(ret)) {
      fprintf(stderr, "ERROR: El alquiler no pudo ser eliminado.\n");
      return err_disconnect(&env, &dbc, &stmt);
    }
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
  return EXIT_SUCCESS;
}


int remove_rent(char* r_id){
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[1024];

  SQLCHAR rental_id[512];

  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)){
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  /* First we check that in fact the rent we want to remove
  has been done at any time*/
  sprintf(query, "SELECT rental.rental_id "
  "FROM rental "
  "WHERE rental.rental_id = ? ");
  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, r_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetro <%s> no válido\n", r_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, rental_id, sizeof(rental_id), NULL);
  ret = SQLFetch(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "ERROR: el alquiler con id %s no existe.\n", r_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLCloseCursor(stmt);

  /* We remove the payment */
  sprintf(query, "DELETE FROM payment "
  "WHERE payment.rental_id = ?");
  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, r_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "No existes pagos asociados al alquiler de id %s.\n", r_id);
  }
  SQLCloseCursor(stmt);

  /* We remove the rent from the database  */
  sprintf(query, "DELETE FROM rental "
  "WHERE rental.rental_id = ?");
  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, r_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)){
    fprintf(stderr, "Error en la eliminación del alquiler\n" );
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
  fprintf(stderr, "./dvdrent new <customer Id> <film id> <staff id> <store id> <amount>\n");
  fprintf(stderr, "./dvdrent remove <rent Id>\n");
  return EXIT_FAILURE;
}
