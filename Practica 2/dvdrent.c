#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"


int param_error();
int add_rent(char* c_id, char* f_id, char* s_id, char* st_id, int am);
int remove_rent(char* r_id);

int main(int argc, char** argv) {

  /* CHECK COMMAND LINE */
  if(argc < 2) {
    fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
    fprintf(stderr, "./dvdrent new <Customer Id> <Film Id> <Staff Id> <Store Id> <Amount>\n");
    fprintf(stderr, "./dvdrent remove <Rent Id>\n");

    return EXIT_FAILURE;
  }

  /* CASO NEW */
  if(strcmp(argv[1], "new") == 0) {
    if(argc < 7) return param_error();
   /* QUERY USING PARAMETERS */
   return add_rent(argv[2], argv[3], argv[4], argv[5],argv[6]);
  }

  /* CASO REMOVE */
  else if(strcmp(argv[1], "remove") == 0) {
    if(argc < 3) return param_error();
    return remove_rent(argv[2]);
  }

  else {
    return param_error();
  }

  return EXIT_FAILURE;
}

/* c_id = customer_id, f_id = film_id, s_id = staff_id,
   st_id = store_id, am = amount */
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
                   "WHERE customer.customer_id = %s;", c_id);

    SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    SQLBindCol(stmt, 1, SQL_C_CHAR, customer_id, sizeof(customer_id), NULL);
    ret = SQLFetch(stmt);

    /* If the customer does not exist we cannot add the rent */
    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: customer not found.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We check if the film exists and if it is available for rent */
    sprintf(query, "SELECT film.film_id, inventory.inventory_id "
                    "FROM film, rental, inventory "
                    "WHERE film.film_id = %s and "
                          "film.film_id = inventory.film_id and "
                          "rental.inventory_id = inventory.inventory_id);", f_id);

    SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindCol(stmt, 1, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
    SQLBindCol(stmt, 1, SQL_C_CHAR, inventory_id, sizeof(inventory_id), NULL);

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: film_id %s not found or not available for rent.\n", f_id);
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We check if the staff matchs with the store in which you want to rent */
    sprintf(query, "SELECT staff.staff_id "
                    "FROM store, staff "
                    "WHERE staff.staff_id = %s and "
                          "store.store_id = %s and "
                          "staff.store_id = store.store_id;" s_id, st_id);

    SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindCol(stmt, 1, SQL_C_CHAR, staff_id, sizeof(staff_id), NULL);
    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: store_id %s not match with the staff with id %s.\n", st_id, s_id);
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We check if the customer can rent in the store he wants to rent */
    sprintf(query, "SELECT store.store_id "
                    "FROM store, customer "
                    "WHERE store.store_id = %s and "
                           "customer.customer_id = %s and "
                           "customer.store_id = store.store_id;" st_id, c_id);

    SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindCol(stmt, 1, SQL_C_CHAR, store_id, sizeof(store_id), NULL);
    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: store_id %s not match with the customer with id %s.\n", st_id, c_id);
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We add the rent */
    sprintf(query, "INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) "
                   "VALUES (DEFAULT, GETDATE(), %s, %s, TO_DATE('20/12/2019', 'DD/MM/YYYY'), %s, DEFAULT);", inventory_id, customer_id, ,staff_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: rent could not be done.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We obtain the rental_id of the rent we have just done */
    sprintf(query, "SELECT rental.rental_id "
                    "FROM rental "
                    "ORDER BY rental.rental_id DESC;");

    SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);
    SQLBindCol(stmt, 1, SQL_C_CHAR, rental_id, sizeof(rental_id), NULL);
    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret)){
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We add the payment */
    sprintf(query, "INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date) "
                   "VALUES (DEFAULT, %s, %s, %s, %d, GETDATE());", customer_id, staff_id, rental_id, amount);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: payment could not be processed.\n");
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
    fprintf(stdout, "Rent added correctly\n");
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
                  "FROM rental ,customer, inventory "
                  "WHERE rental.rental_id = %s and "
                        "rental.inventory_id = inventory.inventory_id and "
                        "inventory.film_id = film.film_id;", r_id);

   SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

   SQLBindCol(stmt, 1, SQL_C_CHAR, rental_id, sizeof(rental_id), NULL);
   ret = SQLFetch(stmt);

   if(!SQL_SUCCEEDED(ret)){
     fprintf(stdout, "ERROR: that rent has never been done.\n");
     SQLCloseCursor(stmt);
     SQLFreeHandle(SQL_HANDLE_STMT, stmt);
     ret = odbc_disconnect(env, dbc);
     if(!SQL_SUCCEEDED(ret)) {
       return EXIT_FAILURE;
     }
     return EXIT_FAILURE;
   }

    SQLCloseCursor(stmt);

    /* We remove the rent from the database  */
    sprintf(query, "DELETE FROM rental "
                   "WHERE rental.rental_id = %s;", r_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if (!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: rental could not be removed.\n");
      SQLCloseCursor(stmt);
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      ret = odbc_disconnect(env, dbc);
      if(!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE;
      }
      return EXIT_FAILURE;
    }

    SQLCloseCursor(stmt);

    /* We remove the payment */
    sprintf(query, "DELETE FROM payment "
                   "WHERE payment.rental_id = %s;", r_id);

    ret = SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

    if(!SQL_SUCCEEDED(ret)){
      fprintf(stdout, "ERROR: payment could not be removed.\n");
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
    fprintf(stdout, "Rent removed correctly\n");
    return EXIT_SUCCESS;
 }

 int param_error() {
   fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
   fprintf(stderr, "./dvdrent new <Customer Id> <Film Id> <Staff Id> <Store Id> <Amount>\n");
   fprintf(stderr, "./dvdrent remove <Rent Id>\n");
   return EXIT_FAILURE;
 }
