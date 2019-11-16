#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"

/*
* example 4 with a queries build on-the-fly, the good way
*/

int param_error();
int customer_query(char* f_n, char* l_n);
int film_query(char* f_title);
int rent_query(char* c_id, char* d_1, char* d_2);
int recommend_query();

int main(int argc, char** argv) {
  char param_1[512];
  char param_2[512];



  /* CHECK COMMAND LINE */
  if(argc < 2) {
    fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
    fprintf(stderr, "./dvdrent new <customer Id> <film id> <staff id> <store id> <amount>\n");
    fprintf(stderr, "./dvdrent remove <rent Id>\n");
    fprintf(stderr, "./dvdrent rent <customer_id> <init date> <end date>\n");
    return EXIT_FAILURE;
  }

  /* CASO CUSTOMER */
  if(strcmp(argv[1], "new") == 0) {
    if(argc < 6) return param_error();
    /* FETCH PARAMETERS */
    if (strcmp(argv[2], "-n") == 0) {
      strcpy(param_1, argv[3]);
    }
    else return param_error();

    if (strcmp(argv[4], "-a") == 0) {
      strcpy(param_2, argv[5]);
    }
    else return param_error();

    /* QUERY USING PARAMETERS */
    return customer_query(param_1, param_2);
  }

  /* CASO FILM */
  else if(strcmp(argv[1], "film") == 0) {
    if(argc < 3) return param_error();
    return film_query(argv[2]);
  }

  /* CASO RENT*/
  else if(strcmp(argv[1], "rent") == 0) {
    if(argc < 5) return param_error();
    return rent_query(argv[2], argv[3], argv[4]);
  }

  /**/
  else if(strcmp(argv[1], "recommend") == 0) {
    if(argc < 3) return param_error();
    return recommend_query();
  }
  /**/
  else {
    return param_error();
  }

  return EXIT_FAILURE;
}

/***********************/
/* FUNCTIONS ***********/
/***********************/

int customer_query(char* f_n, char* l_n) {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[512];

  SQLCHAR customer_id[512];
  SQLCHAR first_name[512];
  SQLCHAR last_name[512];
  SQLCHAR create_date[512];
  SQLCHAR address[512];
  SQLCHAR address_2[512];
  SQLCHAR city[512];
  SQLCHAR district[512];
  SQLCHAR postal_code[512];
  SQLCHAR country[512];

  if(f_n == NULL || l_n == NULL) return EXIT_FAILURE;

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  fflush(stdout);
  sprintf(query, "SELECT customer.customer_id, customer.first_name, customer.last_name, "
                        "customer.create_date, address.address, address.address2, country.country, city.city, "
                        "address.district, address.postal_code "
                  "FROM customer, address, city, country "
                  "WHERE customer.address_id = address.address_id and "
                        "address.city_id = city.city_id and "
                        "city.country_id = country.country_id and "
                        "(customer.first_name = '%s' or "
                        "customer.last_name = '%s');", f_n, l_n);

  /*printf("%s\n", query);*/

  SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

  SQLBindCol(stmt, 1, SQL_C_CHAR, customer_id, sizeof(customer_id), NULL);
  SQLBindCol(stmt, 2, SQL_C_CHAR, first_name, sizeof(first_name), NULL);
  SQLBindCol(stmt, 3, SQL_C_CHAR, last_name, sizeof(last_name), NULL);
  SQLBindCol(stmt, 4, SQL_C_CHAR, create_date, sizeof(create_date), NULL);
  SQLBindCol(stmt, 5, SQL_C_CHAR, address, sizeof(address), NULL);
  SQLBindCol(stmt, 6, SQL_C_CHAR, address_2, sizeof(address_2), NULL);
  SQLBindCol(stmt, 7, SQL_C_CHAR, country, sizeof(country), NULL);
  SQLBindCol(stmt, 8, SQL_C_CHAR, city, sizeof(city), NULL);
  SQLBindCol(stmt, 9, SQL_C_CHAR, district, sizeof(district), NULL);
  SQLBindCol(stmt, 10, SQL_C_CHAR, postal_code, sizeof(postal_code), NULL);

  /* Loop through the rows in the result-set */
  printf(" Id del cliente | Nombre | Apellido | Fecha de Registro | País | Ciudad | Distrito | Código Postal | Dirección 1 | Dirección 2\n\n");

  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
    printf(" %s |", customer_id);
    printf(" %s |", first_name);
    printf(" %s |", last_name);
    printf(" %s |", create_date);
    printf(" %s |", country);
    printf(" %s |", city);
    printf(" %s |", district);
    printf(" %s |", postal_code);
    printf(" %s |", address);
    printf(" %s ", address_2);
    printf("\n");
  }

  SQLCloseCursor(stmt);

  /* free up statement handle */
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  /* DISCONNECT */
  ret = odbc_disconnect(env, dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

int film_query(char* f_title) {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt_1;
  SQLHSTMT stmt_2;
  SQLRETURN ret; /* ODBC API return status */
  char query_1[512];
  char query_2[512];

  SQLCHAR film_id_1[512];
  SQLCHAR title[512];
  SQLCHAR year[512];
  SQLCHAR length[512];
  SQLCHAR language[512];
  SQLCHAR description[512];
  SQLCHAR first_name[512];
  SQLCHAR last_name[512];

  if(f_title == NULL) return EXIT_FAILURE;

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_1);
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_2);

  fflush(stdout);
  sprintf(query_1, "SELECT film.film_id, film.title, film.release_year, "
                        "film.length, film.description, language.name "
                  "FROM film, language "
                  "WHERE language.language_id = film.language_id and "
                        "film.title LIKE '%%%s%%';", f_title);

  /*printf("%s\n", query_1);*/

  SQLExecDirect(stmt_1, (SQLCHAR*) query_1, SQL_NTS);

  SQLBindCol(stmt_1, 1, SQL_C_CHAR, film_id_1, sizeof(film_id_1), NULL);
  SQLBindCol(stmt_1, 2, SQL_C_CHAR, title, sizeof(title), NULL);
  SQLBindCol(stmt_1, 3, SQL_C_CHAR, year, sizeof(year), NULL);
  SQLBindCol(stmt_1, 4, SQL_C_CHAR, length, sizeof(length), NULL);
  SQLBindCol(stmt_1, 5, SQL_C_CHAR, description, sizeof(description), NULL);
  SQLBindCol(stmt_1, 6, SQL_C_CHAR, language, sizeof(language), NULL);

  /* Loop through the rows in the result-set */
  printf(" Id de la Película | Título | Año de estreno | Duración | Idioma | Descripción | Distrito | Código Postal \n\n");
  while (SQL_SUCCEEDED(ret = SQLFetch(stmt_1))) {
    printf(" %s |", film_id_1);
    printf(" %s |", title);
    printf(" %s |", year);
    printf(" %s |", length);
    printf(" %s |", language);
    printf(" %s |", description);

    /* QUERY 2 - ACTORES */
    sprintf(query_2, "SELECT actor.first_name, actor.last_name "
                    "FROM actor, film_actor "
                    "WHERE film_actor.film_id = %s and "
                          "film_actor.actor_id = actor.actor_id;", film_id_1);
    SQLExecDirect(stmt_2, (SQLCHAR*) query_2, SQL_NTS);
    SQLBindCol(stmt_2, 1, SQL_C_CHAR, first_name, sizeof(first_name), NULL);
    SQLBindCol(stmt_2, 2, SQL_C_CHAR, last_name, sizeof(last_name), NULL);
    while(SQL_SUCCEEDED(ret = SQLFetch(stmt_2))) {
      printf("  %s %s  ", first_name, last_name);
    }
    SQLCloseCursor(stmt_2);
    printf("\n");
  }

  SQLCloseCursor(stmt_1);

  /* free up statement handle */
  SQLFreeHandle(SQL_HANDLE_STMT, stmt_1);
  SQLFreeHandle(SQL_HANDLE_STMT, stmt_2);

  /* DISCONNECT */
  ret = odbc_disconnect(env, dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

int rent_query(char* c_id, char* d_1, char* d_2) {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[1024];

  SQLCHAR rental_id[512];
  SQLCHAR rental_date[512];
  SQLCHAR film_id[512];
  SQLCHAR title[512];
  SQLCHAR staff_id[512];
  SQLCHAR store_id[512];
  SQLCHAR payment[512];
  SQLCHAR first_name[512];
  SQLCHAR last_name[512];

  if(d_1 == NULL || d_2 == NULL || c_id == NULL) return EXIT_FAILURE;

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  fflush(stdout);
  sprintf(query, "SELECT rental.rental_id, rental.rental_date, film.film_id, film.title, "
                          "staff.staff_id, staff.first_name, staff.last_name, store.store_id, "
                          "payment.amount "
                    "FROM customer, rental, inventory, film, staff, store, payment "
                    "WHERE 	customer.customer_id = rental.customer_id and "
	                        "rental.inventory_id = inventory.inventory_id and "
	                        "inventory.film_id = film.film_id and "
	                        "rental.staff_id = staff.staff_id and "
	                        "staff.store_id = store.store_id and "
	                        "rental.rental_date >= '%s' and "
                          "rental.rental_date <= '%s' and "
	                        "payment.rental_id = rental.rental_id and "
	                        "customer.customer_id = %s "
                          "ORDER BY rental.rental_date;", d_1, d_2, c_id);

  /*printf("%s\n", query_1);*/

  SQLExecDirect(stmt, (SQLCHAR*) query, SQL_NTS);

  SQLBindCol(stmt, 1, SQL_C_CHAR, rental_id, sizeof(rental_id), NULL);
  SQLBindCol(stmt, 2, SQL_C_CHAR, rental_date, sizeof(rental_date), NULL);
  SQLBindCol(stmt, 3, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
  SQLBindCol(stmt, 4, SQL_C_CHAR, title, sizeof(title), NULL);
  SQLBindCol(stmt, 5, SQL_C_CHAR, staff_id, sizeof(staff_id), NULL);
  SQLBindCol(stmt, 6, SQL_C_CHAR, first_name, sizeof(first_name), NULL);
  SQLBindCol(stmt, 7, SQL_C_CHAR, last_name, sizeof(last_name), NULL);
  SQLBindCol(stmt, 8, SQL_C_CHAR, store_id, sizeof(store_id), NULL);
  SQLBindCol(stmt, 9, SQL_C_CHAR, payment, sizeof(payment), NULL);

  /* Loop through the rows in the result-set */
  printf(" Id Alquiler | Fecha Alquiler | Película ID | Título | Empleado ID | Nombre | Apellido | Tienda ID | Precio \n\n");
  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
    printf(" %s |", rental_id);
    printf(" %s |", rental_date);
    printf(" %s |", film_id);
    printf(" %s |", title);
    printf(" %s |", staff_id);
    printf(" %s |", first_name);
    printf(" %s |", last_name);
    printf(" %s |", store_id);
    printf(" %s ", payment);
    printf("\n");
  }

  SQLCloseCursor(stmt);

  /* free up statement handle */
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  /* DISCONNECT */
  ret = odbc_disconnect(env, dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

int recommend_query() {return EXIT_FAILURE;}

int param_error() {
  fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
  fprintf(stderr, "./dvdreq customer -n <First Name> -a <Last Name>\n");
  fprintf(stderr, "./dvdreq film <title>\n");
  fprintf(stderr, "./dvdreq rent <customer_id> <init date> <end date>\n");
  fprintf(stderr, "./dvdreq recommend <customer Id>\n");
  return EXIT_FAILURE;
}
