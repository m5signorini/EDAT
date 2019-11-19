#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"

/*
* example 4 with a queries build on-the-fly, the good way
*/
int err_disconnect(SQLHENV* env, SQLHDBC* dbc, SQLHSTMT* stmt);
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
    param_error();
    return EXIT_FAILURE;
  }

  /* CASO CUSTOMER */
  if(strcmp(argv[1], "customer") == 0) {
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

  /* CASO RENT */
  else if(strcmp(argv[1], "rent") == 0) {
    if(argc < 5) return param_error();
    return rent_query(argv[2], argv[3], argv[4]);
  }

  /* CASO RECOMMEND */
  else if(strcmp(argv[1], "recommend") == 0) {
    if(argc < 3) return param_error();
    return recommend_query(argv[2]);
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
                        "(customer.first_name = ? or "
                        "customer.last_name = ?)");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, f_n, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, l_n, 0, NULL);

  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetros <%s>, <%s> no válidos\n", f_n, l_n);
    return err_disconnect(&env, &dbc, &stmt);
  }

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
  printf("Id Cliente |  Nombre  | Apellido | Fecha de Registro | País | Ciudad | Distrito | Código Postal | Dirección 1 | Dirección 2\n\n");

  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
    printf(" %9s |", customer_id);
    printf(" %8s |", first_name);
    printf(" %8s |", last_name);
    printf(" %10s |", create_date);
    printf(" %11s |", country);
    printf(" %10s |", city);
    printf(" %11s |", district);
    printf(" %6s |", postal_code);
    printf(" %20s |", address);
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
  char f_title_2[512];

  SQLCHAR film_id_1[512];
  SQLCHAR title[512];
  SQLCHAR year[512];
  SQLCHAR length[512];
  SQLCHAR language[512];
  SQLCHAR description[512];
  SQLCHAR first_name[512];
  SQLCHAR last_name[512];

  if(f_title == NULL) return EXIT_FAILURE;
  memset(f_title_2, 0, 512);
  sprintf(f_title_2, "%%%s%%", f_title);


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
                        "film.title LIKE ?");

  SQLPrepare(stmt_1, (SQLCHAR*) query_1, SQL_NTS);
  SQLBindParameter(stmt_1, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, f_title_2, 0, NULL);

  ret = SQLExecute(stmt_1);
  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetro <%s> no válido\n", f_title_2);
    return err_disconnect(&env, &dbc, &stmt_1);
  }


  SQLBindCol(stmt_1, 1, SQL_C_CHAR, film_id_1, sizeof(film_id_1), NULL);
  SQLBindCol(stmt_1, 2, SQL_C_CHAR, title, sizeof(title), NULL);
  SQLBindCol(stmt_1, 3, SQL_C_CHAR, year, sizeof(year), NULL);
  SQLBindCol(stmt_1, 4, SQL_C_CHAR, length, sizeof(length), NULL);
  SQLBindCol(stmt_1, 5, SQL_C_CHAR, description, sizeof(description), NULL);
  SQLBindCol(stmt_1, 6, SQL_C_CHAR, language, sizeof(language), NULL);

  /* Loop through the rows in the result-set */
  printf(" Id de la Película | Título | Año de estreno | Duración | Idioma | Descripción | Actores \n\n");
  while (SQL_SUCCEEDED(ret = SQLFetch(stmt_1))) {
    printf(" %5s |", film_id_1);
    printf(" %20s |", title);
    printf(" %4s |", year);
    printf(" %3s |", length);
    printf(" %10s |", language);
    printf(" %50s |", description);

    /* QUERY 2 - ACTORES */
    sprintf(query_2, "SELECT actor.first_name, actor.last_name "
                    "FROM actor, film_actor "
                    "WHERE film_actor.film_id = ? and "
                          "film_actor.actor_id = actor.actor_id;");

    SQLPrepare(stmt_2, (SQLCHAR*) query_2, SQL_NTS);
    SQLBindParameter(stmt_2, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, film_id_1, 0, NULL);
    ret = SQLExecute(stmt_2);
    if(!SQL_SUCCEEDED(ret)) {
      fprintf(stderr, "Error en la búsqueda\n" );
      err_disconnect(&env, &dbc, &stmt_2);
      err_disconnect(NULL, NULL, &stmt_1);
      return EXIT_FAILURE;
    }
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
	                        "rental.rental_date >= ? and "
                          "rental.rental_date <= ? and "
	                        "payment.rental_id = rental.rental_id and "
	                        "customer.customer_id = ? "
                          "ORDER BY rental.rental_date;");

  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, d_1, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, d_2, 0, NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, c_id, 0, NULL);
  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetros <%s>, <%s>, <%s> no válidos\n", d_1, d_2, c_id);
    return err_disconnect(&env, &dbc, &stmt);
  }


  /*printf("%s\n", query_1);*/

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
    printf(" %5s |", rental_id);
    printf(" %s |", rental_date);
    printf(" %4s |", film_id);
    printf(" %23s |", title);
    printf(" %3s |", staff_id);
    printf(" %10s |", first_name);
    printf(" %10s |", last_name);
    printf(" %3s |", store_id);
    printf(" %5s ", payment);
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

int recommend_query(char* customer_id) {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  char query[3048];

  SQLCHAR film_id[512];
  SQLCHAR title[512];
  SQLCHAR category[512];

  if(customer_id == NULL) return EXIT_FAILURE;

  /* CONNECT */
  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    return EXIT_FAILURE;
  }

  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  fflush(stdout);
  sprintf(query, "SELECT film.film_id, film.title, category.name "
  "FROM film, film_category, category, "
  	"(SELECT COUNT(*) AS D, NotRentedByCust.film_id "
  	"FROM customer, rental, inventory, film_category, "
  		"(SELECT film.film_id "
  		"FROM film, film_category, "
  			"(SELECT CountCategory.category_id "
  			"FROM "
  			  "(SELECT COUNT(*) AS C, category.category_id "
  			  "FROM film, film_category, category, customer, rental, inventory "
  			  "WHERE customer.customer_id = rental.customer_id and "
  			      "inventory.inventory_id = rental.inventory_id and "
  			      "inventory.film_id = film.film_id and "
  			      "film_category.film_id = film.film_id and "
  			      "category.category_id = film_category.category_id and "
  			      "customer.customer_id = ? "
  			  "GROUP BY category.category_id) AS CountCategory "
  			"WHERE CountCategory.C = (SELECT MAX(CC.C) FROM "
  			    "(SELECT COUNT(*) AS C, category.category_id "
  			    "FROM film, film_category, category, customer, rental, inventory "
  			    "WHERE customer.customer_id = rental.customer_id and "
  			        "inventory.inventory_id = rental.inventory_id and "
  			        "inventory.film_id = film.film_id and "
  			        "film_category.film_id = film.film_id and "
  			        "category.category_id = film_category.category_id and "
  			        "customer.customer_id = ? "
  			    "GROUP BY category.category_id) AS CC)) AS MaxCategory "
  		"WHERE film.film_id = film_category.film_id and film_category.category_id = MaxCategory.category_id "
  			"and film.film_id NOT IN (SELECT film.film_id "
  				"FROM rental, inventory, film, customer "
  				"WHERE customer.customer_id =rental.customer_id and "
  					   "inventory.inventory_id = rental.inventory_id and "
  					   "inventory.film_id = film.film_id and "
  					   "customer.customer_id = ?)) AS NotRentedByCust, "
  		 "(SELECT CountCategory.category_id "
  		 "FROM "
  		   "(SELECT COUNT(*) AS C, category.category_id "
  		   "FROM film, film_category, category, customer, rental, inventory "
  		   "WHERE customer.customer_id = rental.customer_id and "
  		       "inventory.inventory_id = rental.inventory_id and "
  		       "inventory.film_id = film.film_id and "
  		       "film_category.film_id = film.film_id and "
  		       "category.category_id = film_category.category_id and "
  		       "customer.customer_id = ? "
  		   "GROUP BY category.category_id) AS CountCategory "
  		 "WHERE CountCategory.C = (SELECT MAX(CC.C) FROM "
  		     "(SELECT COUNT(*) AS C, category.category_id "
  		     "FROM film, film_category, category, customer, rental, inventory "
  		     "WHERE customer.customer_id = rental.customer_id and "
  		         "inventory.inventory_id = rental.inventory_id and "
  		         "inventory.film_id = film.film_id and "
  		         "film_category.film_id = film.film_id and "
  		         "category.category_id = film_category.category_id and "
  		         "customer.customer_id = ? "
  		     "GROUP BY category.category_id) AS CC)) AS MaxCategory "
  	"WHERE customer.customer_id = rental.customer_id and "
  	   "inventory.inventory_id = rental.inventory_id and "
  	   "inventory.film_id = NotRentedByCust.film_id and "
  	   "MaxCategory.category_id = film_category.category_id and "
  	   "NotRentedByCust.film_id = film_category.film_id "
  	"GROUP BY NotRentedByCust.film_id "
  ") AS MostRentedFilms "
  "WHERE film.film_id = MostRentedFilms.film_id and "
  			"film.film_id = film_category.film_id and "
  			"film_category.category_id = category.category_id "
  "ORDER BY MostRentedFilms.D DESC LIMIT 3");
  SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);
  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);
  SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, customer_id, 0, NULL);

  ret = SQLExecute(stmt);
  if(!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error en la ejecución de la consulta\n" );
    fprintf(stderr, "Parámetro <%s> no válido\n", customer_id);
    return err_disconnect(&env, &dbc, &stmt);
  }

  SQLBindCol(stmt, 1, SQL_C_CHAR, film_id, sizeof(film_id), NULL);
  SQLBindCol(stmt, 2, SQL_C_CHAR, title, sizeof(title), NULL);
  SQLBindCol(stmt, 3, SQL_C_CHAR, category, sizeof(category), NULL);

  /* Loop through the rows in the result-set */
  printf(" Id de la película | Título | Categoría\n\n");

  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
    printf(" %5s |", film_id);
    printf(" %20s |", title);
    printf(" %s ", category);
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

int err_disconnect(SQLHENV* env, SQLHDBC* dbc, SQLHSTMT* stmt) {
  SQLRETURN ret;

  if(stmt != NULL) {
    SQLCloseCursor(*stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, *stmt);
  }
  if(env != NULL && dbc != NULL) {
    ret = odbc_disconnect(*env, *dbc);
    if(!SQL_SUCCEEDED(ret)) {
      fprintf(stdout, "ERROR: La desconexión ha fallado.\n");
      return EXIT_FAILURE;
    }
  }

  return EXIT_FAILURE;
}

int param_error() {
  fprintf(stderr, "Error en los parametros de entrada, escribe una de las siguientes opciones:\n\n");
  fprintf(stderr, "./dvdreq customer -n <First Name> -a <Last Name>\n");
  fprintf(stderr, "./dvdreq film <title>\n");
  fprintf(stderr, "./dvdreq rent <customer_id> <init date> <end date>\n");
  fprintf(stderr, "./dvdreq recommend <customer Id>\n");
  return EXIT_FAILURE;
}
