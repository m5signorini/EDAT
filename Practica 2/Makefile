
GRUPO=1201
PAREJA=09

export PGDATABASE:=dvdrental2
export PGUSER :=alumnodb
export PGPASSWORD :=alumnodb
export PGCLIENTENCODING:=LATIN9
export PGHOST:=localhost

DBNAME =$(PGDATABASE)
PSQL = psql
CREATEDB = createdb
DROPDB = dropdb --if-exists
PG_DUMP = pg_dump
PG_RESTORE = pg_restore

CC = gcc -std=c99
CFLAGS = -Wall -Wextra -pedantic
LDLIBS = -lodbc

EXE = odbc-test-1 dvdreq

all: $(EXE)

db: dropdb createdb restore

########################################

createdb:
	@echo Creando BBDD
	@$(CREATEDB)
dropdb:
	@echo Eliminando BBDD
	@$(DROPDB) $(DBNAME)
	rm -f *.log
dump:
	@echo creando dumpfile
	@$(PG_DUMP) > $(DBNAME).sql

restore:
	@echo restore data base
	@cat $(DBNAME).sql | $(PSQL)

shell:
	@echo create psql shell
	@$(PSQL)

########################################


clean :
	rm -f *.o core $(EXE)

$(EXE) : % : %.o odbc.o

ejerc_1:
	--leak-check=full --show-leak-kinds=all ./dvdreq customer -n James -a Gannon
