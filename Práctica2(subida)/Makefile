
GRUPO=1201
PAREJA=09

export PGDATABASE:=dvdrental
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
CFLAGS = -g -Wall -Wextra -pedantic
LDLIBS = -lodbc

EXE = dvdreq dvdrent dvdfilm

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

test1:
	@echo query1: "Testing dvdrent new 39 1 1 1 5"
	@cat query1.sql | $(PSQL)
	./dvdrent new 39 1 1 1 5
	@cat query1.sql | $(PSQL)

test2:
	@echo query2: "Testing dvdrent remove 10126"
	@cat query2.sql | $(PSQL)
	./dvdrent remove 10126
	@cat query2.sql | $(PSQL)

test3:
	@echo query11: "Testing dvdfilm remove 111"
	@cat query11.sql | $(PSQL)
	./dvdfilm remove 111
	@cat query11.sql | $(PSQL)
