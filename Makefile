# contrib/pgcat/Makefile

MODULES = pgcat

EXTENSION = pgcat
DATA = pgcat--1.0.sql
PGFILEDESC = "pgcat - replication"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
