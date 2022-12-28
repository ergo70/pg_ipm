# pg_ipm Makefile

MODULE_big = pg_ipm
OBJS = pg_ipm.o $(WIN32RES)
PGFILEDESC = "Modify emitted values on the fly"
#DOCS         = $(wildcard doc/*.md)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
