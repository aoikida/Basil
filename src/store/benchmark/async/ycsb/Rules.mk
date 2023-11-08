d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), ycsb_client.cc ycsb_transaction.cc)

OBJ-ycsb-transaction := $(LIB-store-frontend) $(o)ycsb_transaction.o

OBJ-ycsb-client := $(o)ycsb_client.o

LIB-ycsb := $(OBJ-ycsb-client) $(OBJ-ycsb-transaction) 