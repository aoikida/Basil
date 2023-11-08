d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), ycsb_sync_client.cc ycsb_sync_transaction.cc)

OBJ-ycsb-sync-transaction := $(LIB-store-frontend) $(o)ycsb-sync-transaction.o

OBJ-ycsb-sync-client := $(o)ycsb-sync-client.o

LIB-ycsb-sync := $(OBJ-ycsb-sync-client) $(OBJ-ycsb-sync-transaction) 