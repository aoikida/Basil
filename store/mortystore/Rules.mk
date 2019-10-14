d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc store.cc shardclient.cc)

PROTOS += $(addprefix $(d), morty-proto.proto)

LIB-morty-store := $(o)server.o $(o)store.o \
	$(o)morty-proto.o

LIB-morty-client := $(OBJS-ir-client) $(LIB-udptransport) $(LIB-store-frontend) \
	$(LIB-store-common) $(o)morty-proto.o $(o)client.o $(o)shardclient.o
