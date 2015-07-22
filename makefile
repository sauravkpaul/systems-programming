CC=gcc
CFLAGS=-I.

LIBS = -lpthread
ODIR = ./obj

DEPS = packet.h

_OBJ_S = server.o 
OBJ_S = $(patsubst %,$(ODIR)/%,$(_OBJ_S))

_OBJ_C = client.o 
OBJ_C = $(patsubst %,$(ODIR)/%,$(_OBJ_C))

$(ODIR)/%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

server: $(OBJ_S)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

client: $(OBJ_C)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)
	

.PHONY: clean

clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~ 
