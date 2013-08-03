LIBLEVELDB   = /usr/local/lib/libleveldb*

.PHONY: all travis

all: travis

travis: $(LIBLEVELDB)

$(LIBLEVELDB):
	(cd /tmp; \
		git clone https://code.google.com/p/leveldb/; \
		cd leveldb; \
		make; \
		sudo mv ./libleveldb* /usr/local/lib; \
		sudo cp -a ./include/leveldb /usr/local/include)
