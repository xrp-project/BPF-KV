#ifndef YCSB_CLIENT_H
#define YCSB_CLIENT_H

#include <atomic>
#include <stdexcept>

struct ClientFactory;

struct Client {
	int id;
	ClientFactory *factory;

	Client(int id, ClientFactory *factory);
	virtual int do_set(char *key_buffer, char *value_buffer) = 0;
	virtual int do_get(char *key_buffer, char **value) = 0;
	virtual int reset() = 0;
	virtual void close() = 0;
};

struct ClientFactory {
	virtual Client *create_client() = 0;
	virtual void destroy_client(Client *client) = 0;
};

#endif //YCSB_CLIENT_H
