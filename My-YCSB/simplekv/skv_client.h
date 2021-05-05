#ifndef SKV_CLIENT_H
#define SKV_CLIENT_H

#include <cstring>
#include <sys/file.h>
#include "client.h"
#include "skv.h"

struct SKVFactory;

struct SKVClient : public Client {
	//SKVContext *SKV_context;
	//SKVReply *last_reply;
	size_t worker_num;
	size_t total_node;
	size_t *layer_cap;
	key__t max_key;
	int db;
	Node *cache;
	size_t cache_cap;
	pthread_mutex_t *val_lock;
	size_t read_ratio;
	size_t rmw_ratio;
	size_t op_count;
	size_t index;
	int db_handler;
	size_t timer;

	SKVClient(SKVFactory *factory, int id);
	~SKVClient();
	int do_set(char *key_buffer, char *value_buffer) override;
	int do_get(char *key_buffer, char **value) override;
	int reset() override;
	void close() override;
private:
	void set_last_reply(void *reply);
};

struct SKVFactory : public ClientFactory {
	const char *SKV_addr;
	//const int SKV_port;
	std::atomic<int> client_id;

	//SKVFactory(const char *SKV_addr, int SKV_port);
	SKVFactory();
	SKVClient *create_client() override;
	void destroy_client(Client *client) override;
};

#endif //YCSB_REDIS_CLIENT_H
