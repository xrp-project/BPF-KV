#ifndef YCSB_REDIS_CLIENT_H
#define YCSB_REDIS_CLIENT_H

#include <cstring>
#include "client.h"
#include <hiredis/hiredis.h>

struct RedisFactory;

struct RedisClient : public Client {
	redisContext *redis_context;
	redisReply *last_reply;

	RedisClient(RedisFactory *factory, int id);
	~RedisClient();
	int do_set(char *key_buffer, char *value_buffer) override;
	int do_get(char *key_buffer, char **value) override;
	int reset() override;
	void close() override;
private:
	void set_last_reply(redisReply *reply);
};

struct RedisFactory : public ClientFactory {
	const char *redis_addr;
	const int redis_port;
	std::atomic<int> client_id;

	RedisFactory(const char *redis_addr, int redis_port);
	RedisClient *create_client() override;
	void destroy_client(Client *client) override;
};

#endif //YCSB_REDIS_CLIENT_H
