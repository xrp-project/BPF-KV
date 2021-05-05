#include "redis_client.h"

RedisClient::RedisClient(RedisFactory *factory, int id)
: Client(id, factory), redis_context(nullptr), last_reply(nullptr) {
	this->redis_context = redisConnect(factory->redis_addr, factory->redis_port);
	if (this->redis_context == nullptr || this->redis_context->err) {
		if (this->redis_context) {
			fprintf(stderr, "RedisClient: redisConnect error %s\n", this->redis_context->errstr);
		} else {
			fprintf(stderr, "RedisClient: cannot allocate redis context\n");
		}
		throw std::invalid_argument("failed to open redis_context");
	}
}

RedisClient::~RedisClient() {
	if (this->last_reply != nullptr)
		freeReplyObject(this->last_reply);
	if (this->redis_context)
		redisFree(this->redis_context);
}

int RedisClient::do_set(char *key_buffer, char *value_buffer) {
	redisReply *reply = (redisReply *)redisCommand(this->redis_context, "SET %s %s", key_buffer, value_buffer);
	if (!reply) {
		fprintf(stderr, "RedisClient: SET error: %s\n", this->redis_context->errstr);
		throw std::invalid_argument("failed to SET");
	}
	this->set_last_reply(reply);
	return 0;
}

int RedisClient::do_get(char *key_buffer, char **value) {
	redisReply *reply = (redisReply *)redisCommand(this->redis_context, "GET %s", key_buffer);
	if (!reply) {
		fprintf(stderr, "RedisClient: GET error: %s\n", this->redis_context->errstr);
		throw std::invalid_argument("failed to GET");
	}
	*value = reply->str;
	this->set_last_reply(reply);
	return 0;
}

int RedisClient::reset() {
	throw std::invalid_argument("reset not implemented");
}

void RedisClient::close() {
	if (this->last_reply)
		freeReplyObject(this->last_reply);
	this->last_reply = nullptr;
	if (this->redis_context)
		redisFree(this->redis_context);
	this->redis_context = nullptr;
}

void RedisClient::set_last_reply(redisReply *reply) {
	if (this->last_reply)
		freeReplyObject(this->last_reply);
	this->last_reply = reply;
}

RedisFactory::RedisFactory(const char *redis_addr, int redis_port)
: redis_addr(redis_addr), redis_port(redis_port), client_id(0) {
	;
}

RedisClient *RedisFactory::create_client() {
	RedisClient *client = new RedisClient(this, this->client_id++);
	return client;
}

void RedisFactory::destroy_client(Client *client) {
	RedisClient *redis_client = (RedisClient *) client;
	redis_client->close();
	delete redis_client;
}
