#include "skv_client.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#define LAYER_NUM 5

SKVClient::SKVClient(SKVFactory *factory, int id) : Client(id, factory)
{
    int layer_num = LAYER_NUM;
    // only need runtime mode
    db = get_handler(O_RDWR|O_DIRECT);
    
    layer_cap = (size_t *)malloc(layer_num * sizeof(size_t));
    total_node = 1;
    layer_cap[0] = 1;
    for (size_t i = 1; i < layer_num; i++) {
        layer_cap[i] = layer_cap[i - 1] * FANOUT;
        total_node += layer_cap[i];
    }
    max_key = layer_cap[layer_num - 1] * NODE_CAPACITY;
    cache_cap = 0;
    size_t log_num = max_key / LOG_CAPACITY + 1;
    val_lock = (pthread_mutex_t *)malloc(log_num * sizeof(pthread_mutex_t));
    if (val_lock == NULL) {
        printf("val_lock malloc fail!\n");
        terminate();
        exit(1);
    }
    for (size_t i = 0; i < log_num; i++) {
        if (pthread_mutex_init(&val_lock[i], NULL) != 0) {
            printf("pthread_mutex_init fail at %lu\n", i);
            terminate();
            exit(1);
        }
    }
    printf("%lu locks\n", log_num);

    printf("%lu blocks in total, max key is %lu\n", total_node, max_key);
}

SKVClient::~SKVClient() {
	free(layer_cap);
	free(cache);
	//close(db);
}

int SKVClient::do_set(char *key_buffer, char *value_buffer) {
    /*
    //int SKVClient::do_set(size_t layer_num) {
    int layer_num = LAYER_NUM;
    printf("Load the database of %lu layers\n", layer_num);
    initialize(layer_num, LOAD_MODE);

    // 1. Load the index
    Node *node, *tmp;
    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }
    ptr__t next_pos = 1, tmp_ptr = 0;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            node->num = NODE_CAPACITY;
            node->type = (i == layer_num - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / node->num;
            for (size_t k = 0; k < node->num; k++) {
                node->key[k] = start_key + k * sub_extent;
                node->ptr[k] = node->type == INTERNAL ? 
                              encode(next_pos   * BLK_SIZE) :
                              encode(total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE);
                next_pos++;
            }
            write(db, node, sizeof(Node));
            start_key += extent;

            // Sanity check
            // read_node(tmp_ptr, &tmp);
            // compare_nodes(&node, &tmp);
            // tmp_ptr += BLK_SIZE;
        }
    }

    // 2. Load the value log
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf((char *)log->val[j], "%63lu", i + j);
        }
        write(db, log, sizeof(Log));

        // Sanity check
        // read_log((total_node + i / LOG_CAPACITY) * BLK_SIZE, &log);
    }

    free(log);
    free(node);
    */
}

int SKVClient::do_get(char *key_buffer, char **value) {
//int SKVClient::do_get(key__t key, val__t value) {
	get((key__t)key_buffer, (unsigned char *)value, db_handler);
	return 0;
}

int SKVClient::reset() {
	throw std::invalid_argument("reset not implemented");
}

void SKVClient::close() {
}

/*
void SKVClient::set_last_reply(SKVReply *reply) {
	if (this->last_reply)
		freeReplyObject(this->last_reply);
	this->last_reply = reply;
}
*/

SKVFactory::SKVFactory()
: client_id(0) {
}

SKVClient *SKVFactory::create_client() {
	SKVClient *client = new SKVClient(this, this->client_id++);
	return client;
}

void SKVFactory::destroy_client(Client *client) {
	SKVClient *skv_client = (SKVClient *) client;
	skv_client->close();
	delete skv_client;
}
