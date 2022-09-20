#ifndef SERVER_HELPERS_H
#define SERVER_HELPERS_H

// Server side implementation of UDP client-server model
#include <arpa/inet.h>
#include <error.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

extern "C" {
	  #include "db_types.h"
	  #include "get.h"
}

#define PORT 11211
#define MAXLINE 1024

enum request_type { REQUEST_TYPE_GET, REQUEST_TYPE_SET };

struct request {
    enum request_type type;
    std::string key;
    std::string value;
    std::string id;
};

bool starts_with(std::string str, std::string substr);

std::vector<std::string> str_split(std::string str);

struct request *parse_request(std::string msg);

__extern_always_inline void error_check(int ret, const char *__format, ...) {
    if (ret == 0) {
        return;
    }
    error(EXIT_FAILURE, ret, __format, __va_arg_pack());
};

std::string compute_response(struct request *req);

struct BpfKvUDPConfig {
	struct {
		std::string database_file;
	} bpfkv_udp;

	static BpfKvUDPConfig parse_yaml(YAML::Node &root);
};



#endif /* !SERVER_HELPERS_H */
