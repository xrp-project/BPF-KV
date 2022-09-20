// Server side implementation of UDP client-server model
#include <arpa/inet.h>
#include <error.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "server_helpers.h"

#define PORT 11211
#define MAXLINE 1024

// Copying these settings from the My-YCSB client
const char *table_name = "lsm:karaage";

bool starts_with(std::string str, std::string substr) {
    return str.find(substr) == 0;
}

std::vector<std::string> str_split(std::string str) {
    std::istringstream iss(str);
    std::vector<std::string> results(std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>());
    return results;
}

struct request *parse_request(std::string msg) {
    struct request *req = new struct request();
    std::vector<std::string> parts = str_split(msg);
    if (parts[0] == "GET") {
        // Request format:
        // GET <key> <req_id>
        req->type = REQUEST_TYPE_GET;
        req->key = parts[1];
        req->id = parts[2];
    } else {
        error(EXIT_FAILURE, EXIT_FAILURE, "Unknown request: %s", msg.c_str());
    }
    return req;
}

std::string compute_response(struct request *req) {
    std::string resp;

    if (req->type == REQUEST_TYPE_GET) {
        char *value = server_grab_value(stoi(req->key), 1, ROOT_NODE_OFFSET);
        
        if (value != nullptr)
            resp = std::string("VALUE ") + std::string(value) + std::string(" ") + req->id;
        else
            resp = std::string("VALUE  ") + req->id;
    }

    return resp;
}

BpfKvUDPConfig BpfKvUDPConfig::parse_yaml(YAML::Node &root) {
	BpfKvUDPConfig config;

	YAML::Node bpfkv_udp = root["bpfkv_udp"];
	config.bpfkv_udp.database_file = bpfkv_udp["database_file"].as<std::string>();

	return config;
}
