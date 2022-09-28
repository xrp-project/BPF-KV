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

void listen_and_reply(BpfKvUDPConfig config) {
    int sockfd;
    // Creating socket file descriptor
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Filling server information
    struct sockaddr_in servaddr = {};
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(PORT);

    // Bind the socket with the server address
    if (bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr)) <
        0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    char* db_filename = const_cast<char*>(config.bpfkv_udp.database_file.c_str());

    load_xrp_get();
    load_bpfkv_database(db_filename);

    char incoming_msg_buf[MAXLINE];
    struct sockaddr_in cliaddr;
    socklen_t len;
    len = sizeof(cliaddr);

    printf("Starting server...\n");
    for (;;) {
        int nbytes =
            recvfrom(sockfd, incoming_msg_buf, sizeof(incoming_msg_buf), 0,
                     (struct sockaddr *)&cliaddr, &len);
        if (nbytes < 0) {
            error(EXIT_FAILURE, errno, "Server: Error during recvfrom");
        };

        // Check request type
        incoming_msg_buf[nbytes] = '\0';
        struct request *req = parse_request(std::string(incoming_msg_buf));
        error_check(cursor->reset(cursor), "Resetting cursor failed");

        std::string resp = compute_response(req, cursor);

        // Send response
        sendto(sockfd, resp.c_str(), resp.length(), MSG_CONFIRM,
               (const struct sockaddr *)&cliaddr, len);
        delete req;
    }
}

// Driver code
int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("USAGE: %s [CONFIG_FILE]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    // Parse config
    YAML::Node file = YAML::LoadFile(argv[1]);
    BpfKvUDPConfig config = BpfKvUDPConfig::parse_yaml(file);

    listen_and_reply(config);
    return 0;
}