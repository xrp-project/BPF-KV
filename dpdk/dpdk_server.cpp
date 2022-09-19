#include <arpa/inet.h>
#include <inttypes.h>
#include <stdint.h>  // for portability
#include <bitset>
#include <iostream>

#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_ether.h>  // Ethernet headers
#include <rte_ip.h>     // IP headers
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_net.h>
#include <rte_udp.h>  // UDP headers

#include "server_helpers.h"  // wiredtiger backend

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define BURST_SIZE 32

#define rte_htons rte_cpu_to_be_16
#define rte_ntohs rte_be_to_cpu_16

/* Debug tools */

void print_eth(struct rte_ether_hdr *eth_hdr) {
    printf("Source MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8
           " %02" PRIx8 " %02" PRIx8 "\n",
           RTE_ETHER_ADDR_BYTES(&eth_hdr->src_addr));
    printf("Destination MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8
           " %02" PRIx8 " %02" PRIx8 "\n",
           RTE_ETHER_ADDR_BYTES(&eth_hdr->dst_addr));
    printf("Ether type: %u \n", rte_ntohs(eth_hdr->ether_type));
}

void print_ip(struct rte_ipv4_hdr *ip_hdr) {
    struct in_addr src_addr;
    src_addr.s_addr = ip_hdr->src_addr;
    printf("Source IP: %s\n", inet_ntoa(src_addr));
    struct in_addr dst_addr;
    dst_addr.s_addr = ip_hdr->dst_addr;
    printf("Destination IP: %s\n", inet_ntoa(dst_addr));
    // printf("IP Checksum: %u\n", ip_hdr->hdr_checksum);
}

void print_udp(struct rte_udp_hdr *udp_hdr) {
    printf("UDP Source Port: %u\n", rte_ntohs(udp_hdr->src_port));
    printf("UDP Dest Port: %u\n", rte_ntohs(udp_hdr->dst_port));
}

void print_eth_ipv4_udp_pkt(struct rte_ether_hdr *eth_hdr, rte_ipv4_hdr *ip_hdr,
                            struct rte_udp_hdr *udp_hdr) {
    print_eth(eth_hdr);
    print_ip(ip_hdr);
    print_udp(udp_hdr);
}

/* Server */
static void serve(void *arg) {
    uint16_t port;
    WiredTigerUDPConfig config = *(WiredTigerUDPConfig *)arg;
    const char *data_dir = config.wiredtiger_udp.data_dir.c_str();
    const char *conn_config = config.wiredtiger_udp.conn_config.c_str();
    const char *session_config = config.wiredtiger_udp.session_config.c_str();
    const char *cursor_config = config.wiredtiger_udp.cursor_config.c_str();
    const char *table_name = config.wiredtiger_udp.table_name.c_str();

    printf("BPF-KV connection config: %s\n", conn_config);
    int ret = wiredtiger_open(data_dir, NULL, conn_config, &conn);
    if (ret < 0) {
        error(EXIT_FAILURE, ret, "Failed to open wiredtiger database");
    }

    printf("\nCore %u serving requests. [Ctrl+C to quit]\n", rte_lcore_id());
    for (;;) {
        RTE_ETH_FOREACH_DEV(port) {
            /* Get burst of RX packets, from first port of pair. */
            struct rte_mbuf *bufs[BURST_SIZE];
            struct rte_mbuf *resp_bufs[BURST_SIZE];
            int resp_idx = 0;
            const uint16_t nb_rx = rte_eth_rx_burst(port, 0, bufs, BURST_SIZE);

            if (unlikely(nb_rx == 0)) continue;

            /* Process Packets */
            for (int i = 0; i < nb_rx; i++) {
                struct rte_mbuf *pkt = bufs[i];
                // Filter for UDP IPv4 packets
                rte_net_hdr_lens hdr_lens;
                uint32_t packet_type =
                    rte_net_get_ptype(pkt, &hdr_lens, RTE_PTYPE_ALL_MASK);

                struct rte_ether_hdr *eth_hdr =
                    rte_pktmbuf_mtod(pkt, struct rte_ether_hdr *);
                // print_eth(eth_hdr);

                if (!(packet_type & (RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 |
                                     RTE_PTYPE_L4_UDP))) {
                    printf("Irrelevant packet, skipping...\n");
                    rte_pktmbuf_free(pkt);
                    continue;
                }

                struct rte_ipv4_hdr *ip_hdr = rte_pktmbuf_mtod_offset(
                    pkt, struct rte_ipv4_hdr *, hdr_lens.l2_len);
                struct rte_udp_hdr *udp_hdr =
                    rte_pktmbuf_mtod_offset(pkt, struct rte_udp_hdr *,
                                            hdr_lens.l2_len + hdr_lens.l3_len);

                // Filter for port 11211
                if (udp_hdr->dst_port != rte_htons(PORT)) {
                    printf("Wrong port, skipping...\n");
                    rte_pktmbuf_free(pkt);
                    continue;
                }

                // printf("Processing packet...\n");
                // Extract data from UDP
                int hdr_len =
                    hdr_lens.l2_len + hdr_lens.l3_len + hdr_lens.l4_len;
                int data_len = pkt->pkt_len - hdr_len;
                char buf[512];
                const void *data =
                    rte_pktmbuf_read(pkt, hdr_len, data_len, buf);
                if (unlikely(data != (void *)buf)) {
                    memcpy(buf, data, data_len);
                }
                buf[data_len] = '\0';

                // Parse message, compute and send response
                struct request *req = parse_request(std::string((char *)buf));
                std::string resp = compute_response(req);
                // std::cout << "Response: " << resp << std::endl;
                rte_pktmbuf_trim(pkt, data_len);
                char *new_data_start = rte_pktmbuf_append(pkt, resp.length());
                memcpy(new_data_start, resp.c_str(), resp.length());

                // New IP/UDP packet len
                ip_hdr->total_length = rte_htons(
                    rte_ntohs(ip_hdr->total_length) + resp.length() - data_len);
                udp_hdr->dgram_len = rte_htons(rte_ntohs(udp_hdr->dgram_len) +
                                               resp.length() - data_len);

                // Test: Echo packet back
                // - Swap ethernet address
                // - Swap IP address
                // - Swap UDP ports
                // - Recalculate IP checksum
                // - Clear UDP checksum
                struct rte_ether_addr eth_addr;
                rte_ether_addr_copy(&eth_hdr->src_addr, &eth_addr);
                rte_ether_addr_copy(&eth_hdr->dst_addr, &eth_hdr->src_addr);
                rte_ether_addr_copy(&eth_addr, &eth_hdr->dst_addr);

                uint32_t ip_addr;
                ip_addr = ip_hdr->src_addr;
                ip_hdr->src_addr = ip_hdr->dst_addr;
                ip_hdr->dst_addr = ip_addr;

                uint16_t udp_port;
                udp_port = udp_hdr->src_port;
                udp_hdr->src_port = udp_hdr->dst_port;
                udp_hdr->dst_port = udp_port;

                // XXX: Must zero checksum before calling rte_ipv4_cksum!
                ip_hdr->hdr_checksum = 0;
                ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);
                udp_hdr->dgram_cksum = 0;

                // print_eth_ipv4_udp_pkt(eth_hdr, ip_hdr, udp_hdr);
                // printf("\n\n");
                resp_bufs[resp_idx] = pkt;
                resp_idx++;
            }

            // Send responses
            uint16_t nb_tx = rte_eth_tx_burst(port, 0, resp_bufs, resp_idx);
            if (nb_tx != resp_idx) {
                printf("Couldn't send packet!\n");
            }
            resp_idx = 0;
        }
    }
}

/*
 * Initializes a given port using global settings and with the RX buffers
 * coming from the mbuf_pool passed as a parameter.
 */
static inline int port_init(uint16_t port, struct rte_mempool *mbuf_pool) {
    // TODO: Setup an RX/TX ring per-core.

    struct rte_eth_conf port_conf;
    const uint16_t rx_rings = 1, tx_rings = 1;
    uint16_t nb_rxd = RX_RING_SIZE;
    uint16_t nb_txd = TX_RING_SIZE;
    int retval;
    uint16_t q;
    struct rte_eth_dev_info dev_info;
    struct rte_eth_txconf txconf;

    if (!rte_eth_dev_is_valid_port(port)) return -1;

    memset(&port_conf, 0, sizeof(struct rte_eth_conf));

    retval = rte_eth_dev_info_get(port, &dev_info);
    if (retval != 0) {
        printf("Error during getting device (port %u) info: %s\n", port,
               strerror(-retval));
        return retval;
    }

    /* Configure the Ethernet device. */
    retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
    if (retval != 0) return retval;

    retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
    if (retval != 0) return retval;

    /* Allocate and set up 1 RX queue per Ethernet port. */
    for (q = 0; q < rx_rings; q++) {
        retval = rte_eth_rx_queue_setup(
            port, q, nb_rxd, rte_eth_dev_socket_id(port), NULL, mbuf_pool);
        if (retval < 0) return retval;
    }

    txconf = dev_info.default_txconf;
    txconf.offloads = port_conf.txmode.offloads;
    /* Allocate and set up 1 TX queue per Ethernet port. */
    for (q = 0; q < tx_rings; q++) {
        retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                        rte_eth_dev_socket_id(port), &txconf);
        if (retval < 0) return retval;
    }

    /* Starting Ethernet port. */
    retval = rte_eth_dev_start(port);
    /* End of starting of ethernet port. */
    if (retval < 0) return retval;

    /* Display the port MAC address. */
    struct rte_ether_addr addr;
    retval = rte_eth_macaddr_get(port, &addr);
    if (retval != 0) return retval;

    printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8
           " %02" PRIx8 " %02" PRIx8 "\n",
           port, RTE_ETHER_ADDR_BYTES(&addr));

    if (retval != 0) return retval;

    return 0;
}

/* Initialization of Environment Abstraction Layer (EAL). */
int main(int argc, char **argv) {
    int ret;

    ret = rte_eal_init(argc, argv);
    if (ret < 0) rte_panic("Cannot init EAL\n");
	argc -= ret;
	argv += ret;
    /* End of initialization of Environment Abstraction Layer */

    /* Allocates mempool to hold the mbufs. */
    struct rte_mempool *mbuf_pool;
    unsigned nb_ports = rte_eth_dev_count_avail();
    mbuf_pool = rte_pktmbuf_pool_create(
        "MBUF_POOL", NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
        RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    if (mbuf_pool == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

    /* Initializing all ports. */
    uint16_t portid;
    RTE_ETH_FOREACH_DEV(portid)
    if (port_init(portid, mbuf_pool) != 0)
        rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", portid);

    // Parse config
    YAML::Node file = YAML::LoadFile(argv[1]);
    WiredTigerUDPConfig config = WiredTigerUDPConfig::parse_yaml(file);
    serve(&config);
    // rte_eal_mp_wait_lcore();

    /* clean up the EAL */
    rte_eal_cleanup();

    return 0;
}
