// cli_main.cpp — command-line client for QueryKV.
//
// Usage:
//   querykv_cli <client.conf> get <key>
//   querykv_cli <client.conf> put <key> <value>
//   querykv_cli <client.conf> del <key>
//   querykv_cli <client.conf> status
//   querykv_cli <client.conf>                 (interactive REPL)
//
// client.conf format:
//   num_shards=2
//   cluster=0:0:node0:7000,1:0:node1:7001,2:0:node2:7002,3:1:node3:7003,...
//            id:shard:host:port

#include <iostream>
#include <fstream>
#include <sstream>
#include "client.hpp"

namespace {

struct ClientConfig {
    int num_shards = 1;
    std::vector<qkv::ClientPeer> cluster;
};

std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

ClientConfig load_config(const std::string& path) {
    std::ifstream in(path);
    if (!in.is_open()) throw std::runtime_error("cannot open " + path);
    ClientConfig cfg;
    std::string line;
    while (std::getline(in, line)) {
        std::string t = trim(line);
        if (t.empty() || t[0] == '#') continue;
        auto eq = t.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(t.substr(0, eq));
        std::string value = trim(t.substr(eq + 1));
        if (key == "num_shards") cfg.num_shards = std::stoi(value);
        else if (key == "cluster") cfg.cluster = qkv::parse_client_cluster(value);
    }
    return cfg;
}

void print_get(const qkv::GetResult& r, const std::string& key) {
    if (!r.reached_cluster) {
        std::cout << "ERROR: could not reach a leader for key '" << key << "' (cluster down?)\n";
    } else if (r.found) {
        std::cout << r.value << "\n";
    } else {
        std::cout << "(not found)\n";
    }
}

void print_write(const qkv::WriteResult& r) {
    std::cout << (r.ok ? "OK\n" : "ERROR: write did not commit (no leader / timed out)\n");
}

int run_one(qkv::Client& client, const std::vector<std::string>& args) {
    if (args.empty()) return 0;
    const std::string& cmd = args[0];
    if (cmd == "get" && args.size() == 2) {
        print_get(client.get(args[1]), args[1]);
    } else if (cmd == "put" && args.size() == 3) {
        print_write(client.put(args[1], args[2]));
    } else if (cmd == "del" && args.size() == 2) {
        print_write(client.del(args[1]));
    } else if (cmd == "status") {
        std::cout << client.cluster_status();
    } else {
        std::cout << "commands: get <key> | put <key> <value> | del <key> | status\n";
        return 1;
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <client.conf> [get|put|del|status ...]\n";
        return 1;
    }
    try {
        ClientConfig cfg = load_config(argv[1]);
        qkv::Client client(cfg.cluster, cfg.num_shards);

        if (argc > 2) {
            std::vector<std::string> args(argv + 2, argv + argc);
            return run_one(client, args);
        }

        std::cout << "QueryKV interactive client. Commands: get/put/del/status/quit\n";
        std::string line;
        while (true) {
            std::cout << "qkv> " << std::flush;
            if (!std::getline(std::cin, line)) break;
            std::istringstream iss(line);
            std::vector<std::string> args;
            std::string tok;
            // put's value may contain spaces — only split into 3 parts for put,
            // otherwise plain whitespace tokenization.
            iss >> tok;
            if (tok.empty()) continue;
            if (tok == "quit" || tok == "exit") break;
            if (tok == "put") {
                std::string key, rest;
                iss >> key;
                std::getline(iss, rest);
                run_one(client, {"put", key, trim(rest)});
            } else {
                args.push_back(tok);
                while (iss >> tok) args.push_back(tok);
                run_one(client, args);
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
