// main.cpp — QuorumKV node entrypoint.
// Usage: quorumkv_node <config-file>

#include <iostream>
#include "quorumkv/config.hpp"
#include "quorumkv/node.hpp"

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <config-file>\n";
        return 1;
    }
    try {
        qkv::Config cfg = qkv::parse_config_file(argv[1]);
        qkv::Node node(cfg);
        node.run();
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
