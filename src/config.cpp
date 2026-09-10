#include "querykv/config.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace qkv {

namespace {
std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty()) parts.push_back(item);
    }
    return parts;
}

std::vector<PeerInfo> parse_peer_list(const std::string& value) {
    std::vector<PeerInfo> peers;
    for (const auto& token : split(value, ',')) {
        auto fields = split(token, ':');
        if (fields.size() != 3) {
            throw std::runtime_error("config: malformed peer entry '" + token +
                                      "' (expected id:host:port)");
        }
        PeerInfo p;
        p.id = std::stoull(fields[0]);
        p.host = fields[1];
        p.port = std::stoi(fields[2]);
        peers.push_back(p);
    }
    return peers;
}
} // namespace

Config parse_config_file(const std::string& path) {
    std::ifstream in(path);
    if (!in.is_open()) {
        throw std::runtime_error("config: cannot open " + path);
    }

    Config cfg;
    std::string line;
    while (std::getline(in, line)) {
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;
        auto eq = trimmed.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(trimmed.substr(0, eq));
        std::string value = trim(trimmed.substr(eq + 1));

        if (key == "id") cfg.id = std::stoull(value);
        else if (key == "shard") cfg.shard = std::stoi(value);
        else if (key == "num_shards") cfg.num_shards = std::stoi(value);
        else if (key == "port") cfg.port = std::stoi(value);
        else if (key == "data_dir") cfg.data_dir = value;
        else if (key == "peers") cfg.shard_peers = parse_peer_list(value);
        else if (key == "cluster") cfg.cluster_members = parse_peer_list(value);
        // unknown keys are ignored rather than rejected, so config files
        // stay forward-compatible with fields an older binary doesn't know
    }
    return cfg;
}

} // namespace qkv
