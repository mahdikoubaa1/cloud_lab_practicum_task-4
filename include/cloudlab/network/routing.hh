#ifndef CLOUDLAB_ROUTING_HH
#define CLOUDLAB_ROUTING_HH

#include "cloudlab/kvs.hh"
#include "cloudlab/network/address.hh"
#include <optional>

#include <algorithm>
#include <ctime>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cloudlab {

// actually 840 is a good number
const auto cluster_partitions = 4;

/**
 * Routing class to map keys to peers.
 */
class Routing {
 public:
  explicit Routing(const std::string& backend_address)
      : backend_address{SocketAddress{backend_address}} {
  }

  auto add_peer(uint32_t partition, const SocketAddress& peer) {
    if (table.contains(partition)) {
      auto& peers = table.at(partition);
      if (std::find(peers.begin(), peers.end(), peer) == peers.end()) {
        peers.push_back(peer);
      }
    } else {
      table.insert({partition, {peer}});
    }

    // add timestamp to map
    peer_node[peer] = std::time(nullptr);

    // for (auto& node: peer_node) {
    //   std::cout << node.first.string() << ": " << node.second << std::endl;
    // }
  }

  auto remove_peer(uint32_t partition, const SocketAddress& peer) {
    if (table.contains(partition)) {
      auto& peers = table.at(partition);
      peers.erase(std::remove(peers.begin(), peers.end(), peer), peers.end());
    }

    if (peer_node.contains(peer)) {
      peer_node.erase(peer);
    }
  }

  auto find_peer(const std::string& key) -> std::optional<SocketAddress> {
    // map key to partition ID
    // no need for a cryptographically secure hash function here
    auto partition_id = Routing::get_partition(key);

    // check if partition ID is in map and if there is at least one peer
    if (table.contains(partition_id) && !table.at(partition_id).empty()) {
      return {table.at(partition_id).front()};
    }

    return {};
  }

  auto get_partition(const std::string& key) const -> uint32_t {
    return std::hash<std::string>{}(key) % partitions;
  }

  auto partitions_by_peer()
      -> std::unordered_map<SocketAddress, std::unordered_set<uint32_t>> {
    std::unordered_map<SocketAddress, std::unordered_set<uint32_t>> tmp;

    for (const auto& [partition, peers] : table) {
      if (!peers.empty()) {
        auto peer = peers.front();
        if (tmp.contains(peer)) {
          tmp.at(peer).insert(partition);
        } else {
          tmp.insert({peer, {partition}});
        }
      }
    }

    return tmp;
  }

  auto get_cluster_address() -> std::optional<SocketAddress> {
    return cluster_address;
  }

  auto set_cluster_address(std::optional<SocketAddress> address) -> void {
    cluster_address = std::move(address);
  }

  auto get_backend_address() -> SocketAddress {
    return backend_address;
  }

  auto set_partitions_to_cluster_size() -> void {
    partitions = cluster_partitions;
  }

 private:
  size_t partitions{1};

  std::unordered_map<uint32_t, std::vector<SocketAddress>> table;

  std::unordered_map<SocketAddress, std::time_t> peer_node;

  // API requests are forwarded to this address
  const SocketAddress backend_address;

  // cluster metadata store, e.g., routing tier
  std::optional<SocketAddress> cluster_address{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_ROUTING_HH
