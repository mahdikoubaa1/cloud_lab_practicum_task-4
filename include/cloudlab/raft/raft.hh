#ifndef CLOUDLAB_RAFT_HH
#define CLOUDLAB_RAFT_HH

#include "cloudlab/kvs.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/network/connection.hh"
#include "cloudlab/network/routing.hh"

#include "cloud.pb.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <thread>
#include <unordered_map>
#include <fmt/core.h>

namespace cloudlab {

struct LogEntry {
  uint64_t term_;
  cloud::CloudMessage cmd_;
};

struct PeerIndices {
  uint64_t next_index_;
  uint64_t match_index_;
};

enum class RaftRole {
  LEADER,
  CANDIDATE,
  FOLLOWER,
};

class Raft {
 public:
  explicit Raft(const std::string& path = {}, const std::string& addr = {})
      : kvs{path}, own_addr{addr} {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(1200, 2000);

    election_timeout_val = std::chrono::milliseconds(dist6(rng));
    std::cout << "election_timeouttt: "
              << election_timeout_val.count() / 1000000 << std::endl;
    std::cout << "own addr is: " << own_addr << std::endl;
    std::cout << "kvs.clear(): " << kvs.clear() << std::endl;
    // std::cout << "kvs.open(): " << kvs.open() << std::endl;
    reset_election_timer();
  }

  auto run(Routing* routing, std::mutex& mtx) -> std::thread;

  //   auto open() -> bool {
  //     return kvs.is_open();
  //   }

  auto get(const std::string& key, std::string& result) -> bool {
    return kvs.get(key, result);
  }

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer)
      -> bool {
    return kvs.get_all(buffer);
  }

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool {
    return kvs.remove(key);
  }

  auto leader() -> bool {
    return role == RaftRole::LEADER;
  }

  auto candidate() -> bool {
    return role == RaftRole::CANDIDATE;
  }

  auto follower() -> bool {
    return role == RaftRole::FOLLOWER;
  }

  auto set_leader() -> void {
    role = RaftRole::LEADER;
    leader_addr = own_addr;
  }

  auto set_candidate() -> void {
    role = RaftRole::CANDIDATE;
  }

  auto set_follower() -> void {
    role = RaftRole::FOLLOWER;
  }

  auto get_role() -> RaftRole {
    return role;
  }

  auto term() -> uint64_t {
    return current_term;
  }

  auto add_peer(const SocketAddress& peer) -> void {
    // peers[peer] = {.next_index_ = log_.size(), .match_index_ = 0};
    // add timestamp to peers
    peers[peer] = std::time(nullptr);
  }

  auto remove_peer(const SocketAddress& peer) -> void {
    if (peers.contains(peer)) {
      peers.erase(peer);
    }
  }

  auto print_peers() -> void {
    for (auto& node : peers) {
      std::cout << node.first.string() << ": " << node.second << std::endl;
    }
  }

  auto log(const cloud::CloudMessage& msg, uint64_t term = 0) -> uint64_t {
    term = (term) ? term : current_term;

    assert(log_.empty() || term_of_log_entry(last_log_index()) <= term);
    log_.push_back({.term_ = term, .cmd_ = msg});

    return last_log_index();
  }

  auto last_log_index() -> uint64_t {
    return log_.size() - 1;
  }

  auto term_of_log_entry(uint64_t id) -> uint64_t {
    return log_.at(id).term_;
  }

  auto replicate(uint64_t log_id = 0) -> bool {
    // TODO(someone)
    if (log_id == 0) {
      // spawn threads and replicate log across peers
    } else {
      // fast replication, e.g., for put operation:
      // replicate up to certain entry on majority of peers
      // return once certain entry has been replicated on majority of peers
    }

    return false;
  }

  auto append_entries(uint64_t term, uint64_t prev_log_index,
                      uint64_t prev_log_term,
                      const std::span<cloud::CloudMessage_LogEntry>& entries,
                      uint64_t commit_index) -> bool;

  auto election_timeout() -> bool {
    auto current_time = std::chrono::high_resolution_clock::now();
    // std::cout << "in election_timeout()" << std::endl;
    return (election_timer + election_timeout_val < current_time);
  }

  auto set_random_timeout() -> void {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(1200, 2000);

    election_timeout_val = std::chrono::milliseconds(dist6(rng));
    std::cout << "new timeout: " << election_timeout_val.count() / 1000000
              << std::endl;
  }

  auto reset_election_timer() -> void {
    election_timer = std::chrono::high_resolution_clock::now();
  }

  auto add_vote() -> void {
    votes_received.fetch_add(1);
  }

  auto reset_vote() -> void {
    votes_received = 0;
  }

  auto set_voted_for(const SocketAddress& addr) -> void {
    voted_for = {addr};
  }

  auto has_voted() -> bool {
    return voted_for.has_value();
  }

  auto reset_voted_for() -> void {
    voted_for.reset();
  }

  auto broadcast(const cloud::CloudMessage& msg) -> void;

  auto broadcast_multi(const SocketAddress& node) -> void;

  auto perform_election(Routing* routing) -> void;

  auto heartbeat(Routing* routing, std::mutex& mtx) -> void;

  auto get_dropped_peers(std::vector<std::string>& result) -> void {
    for (const auto& peer : dropped_peers) {
      result.push_back(peer.string());
    }
  }

  auto set_leader_addr(const std::string& addr) -> void {
    leader_addr = addr;
  }

  auto get_leader_addr(std::string& result) -> void {
    result = leader_addr;
  }

  auto get_timeout(std::string& result) -> void {
    result = std::to_string(election_timeout_val.count() / 1000000);
  }

  auto set_term(uint64_t& input) -> void {
    current_term = input;
  }

  auto get_term(uint64_t& result) -> void {
    result = current_term;
  }

 private:
  auto worker(Routing& routing) -> void;
  // the actual kvs
  KVS kvs;

  // every peer is initially a follower
  RaftRole role{RaftRole::FOLLOWER};

  std::string own_addr{""};
  std::string leader_addr{""};

  // persistent state on all servers
  uint64_t current_term{0};
  std::optional<SocketAddress> voted_for{};
  std::vector<LogEntry> log_;
  std::vector<std::pair<std::string, std::string>> history;

  // for returning dropped followers
  std::vector<SocketAddress> dropped_peers;

  // volatile state on all servers
  uint64_t commit_index{};
  uint64_t last_applied{};

  // volatile state on leaders
  // std::unordered_map<SocketAddress, PeerIndices> peers;
  // in current design peers does not have leader and its own address
  std::unordered_map<SocketAddress, std::time_t> peers;

  std::atomic_uint16_t votes_received{0};
  // election timer
  std::chrono::high_resolution_clock::time_point election_timer;
  std::chrono::high_resolution_clock::duration election_timeout_val{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_RAFT_HH
