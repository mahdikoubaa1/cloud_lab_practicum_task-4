#include "cloudlab/raft/raft.hh"

namespace cloudlab {

auto Raft::append_entries(
    uint64_t term, uint64_t prev_log_index, uint64_t prev_log_term,
    const std::span<cloud::CloudMessage_LogEntry>& entries,
    uint64_t commit_index) -> bool {
  if (term < current_term) {
    return false;
  }

  if (log_.size() <= prev_log_index ||
      term_of_log_entry(prev_log_index) != prev_log_term) {
    return false;
  }

  auto index = prev_log_index + 1;

  // optimization: only delete in case of conflict
  if (index < log_.size()) {
    log_.erase(log_.begin() + index, log_.end());
  }

  for (auto& entry : entries) {
    log_.push_back({.term_ = entry.term(), .cmd_ = entry.cmd()});
  }

  return true;
}

auto Raft::put(const std::string& key, const std::string& value) -> bool {
  // TODO(someone): implement RAFT stuff
  //                1. write into log
  //                2. replicate log across majority of peers
  //                3. return once replicated across majority of peers
  if (leader()) {
    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    msg.mutable_address()->set_address(own_addr);
    auto* kvp = msg.add_kvp();
    kvp->set_key(key);
    kvp->set_value(value);
    broadcast(msg);
  }

  history.emplace_back(key, value);
  return kvs.put(key, value);
}

auto Raft::perform_election(Routing* routing) -> void {
  std::cout << "in Raft::perform_election" << std::endl;
  std::vector<std::thread> threads(peers.size());
  cloud::CloudMessage msg{};

  // we are a candidate now
  role = RaftRole::CANDIDATE;

  // update current term, vote for self, reset election timer
  current_term++;
  voted_for = {routing->get_backend_address()};
  votes_received = 1;
  reset_election_timer();
  std::cout << "current term: " << current_term << std::endl;

  // prepare the request vote rpc messages
  msg.set_type(cloud::CloudMessage_Type_REQUEST);
  msg.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);

  msg.set_term(current_term);
  msg.mutable_address()->set_address(routing->get_backend_address().string());

  if (!log_.empty()) {
    msg.set_log_index(last_log_index());
    msg.set_log_term(term_of_log_entry(last_log_index()));
  }

  auto send_request_vote =
      [&msg, this](const std::pair<const SocketAddress, std::time_t>& peer,
                   std::atomic_uint16_t& votes) {
        try {
          cloud::CloudMessage response{};

          // timeout might be a good idea here
          Connection con{peer.first};
          con.send(msg);

          con.receive(response);

          if (response.type() != cloud::CloudMessage_Type_RESPONSE) {
            return;
          }

          if (response.operation() != cloud::CloudMessage_Operation_RAFT_VOTE) {
            return;
          }

          // fall back to follower state if we missed a term
          if (current_term <= response.term()) {
            role = RaftRole::FOLLOWER;
          }

          if (response.success()) {
            votes.fetch_add(1);
          }
        } catch (std::exception& e) {
          fmt::print("election exception: \n", e.what());
        }
      };

  for (auto& peer : peers) {
    std::cout << "send vote request " << peer.first.string() << std::endl;
    Connection con{peer.first.string()};
    if (con.connect_failed) {
      std::cout << "removing " << peer.first.string() << std::endl;
      remove_peer(peer.first);
      // leader is not supposed to quit gracefully in current design
      // so only pushing no poping
      dropped_peers.push_back(peer.first);
      // FIXME: when multiple nodes fail, skipping send can be a problem.
      break;
    }
    std::cout << "sending" << std::endl;
    con.send(msg);
  }
}

auto Raft::heartbeat(Routing* routing, std::mutex& mtx) -> void {
  if (leader()) {
    std::vector<std::thread> threads(peers.size());
    cloud::CloudMessage msg{};
    // std::cout << "in heartbeat leader" << std::endl;
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    msg.set_success(true);
    msg.set_term(current_term);
    msg.mutable_address()->set_address(own_addr);

    // do we have to set log term and log index on heartbeats as well?

    // std::cout << "peers size: " << peers.size() << std::endl;

    // prevent multiple con on same port
    std::lock_guard<std::mutex> lck(mtx);

    // FIXME: not sure why threads does not work.
    for (auto& peer : peers) {
      // std::cout << peer.first.string() << std::endl;
      Connection con{peer.first.string()};
      if (con.connect_failed) {
        std::cout << "removing " << peer.first.string() << std::endl;
        remove_peer(peer.first);
        // leader is not supposed to quit gracefully in current design
        // so only pushing no poping
        dropped_peers.push_back(peer.first);
        // FIXME: when multiple nodes fail, skipping send can be a problem.
        break;
      }
      con.send(msg);
    }
    return;
  } else {
    // std::cout << "follower heartbeat" << std::endl;
    // std::cout << "follower peers size: " << peers.size() << std::endl;
    if (candidate() && votes_received > (peers.size() + 1) / 2) {
      std::cout << "vote received: " << votes_received.load() << std::endl;
      std::cout << "I'm leader!" << std::endl;
      role = RaftRole::LEADER;
      leader_addr = own_addr;
    }
    // for follower, if there are peers and leader is gone, perform election
    if (election_timeout()) {
      // std::cout << "election_timeout" << std::endl;
      // still candidate and votes received from majority
      if (candidate() && votes_received > (peers.size() + 1) / 2) {
        std::cout << "vote received: " << votes_received.load() << std::endl;
        std::cout << "I'm leader!" << std::endl;

        role = RaftRole::LEADER;
        leader_addr = own_addr;
      }
      // in candidate but cannot determine itself as leader
      if (peers.size() > 0 && (follower() || candidate())) {
        std::cout << "restart, vote received: " << votes_received.load()
                  << std::endl;
        std::cout << "follower peers size: " << peers.size() << std::endl;
        std::cout << "threshold: " << ((peers.size() + 1) / 2) << std::endl;

        perform_election(routing);
        set_random_timeout();
      }
      // When there's only two nodes and the leader fail, the other one will
      // not become leader in current design since peers.size() is 0
    }
  }
}

auto Raft::run(Routing* routing, std::mutex& mtx) -> std::thread {
  std::cout << "in Raft::run" << std::endl;

  // FIXME: probably bad design
  auto worker_thread = [this](Routing* routing, std::mutex& mtx) {
    using namespace std::chrono_literals;
    while (true) {
      std::this_thread::sleep_for(500ms);
      // FIXME(someone): move connection outside of loop and ensure in loop that
      //                 connection is still ok
      heartbeat(routing, mtx);
    }
  };
  auto thread = std::thread(worker_thread, routing, std::ref(mtx));

  // return thread handle
  return thread;
}

// sending new nodes to existing node
auto Raft::broadcast(const cloud::CloudMessage& msg) -> void {
  for (auto& peer : peers) {
    // std::cout << "broadcast new node to: " << peer.first.string() <<
    // std::endl;
    Connection con{peer.first.string()};
    if (con.connect_failed) {
      std::cout << "removing " << peer.first.string() << std::endl;
      remove_peer(peer.first);
      // leader is not supposed to quit gracefully in current design
      // so only pushing no poping
      dropped_peers.push_back(peer.first);
      // FIXME: when multiple nodes fail, skipping send can be a problem.
      break;
    }
    con.send(msg);
  }
}

auto Raft::broadcast_multi(const SocketAddress& node) -> void {
  // don't broadcast is no existing peers
  if (peers.size() == 0) {
    return;
  }

  cloud::CloudMessage msg;
  for (auto& peer : peers) {
    auto* partition = msg.add_partition();
    partition->set_id(1);
    partition->set_peer(peer.first.string());
  }

  msg.set_type(cloud::CloudMessage_Type_REQUEST);
  msg.set_operation(cloud::CloudMessage_Operation_RAFT_ADD_NODE);

  Connection con{node.string()};
  con.send(msg);
}

}  // namespace cloudlab
