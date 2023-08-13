#include "cloudlab/handler/p2p.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace cloudlab {

P2PHandler::P2PHandler(Routing* _routing)
    : routing{_routing},
      partitions{std::make_unique<
          std::unordered_map<uint32_t, std::unique_ptr<KVS>>>()},
      nodes{std::make_unique<std::unordered_set<SocketAddress>>()}

{
  const auto& own_addr = routing->get_backend_address();
  auto hash = std::hash<SocketAddress>()(own_addr);
  auto path = fmt::format("/tmp/{}-initial", hash);
  auto raft_path = fmt::format("/tmp/{}-raft", hash);
  partitions->insert({0, std::make_unique<KVS>(path)});
  raft = std::make_unique<Raft>(raft_path, own_addr.string());

  txManager = std::make_unique<TXManager>(routing, partitions.get());
}

auto P2PHandler::handle_connection(Connection& con) -> void {
  cloud::CloudMessage request{}, response{};

  if (!con.receive(request)) {
    return;
  }

  // prevent con with same address
  std::lock_guard<std::mutex> lck(mtx);

  if (raft->leader()) {
    routing->set_partitions_to_cluster_size();
  }

  switch (request.operation()) {
    case cloud::CloudMessage_Operation_PUT: {
      if (raft->leader()) {
        handle_key_operation_leader(request, response);
      } else {
        handle_put(request, response);
      }
      break;
    }
    case cloud::CloudMessage_Operation_GET: {
      if (raft->leader()) {
        handle_key_operation_leader(request, response);
      } else {
        handle_get(request, response);
      }
      break;
    }
    case cloud::CloudMessage_Operation_DELETE: {
      if (raft->leader()) {
        handle_key_operation_leader(request, response);
      } else {
        handle_delete(request, response);
      }
      break;
    }
    case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
      if (raft->leader()) {
        handle_join_cluster_leader(request, response);
      } else {
        handle_join_cluster(request, response);
      }
      break;
    }
    case cloud::CloudMessage_Operation_PARTITIONS_ADDED: {
      handle_partitions_added(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_PARTITIONS_REMOVED: {
      handle_partitions_removed(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_CREATE_PARTITIONS: {
      handle_create_partitions(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_STEAL_PARTITIONS: {
      handle_steal_partitions(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_DROP_PARTITIONS: {
      handle_drop_partitions(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_TRANSFER_PARTITION: {
      handle_transfer_partition(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES: {
      handle_raft_append_entries(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_ADD_NODE: {
      handle_raft_add_node(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_REMOVE_NODE: {
      handle_raft_remove_node(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_VOTE: {
      handle_raft_vote(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE: {
      handle_raft_dropped_node(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_GET_LEADER: {
      handle_raft_get_leader(request, response);
      break;
    }
    // case cloud::CloudMessage_Operation_RAFT_GET_TIMEOUT: {
    //   handle_raft_get_timeout(request, response);
    //   break;
    // }
    case cloud::CloudMessage_Operation_RAFT_DIRECT_GET: {
      handle_raft_direct_get(request, response);
      break;
    }
    case cloud::CloudMessage_Operation_TX_CLT_BEGIN:
    case cloud::CloudMessage_Operation_TX_CLT_COMMIT:
    case cloud::CloudMessage_Operation_TX_CLT_ABORT:
    case cloud::CloudMessage_Operation_TX_CLT_GET:
    case cloud::CloudMessage_Operation_TX_CLT_PUT:
    case cloud::CloudMessage_Operation_TX_CLT_DELETE:
    case cloud::CloudMessage_Operation_TX_BEGIN:
    case cloud::CloudMessage_Operation_TX_COMMIT:
    case cloud::CloudMessage_Operation_TX_ABORT:
    case cloud::CloudMessage_Operation_TX_GET:
    case cloud::CloudMessage_Operation_TX_PUT:
    case cloud::CloudMessage_Operation_TX_DELETE: {
      txManager->HandleMessage(request, response);
      break;
    }
    default:
      std::cout << "in default" << std::endl;
      response.set_type(cloud::CloudMessage_Type_RESPONSE);
      response.set_operation(request.operation());
      response.set_success(false);
      response.set_message("Operation not (yet) supported");
      break;
  }
  con.send(response);
}

auto P2PHandler::handle_key_operation_leader(const cloud::CloudMessage& msg,
                                             cloud::CloudMessage& response)
    -> void {
  // std::cout << "in P2PHandler::handle_key_operation_leader" << std::endl;
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(msg.operation());
  response.set_success(true);

  // for every key-value pair in the request we got
  for (const auto& kvp : msg.kvp()) {
    // look up peer responsible for key
    auto peer_address = routing->find_peer(kvp.key());

    auto* response_kvp = response.add_kvp();
    response_kvp->set_key(kvp.key());

    // if there is a peer -> send message containing key-value pair to peer
    if (peer_address.has_value()) {
      cloud::CloudMessage peer_request{}, peer_response{};

      Connection peer{peer_address.value()};
      // std::cout << "peer_address: " << peer_address.value().string() <<
      // std::endl;
      if (peer.connect_failed) {
        response_kvp->set_value("ERROR: operation failed");
        // con.send(response);
        return;
      }

      peer_request.set_type(cloud::CloudMessage_Type_REQUEST);
      peer_request.set_operation(msg.operation());

      auto* peer_request_kvp = peer_request.add_kvp();
      peer_request_kvp->set_key(kvp.key());
      peer_request_kvp->set_value(kvp.value());

      peer.send(peer_request);
      peer.receive(peer_response);

      if (peer_response.success() && peer_response.kvp_size() > 0) {
        response_kvp->set_value(peer_response.kvp().Get(0).value());
      } else {
        response_kvp->set_value("ERROR: operation failed");
      }
    } else {
      response_kvp->set_value("ERROR: no peer found for key");
    }

    if (msg.operation() == cloud::CloudMessage_Operation_PUT) {
      // append entry to local
      auto partition_id = routing->get_partition(kvp.key());
      std::cout << "adding to local: key: " << kvp.key()
                << ", value: " << kvp.value() << std::endl;
      raft->put(kvp.key(), kvp.value());
    } else if (msg.operation() == cloud::CloudMessage_Operation_GET) {
      std::string value;
      raft->get(kvp.key(), value);
      response_kvp->set_value(value);
    }
  }

  // con.send(response);
}

auto P2PHandler::handle_join_cluster_leader(const cloud::CloudMessage& msg,
                                            cloud::CloudMessage& response)
    -> void {
  std::optional<SocketAddress> peer{};

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);

  auto address = msg.address().address();
  std::cout << "in P2PHandler::handle_join_cluster_leader" << std::endl;
  try {
    peer = {SocketAddress{address}};

    response.set_success(true);
    response.set_message("OK");
  } catch (std::invalid_argument& e) {
    response.set_success(false);
    response.set_message(fmt::format("ERROR: {}", e.what()));
  }

  // Caller sends response
  // con.send(response);

  if (peer.has_value()) {
    std::cout << "from: " << peer.value().string() << std::endl;
    cloud::CloudMessage add_msg{};
    add_msg.set_type(cloud::CloudMessage_Type_REQUEST);
    add_msg.set_operation(cloud::CloudMessage_Operation_RAFT_ADD_NODE);

    auto* partition = add_msg.add_partition();
    // setting 1 as place holder
    partition->set_id(1);
    partition->set_peer(peer.value().string());

    // notify other nodes of this new node
    raft->broadcast(add_msg);
    // std::cout << "About to send broadcast_multi" << std::endl;

    // notify this new node of other existing nodes
    raft->broadcast_multi(peer.value());

    // send all existing kvp to new node.
    cloud::CloudMessage kvp_msg{};
    kvp_msg.set_type(cloud::CloudMessage_Type_REQUEST);
    kvp_msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    kvp_msg.set_success(true);
    kvp_msg.mutable_address()->set_address(
        routing->get_backend_address().string());
    std::vector<std::pair<std::string, std::string>> buffer;
    raft->get_all(buffer);
    std::cout << "kvp to send" << std::endl;
    for (auto& [key, value] : buffer) {
      auto* kvp = kvp_msg.add_kvp();
      std::cout << "key: " << key << ", value: " << value << std::endl;
      kvp->set_key(key);
      kvp->set_value(value);
    }

    Connection con{peer.value().string()};
    con.send(kvp_msg);

    add_new_node(peer.value());
  }
}

auto P2PHandler::handle_put(const cloud::CloudMessage& msg,
                            cloud::CloudMessage& response) -> void {
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition_id = routing->get_partition(kvp.key());

    if (partitions->contains(partition_id) &&
        partitions->at(partition_id)->put(kvp.key(), kvp.value())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
      response.set_success(false);
      response.set_message("ERROR");
    }
  }

  // con.send(response);
}

auto P2PHandler::handle_get(const cloud::CloudMessage& msg,
                            cloud::CloudMessage& response) -> void {
  std::string value;

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition_id = routing->get_partition(kvp.key());

    if (partitions->contains(partition_id) &&
        partitions->at(partition_id)->get(kvp.key(), value)) {
      tmp->set_value(value);
    } else {
      tmp->set_value("ERROR");
    }
  }

  // con.send(response);
}

auto P2PHandler::handle_delete(const cloud::CloudMessage& msg,
                               cloud::CloudMessage& response) -> void {
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition = routing->get_partition(kvp.key());

    if (partitions->contains(partition) &&
        partitions->at(partition)->remove(kvp.key())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
    }
  }

  // con.send(response);
}

auto P2PHandler::handle_join_cluster(const cloud::CloudMessage& msg,
                                     cloud::CloudMessage& response) -> void {
  cloud::CloudMessage cluster_request{}, cluster_response{};
  std::cout << "in P2PHandler::handle_join_cluster" << std::endl;
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
  response.set_success(true);
  response.set_message("OK");

  cluster_request.set_type(cloud::CloudMessage_Type_REQUEST);
  cluster_request.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
  cluster_request.mutable_address()->set_address(
      routing->get_backend_address().string());

  auto cluster_address = msg.address().address();

  try {
    Connection cluster_con{cluster_address};
    cluster_con.send(cluster_request);
    cluster_con.receive(cluster_response);

    if (cluster_response.success()) {
      routing->set_cluster_address({SocketAddress{cluster_address}});
      routing->set_partitions_to_cluster_size();
    }
  } catch (std::invalid_argument& e) {
    response.set_success(false);
    response.set_message(fmt::format("ERROR: {}", e.what()));
  } catch (std::runtime_error& e) {
    response.set_success(false);
    response.set_message(
        fmt::format("ERROR: Could not connect to {}", cluster_address));
  }

  // con.send(response);
}

auto P2PHandler::handle_create_partitions(const cloud::CloudMessage& msg,
                                          cloud::CloudMessage& response)
    -> void {
  std::cout << "in P2PHandler::handle_create_partitions" << std::endl;
  // FIXME: insert key-value pairs into cluster instead of forgetting about them
  partitions->clear();

  // create new partitions
  for (const auto& partition : msg.partition()) {
    auto hash = std::hash<SocketAddress>()(routing->get_backend_address());
    auto path = fmt::format("/tmp/{}-{}", hash, partition.id());
    partitions->insert({partition.id(), std::make_unique<KVS>(path)});
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_CREATE_PARTITIONS);
  response.set_success(true);
  response.set_message("OK");

  // con.send(response);
}

auto P2PHandler::add_new_node(const SocketAddress& peer) -> void {
  if (nodes->empty()) {
    // first node needs to create all partitions
    cloud::CloudMessage request{}, response{};
    std::cout << "in P2PHandler::add_new_node" << std::endl;
    request.set_type(cloud::CloudMessage_Type_REQUEST);
    request.set_operation(cloud::CloudMessage_Operation_CREATE_PARTITIONS);

    for (auto i = 0; i < cluster_partitions; i++) {
      auto* partition = request.add_partition();
      partition->set_id(i);
    }

    Connection con{peer};
    con.send(request);
    con.receive(response);

    if (response.success()) {
      nodes->insert(peer);
      for (auto partition = 0; partition < cluster_partitions; partition++) {
        routing->add_peer(partition, peer);
        raft->add_peer(peer);
      }
    }
  } else {
    // nth node to join needs to steal partitions from other nodes
    nodes->insert(peer);
    raft->add_peer(peer);
    // FIXME: do not re-distribute partitions during ongoing re-distribution
    redistribute_partitions();
  }
}

auto P2PHandler::redistribute_partitions() -> void {
  auto partitions_by_peer = routing->partitions_by_peer();
  std::cout << "in P2PHandler::redistribute_partitions" << std::endl;
  // insert all known nodes into partitions_by_peer
  for (const auto& peer : *(nodes)) {
    if (!partitions_by_peer.contains(peer)) {
      partitions_by_peer.insert({peer, {}});
    }
  }

  std::vector<std::pair<SocketAddress, uint32_t>> partitions_to_redistribute;

  // for every peer check if the peer has too many partitions
  for (auto [peer, peer_partitions] : partitions_by_peer) {
    auto it = peer_partitions.begin();

    for (auto i = 0;
         peer_partitions.size() - i > (cluster_partitions / nodes->size() + 1);
         i++) {
      partitions_to_redistribute.emplace_back(peer, *it++);
    }
  }

  // for every peer check if the peer has too few partitions
  for (auto [peer, peer_partitions] : partitions_by_peer) {
    cloud::CloudMessage request{}, response{};

    for (auto i = 0;
         peer_partitions.size() + i < (cluster_partitions / nodes->size() + 1);
         i++) {
      if (!partitions_to_redistribute.empty()) {
        auto partition = partitions_to_redistribute.back();

        auto* cloud_partition = request.add_partition();
        cloud_partition->set_id(partition.second);
        cloud_partition->set_peer(partition.first.string());

        partitions_to_redistribute.pop_back();
      }
    }

    if (!request.partition().empty()) {
      request.set_type(cloud::CloudMessage_Type_REQUEST);
      request.set_operation(cloud::CloudMessage_Operation_STEAL_PARTITIONS);

      Connection con{peer};
      con.send(request);
    }
  }

  // two possible implementation ways:
  // * force partition stealing (lock partition while re-distributing!)
  //   ->
  // * RAFT  2 nodes: add as follower, wait until consensus, remove lead
  // * RAFT >2 nodes: add as follower, remove other node
}

auto P2PHandler::handle_partitions_added(const cloud::CloudMessage& msg,
                                         cloud::CloudMessage& response)
    -> void {
  std::cout << "in P2PHandler::handle_partitions_added" << std::endl;
  for (const auto& partition : msg.partition()) {
    routing->add_peer(partition.id(), SocketAddress{partition.peer()});
    raft->add_peer(SocketAddress{partition.peer()});
  }
}

auto P2PHandler::handle_partitions_removed(const cloud::CloudMessage& msg,
                                           cloud::CloudMessage& response)
    -> void {
  std::cout << "in P2PHandler::handle_partitions_removed" << std::endl;
  for (const auto& partition : msg.partition()) {
    routing->remove_peer(partition.id(), SocketAddress{partition.peer()});
    // raft->remove_peer(SocketAddress{partition.peer()});
  }
}

auto P2PHandler::handle_steal_partitions(const cloud::CloudMessage& msg,
                                         cloud::CloudMessage& response)
    -> void {
  cloud::CloudMessage request{}, notification{};
  std::cout << "in P2PHandler::handle_steal_partitions" << std::endl;
  auto cluster_address = routing->get_cluster_address();

  if (!cluster_address.has_value()) {
    throw std::runtime_error{
        "received message to steal partitions but cluster address is not "
        "defined"};
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_STEAL_PARTITIONS);
  response.set_success(true);
  response.set_message("OK");
  // Caller sends reponse
  // con.send(response);

  response.Clear();

  request.set_type(cloud::CloudMessage_Type_REQUEST);
  request.set_operation(cloud::CloudMessage_Operation_TRANSFER_PARTITION);

  for (const auto& partition : msg.partition()) {
    Connection peer{partition.peer()};

    auto* cloud_partition = request.add_partition();
    cloud_partition->set_id(partition.id());

    peer.send(request);
    peer.receive(response);

    if (response.success()) {
      if (partitions->contains(partition.id())) {
        partitions->at(partition.id())->clear();
      } else {
        auto hash = std::hash<SocketAddress>()(routing->get_backend_address());
        auto path = fmt::format("/tmp/{}-{}", hash, partition.id());
        partitions->insert({partition.id(), std::make_unique<KVS>(path)});
      }

      for (const auto& kvp : response.kvp()) {
        partitions->at(partition.id())->put(kvp.key(), kvp.value());
      }
    } else {
      // we did not receive any partition from the peer
      continue;
    }

    // inform cluster of added partition
    notification.set_type(cloud::CloudMessage_Type_NOTIFICATION);
    notification.set_operation(cloud::CloudMessage_Operation_PARTITIONS_ADDED);

    cloud_partition = notification.add_partition();
    cloud_partition->set_id(partition.id());
    cloud_partition->set_peer(routing->get_backend_address().string());

    Connection cluster{cluster_address.value()};
    cluster.send(notification);

    // tell peer to drop partition
    request.set_type(cloud::CloudMessage_Type_REQUEST);
    request.set_operation(cloud::CloudMessage_Operation_DROP_PARTITIONS);

    Connection peer_new{partition.peer()};
    peer_new.send(request);
  }
}

auto P2PHandler::handle_drop_partitions(const cloud::CloudMessage& msg,
                                        cloud::CloudMessage& response) -> void {
  cloud::CloudMessage notification{};

  auto cluster_address = routing->get_cluster_address();

  if (!cluster_address.has_value()) {
    throw std::runtime_error{
        "received message to drop partitions but cluster address is not "
        "defined"};
  }

  notification.set_type(cloud::CloudMessage_Type_NOTIFICATION);
  notification.set_operation(cloud::CloudMessage_Operation_PARTITIONS_REMOVED);

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_DROP_PARTITIONS);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& partition : msg.partition()) {
    if (partitions->contains(partition.id())) {
      partitions->at(partition.id())->clear();
      auto* cloud_partition = notification.add_partition();
      cloud_partition->set_id(partition.id());
      cloud_partition->set_peer(routing->get_backend_address().string());
    }
  }
  // Caller sends response
  // con.send(response);

  Connection cluster{cluster_address.value()};
  cluster.send(notification);
}

auto P2PHandler::handle_transfer_partition(const cloud::CloudMessage& msg,
                                           cloud::CloudMessage& response)
    -> void {
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_TRANSFER_PARTITION);
  response.set_success(true);
  response.set_message("OK");

  auto partition_id = msg.partition().Get(0).id();

  std::vector<std::pair<std::string, std::string>> buffer;

  if (partitions->contains(partition_id)) {
    auto* partition = partitions->at(partition_id).get();
    partition->get_all(buffer);
  } else {
    response.set_success(false);
    response.set_message("ERROR: partition not stored on this peer");
  }

  for (auto& [key, value] : buffer) {
    auto* kvp = response.add_kvp();
    kvp->set_key(key);
    kvp->set_value(value);
  }

  // con.send(response);
}

auto P2PHandler::handle_raft_append_entries(const cloud::CloudMessage& msg,
                                            cloud::CloudMessage& response)
    -> void {
  // std::cout << "P2PHandler::handle_raft_append_entries" << std::endl;
  std::string leader_addr;
  raft->reset_election_timer();
  auto address = msg.address().address();
  auto req_term = msg.term();
  std::cout << "got append_entries from: " << address << std::endl;

  // FIXME: might need to remove leader address from peer to make the design
  // consistent

  raft->get_leader_addr(leader_addr);

  // first time receiving from new leader
  if (leader_addr != address) {
    raft->set_leader_addr(address);
    raft->remove_peer(SocketAddress{address});
    raft->reset_voted_for();
    if (!raft->follower()) {
      raft->set_follower();
    }

    raft->set_term(req_term);
  }

  for (const auto& kvp : msg.kvp()) {
    std::cout << "add " << kvp.key() << ", " << kvp.value() << std::endl;
    raft->put(kvp.key(), kvp.value());
  }
}

auto P2PHandler::handle_raft_vote(const cloud::CloudMessage& msg,
                                  cloud::CloudMessage& response) -> void {
  // got response from its own vote request
  if (msg.type() == cloud::CloudMessage_Type_RESPONSE) {
    std::cout << "got vote response" << std::endl;

    if (msg.success()) {
      raft->add_vote();
    }
  } else if (msg.type() == cloud::CloudMessage_Type_REQUEST) {
    raft->reset_election_timer();
    auto address = msg.address().address();
    auto req_term = msg.term();

    uint64_t my_term;
    raft->get_term(my_term);

    std::cout << "my term: " << my_term << ", got vote request: " << req_term
              << ", " << address << std::endl;
    if (my_term >= req_term && raft->has_voted()) return;
    std::cout << "voted for: " << address << std::endl;
    raft->set_voted_for(SocketAddress{address});
    raft->set_term(req_term);

    response.set_type(cloud::CloudMessage_Type_RESPONSE);
    response.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);
    response.set_success(true);

    // Guess it replies back
    // Connection rp_con{address};
    // rp_con.send(response);
  }
}

auto P2PHandler::handle_raft_add_node(const cloud::CloudMessage& msg,
                                      cloud::CloudMessage& response) -> void {
  std::optional<SocketAddress> peer{};
  std::cout << "in P2PHandler::handle_raft_add_node" << std::endl;

  // avoid node
  raft->reset_election_timer();

  auto address = msg.address().address();

  for (const auto& partition : msg.partition()) {
    raft->add_peer(SocketAddress{partition.peer()});
  }
  raft->print_peers();

  // try {
  //   peer = {SocketAddress{address}};
  // } catch (std::invalid_argument& e) {
  //   std::cout << "address has no value" << std::endl;
  // }

  // if (peer.has_value()) {
  //   raft->add_peer(peer.value());
  // }

  // raft->print_peers();
}

auto P2PHandler::handle_raft_remove_node(const cloud::CloudMessage& msg,
                                         cloud::CloudMessage& response)
    -> void {
  std::cout << "in P2PHandler::handle_raft_remove_node" << std::endl;
  std::optional<SocketAddress> peer{};

  auto address = msg.address().address();
  try {
    peer = {SocketAddress{address}};
  } catch (std::invalid_argument& e) {
    std::cout << "address has no value" << std::endl;
  }

  if (peer.has_value()) {
    raft->remove_peer(peer.value());
  }
}

auto P2PHandler::handle_raft_dropped_node(const cloud::CloudMessage& msg,
                                          cloud::CloudMessage& response)
    -> void {
  // std::cout << "in P2PHandler::handle_raft_dropped_node" << std::endl;
  std::vector<std::string> result;
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_DROPPED_NODE);

  raft->get_dropped_peers(result);

  if (result.size() == 0) {
    response.set_success(false);
    response.set_message("No dropped nodes yet.");
  } else {
    response.set_success(true);
    for (const auto& node_ip : result) {
      auto* response_kvp = response.add_kvp();
      response_kvp->set_key("0");
      response_kvp->set_value(node_ip);
    }
  }
  response.set_success(true);
  // con.send(response);
}

auto P2PHandler::handle_raft_get_leader(const cloud::CloudMessage& msg,
                                        cloud::CloudMessage& response) -> void {
  // std::cout << "in P2PHandler::handle_raft_get_leader" << std::endl;
  std::string result;
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_GET_LEADER);
  response.set_success(true);
  raft->get_leader_addr(result);
  response.set_message(result);

  // con.send(response);
}

// auto P2PHandler::handle_raft_get_timeout(// const cloud::CloudMessage& msg,
// cloud::CloudMessage& response)
//     -> void {
//   // std::cout << "in P2PHandler::handle_raft_get_timeout" << std::endl;
//
//   std::string result;
//   response.set_type(cloud::CloudMessage_Type_RESPONSE);
//   response.set_operation(cloud::CloudMessage_Operation_RAFT_GET_TIMEOUT);
//   response.set_success(true);
//   raft->get_timeout(result);
//   response.set_message(result);

//   con.send(response);
// }

auto P2PHandler::handle_raft_direct_get(const cloud::CloudMessage& msg,
                                        cloud::CloudMessage& response) -> void {
  std::cout << "in P2PHandler::handle_raft_direct_get" << std::endl;
  std::string result;
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_DIRECT_GET);
  response.set_success(true);
  // response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());
    raft->get(kvp.key(), result);
    tmp->set_value(result);
  }

  // con.send(response);
}

}  // namespace cloudlab
