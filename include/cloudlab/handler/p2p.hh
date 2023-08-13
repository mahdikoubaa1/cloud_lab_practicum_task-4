#ifndef CLOUDLAB_P2P_HH
#define CLOUDLAB_P2P_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/kvs.hh"
#include "cloudlab/network/routing.hh"
#include "cloudlab/raft/raft.hh"
#include "cloudlab/tx/tx.hh"

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace cloudlab {

/**
 * Handler for P2P requests. Takes care of the messages from peers, cluster
 * metadata / routing tier, and the API.
 */
class P2PHandler : public ServerHandler {
 public:
  explicit P2PHandler(Routing* _routing);

  auto handle_connection(Connection& con) -> void override;

  auto set_tx_coordinator() -> void {
    txManager->SetRole(TXRole::COORDINATOR);
  }

  auto set_raft_leader() -> void {
    raft->set_leader();
  }

  auto set_raft_candidate() -> void {
    raft->set_candidate();
  }

  auto set_raft_follower() -> void {
    raft->set_follower();
  }

  auto get_raft_role() -> RaftRole {
    return raft->get_role();
  }

  auto raft_run() -> std::thread {
    return raft->run(routing, mtx);
  }

  auto raft_add_peer(const SocketAddress& peer) -> void {
    raft->add_peer(peer);
  }

  auto raft_remove_peer(const SocketAddress& peer) -> void {
    raft->remove_peer(peer);
  }

 private:
  auto handle_put(const cloud::CloudMessage& msg, cloud::CloudMessage& response)
      -> void;
  auto handle_get(const cloud::CloudMessage& msg, cloud::CloudMessage& response)
      -> void;
  auto handle_delete(const cloud::CloudMessage& msg,
                     cloud::CloudMessage& response) -> void;
  auto handle_join_cluster(const cloud::CloudMessage& msg,
                           cloud::CloudMessage& response) -> void;
  auto handle_create_partitions(const cloud::CloudMessage& msg,
                                cloud::CloudMessage& response) -> void;
  auto handle_steal_partitions(const cloud::CloudMessage& msg,
                               cloud::CloudMessage& response) -> void;
  auto handle_drop_partitions(const cloud::CloudMessage& msg,
                              cloud::CloudMessage& response) -> void;
  auto handle_transfer_partition(const cloud::CloudMessage& msg,
                                 cloud::CloudMessage& response) -> void;
  auto handle_raft_append_entries(const cloud::CloudMessage& msg,
                                  cloud::CloudMessage& response) -> void;
  auto handle_raft_vote(const cloud::CloudMessage& msg,
                        cloud::CloudMessage& response) -> void;
  auto handle_raft_add_node(const cloud::CloudMessage& msg,
                            cloud::CloudMessage& response) -> void;
  auto handle_raft_remove_node(const cloud::CloudMessage& msg,
                               cloud::CloudMessage& response) -> void;
  auto handle_raft_dropped_node(const cloud::CloudMessage& msg,
                                cloud::CloudMessage& response) -> void;
  auto handle_raft_get_leader(const cloud::CloudMessage& msg,
                              cloud::CloudMessage& response) -> void;
  auto handle_raft_get_timeout(const cloud::CloudMessage& msg,
                               cloud::CloudMessage& response) -> void;
  auto handle_raft_direct_get(const cloud::CloudMessage& msg,
                              cloud::CloudMessage& response) -> void;

  // from router.hh
  auto handle_key_operation_leader(const cloud::CloudMessage& msg,
                                   cloud::CloudMessage& response) -> void;
  auto handle_join_cluster_leader(const cloud::CloudMessage& msg,
                                  cloud::CloudMessage& response) -> void;
  auto handle_partitions_added(const cloud::CloudMessage& msg,
                               cloud::CloudMessage& response) -> void;
  auto handle_partitions_removed(const cloud::CloudMessage& msg,
                                 cloud::CloudMessage& response) -> void;
  auto add_new_node(const SocketAddress& peer) -> void;
  auto redistribute_partitions() -> void;

  // partitions stored on this peer: [partition ID -> KVS]
  std::unique_ptr<std::unordered_map<uint32_t, std::unique_ptr<KVS>>>
      partitions;

  std::unique_ptr<std::unordered_set<SocketAddress>> nodes;

  std::unique_ptr<Raft> raft;
  Routing* routing;
  std::mutex mtx;
  // Transactions Manager. Does it's own message handling.
  std::unique_ptr<TXManager> txManager;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_P2P_HH
