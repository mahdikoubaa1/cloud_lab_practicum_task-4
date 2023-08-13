#ifndef CLOUDLAB_TX_HH
#define CLOUDLAB_TX_HH

#include "cloud.pb.h"
#include "cloudlab/kvs.hh"
#include "cloudlab/message/message_helper.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/network/routing.hh"

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace cloudlab {

enum class TXRole {
  PARTICIPANT,
  COORDINATOR
};

class TXManager {
 public:
  using PartitionsPtr = std::unordered_map<uint32_t, std::unique_ptr<KVS>>*;
  using NodesPtr = std::unordered_set<SocketAddress>*;
  using RoutingPtr = Routing*;
  using PairVec = std::vector<std::pair<std::string, std::string>>;
  using KeySet = std::set<std::string>;
  using TransactionKeysMap = std::map<std::string, KeySet>;

  auto SetRole(const TXRole& _val) -> void {
    role = _val;
  }

  explicit TXManager(RoutingPtr _routing, PartitionsPtr _partitions)
      : routing{_routing}, partitions{_partitions} {};

  auto GetTransactionPairs(const MessageHelper& msg, MessageHelper& resHelper)
      -> PairVec;
  auto HandleOpCoordinator(const cloud::CloudMessage_Operation& op,
                           const cloud::CloudMessage_Operation& peerOp,
                           const cloud::CloudMessage& request,
                           cloud::CloudMessage& response) -> void;
  auto HandleOpParticipant(const cloud::CloudMessage_Operation& op,
                           const cloud::CloudMessage& request,
                           cloud::CloudMessage& response) -> void;

  auto HandleMessage(const cloud::CloudMessage& request,
                     cloud::CloudMessage& response) -> void;

 private:
  auto HandleBeginCoordinator(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void;
  auto HandleCommitCoordinator(const cloud::CloudMessage& request,
                               cloud::CloudMessage& response) -> void;
  auto HandleAbortCoordinator(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void;
  auto HandleGetCoordinator(const cloud::CloudMessage& request,
                            cloud::CloudMessage& response) -> void;
  auto HandlePutCoordinator(const cloud::CloudMessage& request,
                            cloud::CloudMessage& response) -> void;
  auto HandleDeleteCoordinator(const cloud::CloudMessage& request,
                               cloud::CloudMessage& response) -> void;
  auto HandleBeginParticipant(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void;
  auto HandleCommitParticipant(const cloud::CloudMessage& request,
                               cloud::CloudMessage& response) -> void;
  auto HandleAbortParticipant(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void;
  auto HandleGetParticipant(const cloud::CloudMessage& request,
                            cloud::CloudMessage& response) -> void;
  auto HandlePutParticipant(const cloud::CloudMessage& request,
                            cloud::CloudMessage& response) -> void;
  auto HandleDeleteParticipant(const cloud::CloudMessage& request,
                               cloud::CloudMessage& response) -> void;

  auto GetRole() -> TXRole {
    return role;
  }
  auto IsCoordinator() -> bool {
    return GetRole() == TXRole::COORDINATOR;
  }

  RoutingPtr routing{};
  PartitionsPtr partitions{};
  TXRole role{TXRole::PARTICIPANT};
  TransactionKeysMap transactionKeysMap{};
};

}  // namespace cloudlab

#endif
