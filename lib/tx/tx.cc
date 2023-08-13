#include "cloudlab/tx/tx.hh"
#include "cloudlab/message/message_helper.hh"
#include "cloudlab/network/connection.hh"
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <fmt/core.h>

namespace cloudlab {

auto TXManager::GetTransactionPairs(const MessageHelper& msg,
                                    MessageHelper& resHelper) -> PairVec {
  // TODO(you)
  return {};
}

auto TXManager::HandleOpCoordinator(const cloud::CloudMessage_Operation& op,
                                    const cloud::CloudMessage_Operation& peerOp,
                                    const cloud::CloudMessage& request,
                                    cloud::CloudMessage& response) -> void {
  // TODO(you)
}

auto TXManager::HandleOpParticipant(const cloud::CloudMessage_Operation& op,
                                    const cloud::CloudMessage& request,
                                    cloud::CloudMessage& response) -> void {
  // TODO(you)
}

auto TXManager::HandleMessage(const cloud::CloudMessage& request,
                              cloud::CloudMessage& response) -> void {
  std::cout << "TXManager::HandleMessage\n";

  assert(request.type() == cloud::CloudMessage_Type_REQUEST);
  switch (request.operation()) {
    case cloud::CloudMessage_Operation_TX_CLT_BEGIN:
      assert(IsCoordinator());
      HandleBeginCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_COMMIT:
      assert(IsCoordinator());
      HandleCommitCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_ABORT:
      assert(IsCoordinator());
      HandleAbortCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_GET:
      assert(IsCoordinator());
      HandleGetCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_PUT:
      assert(IsCoordinator());
      HandlePutCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_CLT_DELETE:
      assert(IsCoordinator());
      HandleDeleteCoordinator(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_BEGIN:
      assert(!IsCoordinator());
      HandleBeginParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_COMMIT:
      assert(!IsCoordinator());
      HandleCommitParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_ABORT:
      assert(!IsCoordinator());
      HandleAbortParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_GET:
      assert(!IsCoordinator());
      HandleGetParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_PUT:
      assert(!IsCoordinator());
      HandlePutParticipant(request, response);
      break;
    case cloud::CloudMessage_Operation_TX_DELETE:
      assert(!IsCoordinator());
      HandleDeleteParticipant(request, response);
      break;
    default:
      std::cout << "TXManager::HandleMessage default" << std::endl;
      response.set_type(cloud::CloudMessage_Type_RESPONSE);
      response.set_operation(request.operation());
      response.set_success(false);
      response.set_message("TX Operation not supported");
      assert(false);
      break;
  }
}

auto TXManager::HandleBeginCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleBeginCoordinator\n";
}
auto TXManager::HandleCommitCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleCommitCoordinator\n";
}
auto TXManager::HandleAbortCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleAbortCoordinator\n";
}
auto TXManager::HandleGetCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleGetCoordinator\n";
}
auto TXManager::HandlePutCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandlePutCoordinator\n";
}
auto TXManager::HandleDeleteCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleDeleteCoordinator\n";
}
auto TXManager::HandleBeginParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleBeginParticipant\n";
}
auto TXManager::HandleCommitParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleCommitParticipant\n";
}
auto TXManager::HandleAbortParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleAbortParticipant\n";
}
auto TXManager::HandleGetParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleGetParticipant\n";
}
auto TXManager::HandlePutParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandlePutParticipant\n";
}
auto TXManager::HandleDeleteParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::cout << "TXManager::HandleDeleteParticipant\n";
}

}  // namespace cloudlab
