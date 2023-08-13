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
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleBeginCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_CLT_BEGIN);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  std::unordered_map<SocketAddress,
                     std::pair<std::unique_ptr<Connection>,
                               std::unique_ptr<cloud::CloudMessage>>>
      tosend;
  std::map<std::string, KeySet>::iterator txentry;
  if (transactionKeysMap.contains(request.tx_id())) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  txentry = transactionKeysMap.insert({request.tx_id(), {}}).first;
  for (const auto& kvp : request.kvp()) {
    txentry->second.insert(kvp.key());
    if (!response.success()) continue;
    auto peer = SocketAddress{routing->find_peer(kvp.key()).value()};
    auto it = tosend.find(peer);
    if (it != tosend.end()) {
      auto* tmp = it->second.second->add_kvp();
      tmp->set_value("");
      tmp->set_key(kvp.key());
    } else {
      std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                  std::make_unique<cloud::CloudMessage>()};
      if (p.first->connect_failed) {
        response.set_success(false);
        response.set_message("ERROR");
        continue;
      }

      p.second->set_success(true);
      p.second->set_message("OK");
      p.second->set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
      p.second->set_type(cloud::CloudMessage_Type_REQUEST);
      p.second->set_tx_id(request.tx_id());
      auto* tmp = p.second->add_kvp();
      tmp->set_key(kvp.key());
      tmp->set_value("");
      tosend.insert({peer, std::move(p)});
    }
  }
  if (!response.success()) {
    transactionBeginMap.insert({request.tx_id(), false});
    return;
  }
  lck.unlock();
  for (const auto& s : tosend) {
    s.second.first->send(*s.second.second);
  }
  for (auto it = tosend.begin(); it != tosend.end();) {
    if (!it->second.first->receive(*it->second.second) ||
        !it->second.second->success()) {
      response.set_success(false);
      response.set_message("ERROR");
      it = tosend.erase(it);
    } else
      ++it;
  }
  lck.lock();
  if (!response.success()) {
    cloud::CloudMessage ack;
    ack.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
    ack.set_type(cloud::CloudMessage_Type_REQUEST);
    ack.set_tx_id(request.tx_id());
    transactionBeginMap.insert({request.tx_id(), false});
    for (auto& s : tosend) {
      s.second.first = std::make_unique<Connection>(s.first);
      s.second.first->send(ack);
    }
    for (const auto& r : tosend) {
      r.second.first->receive(ack);
    }
  } else {
    transactionBeginMap.insert({request.tx_id(), true});
  }
}
auto TXManager::HandleCommitCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  std::unique_lock<std::mutex> lck(mtx);

  // TODO(you)
  std::cout << "TXManager::HandleCommitCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_CLT_COMMIT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto tocommit = transactionBeginMap.find(request.tx_id());
  if (tocommit == transactionBeginMap.end()) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  cloud::CloudMessage commitmsg1;
  commitmsg1.set_operation(cloud::CloudMessage_Operation_TX_COMMIT);
  commitmsg1.set_type(cloud::CloudMessage_Type_REQUEST);
  commitmsg1.set_tx_id(request.tx_id());
  commitmsg1.set_message("1");
  std::unordered_map<SocketAddress, std::unique_ptr<Connection>> connections;
  auto tocommitk = transactionKeysMap.find(request.tx_id());
  if (tocommit->second) {
    for (const auto& key : tocommitk->second) {
      auto peer{routing->find_peer(key).value()};
      if (connections.contains(peer)) continue;
      auto conit =
          connections.insert({peer, std::make_unique<Connection>(peer)}).first;
      if (conit->second->connect_failed) {
        response.set_message("ERROR");
        response.set_success(false);
        connections.erase(conit);
        continue;
      }
    }
    if (response.success()) {
      for (const auto& con : connections) {
        con.second->send(commitmsg1);
      }
      for (auto conit = connections.begin(); conit != connections.end();) {
        if (!conit->second->receive(commitmsg1)) {
          response.set_message("ERROR");
          response.set_success(false);
          conit = connections.erase(conit);
          continue;
        }
        if (!commitmsg1.success()) {
          response.set_message("ERROR");
          response.set_success(false);
        }
        ++conit;
      }
      for (auto conit = connections.begin(); conit != connections.end();) {
        conit->second = std::make_unique<Connection>(conit->first);
        if (conit->second->connect_failed) {
          response.set_message("ERROR");
          response.set_success(false);
          conit = connections.erase(conit);
          continue;
        }
        ++conit;
      }
    }
    cloud::CloudMessage msg2;
    msg2.set_type(cloud::CloudMessage_Type_REQUEST);
    msg2.set_tx_id(request.tx_id());
    if (response.success()) {
      msg2.set_operation(cloud::CloudMessage_Operation_TX_COMMIT);
      msg2.set_message("2");
    } else {
      msg2.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
    }
    for (auto& con : connections) {
      con.second->send(msg2);
    }
    for (auto& con : connections) {
      con.second->receive(msg2);
    }
  }
  transactionBeginMap.erase(tocommit);
  transactionKeysMap.erase(tocommitk);
}
auto TXManager::HandleAbortCoordinator(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  std::unique_lock<std::mutex> lck(mtx);

  std::cout << "TXManager::HandleAbortCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_CLT_ABORT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto toabort = transactionBeginMap.find(request.tx_id());
  if (toabort == transactionBeginMap.end()) {
    return;
  }
  cloud::CloudMessage abortmsg;
  abortmsg.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
  abortmsg.set_type(cloud::CloudMessage_Type_REQUEST);
  abortmsg.set_tx_id(request.tx_id());
  std::unordered_map<SocketAddress, std::unique_ptr<Connection>> connections;
  auto toabortk = transactionKeysMap.find(request.tx_id());
  if (toabort->second) {
    for (const auto& key : toabortk->second) {
      auto peer{routing->find_peer(key).value()};
      if (connections.contains(peer)) continue;
      auto conit =
          connections.insert({peer, std::make_unique<Connection>(peer)}).first;
      conit->second->send(abortmsg);
    }
    for (const auto& con : connections) {
      con.second->receive(abortmsg);
    }
  }
  transactionBeginMap.erase(toabort);
  transactionKeysMap.erase(toabortk);
}
auto TXManager::HandleGetCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);

  std::cout << "TXManager::HandleGetCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_CLT_GET);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto txentry = transactionKeysMap.find(request.tx_id());
  if (txentry == transactionKeysMap.end()) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  auto txbentry = transactionBeginMap.find(request.tx_id());
  if (!txbentry->second) {
    std::cout<< "get needs being first\n";
    std::unordered_map<SocketAddress,
                       std::pair<std::unique_ptr<Connection>,
                                 std::unique_ptr<cloud::CloudMessage>>>
        tosend;
    for (const auto& key : txentry->second) {
      auto peer = SocketAddress{routing->find_peer(key).value()};
      auto it = tosend.find(peer);
      if (it != tosend.end()) {
        auto* tmp = it->second.second->add_kvp();
        tmp->set_value("");
        tmp->set_key(key);
      } else {
        std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                    std::make_unique<cloud::CloudMessage>()};
        if (p.first->connect_failed) {
          response.set_success(false);
          response.set_message("ERROR");
          return;
        }
        p.second->set_success(true);
        p.second->set_message("OK");
        p.second->set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
        p.second->set_type(cloud::CloudMessage_Type_REQUEST);
        p.second->set_tx_id(request.tx_id());
        auto* tmp = p.second->add_kvp();
        tmp->set_key(key);
        tmp->set_value("");
        tosend.insert({peer, std::move(p)});
      }
    }
    for (const auto& s : tosend) {
      s.second.first->send(*s.second.second);
    }
    for (auto it = tosend.begin(); it != tosend.end();) {
      if (!it->second.first->receive(*it->second.second) ||
          !it->second.second->success()) {
        response.set_success(false);
        response.set_message("ERROR");
        it = tosend.erase(it);
      } else
        ++it;
    }
    if (!response.success()) {
      cloud::CloudMessage ack;
      ack.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
      ack.set_type(cloud::CloudMessage_Type_REQUEST);
      ack.set_tx_id(request.tx_id());
      for (auto& s : tosend) {
        s.second.first = std::make_unique<Connection>(s.first);
        s.second.first->send(ack);
      }
      for (const auto& r : tosend) {
        r.second.first->receive(ack);
      }
      return;
    }
    txbentry->second = true;
  }
  std::unordered_map<SocketAddress,
                     std::pair<std::unique_ptr<Connection>,
                               std::unique_ptr<cloud::CloudMessage>>>
      tosendop;
  for (const auto& kvp : request.kvp()) {
    if (!txentry->second.contains(kvp.key())) {
      auto* tmp1 = response.add_kvp();
      tmp1->set_value("ERROR");
      tmp1->set_key(kvp.key());
      continue;
    }
    auto peer = SocketAddress{routing->find_peer(kvp.key()).value()};
    auto it = tosendop.find(peer);
    if (it != tosendop.end()) {
      auto* tmp = it->second.second->add_kvp();
      tmp->set_value("");
      tmp->set_key(kvp.key());
    } else {
      std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                  std::make_unique<cloud::CloudMessage>()};
      if (p.first->connect_failed) {
        auto* tmp1 = response.add_kvp();

        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
        continue;
      }
      p.second->set_success(true);
      p.second->set_message("OK");
      p.second->set_operation(cloud::CloudMessage_Operation_TX_GET);
      p.second->set_type(cloud::CloudMessage_Type_REQUEST);
      p.second->set_tx_id(request.tx_id());
      auto* tmp = p.second->add_kvp();
      tmp->set_key(kvp.key());
      tmp->set_value("");
      tosendop.insert({peer, std::move(p)});
    }
  }
  for (const auto& s : tosendop) {
    s.second.first->send(*s.second.second);
  }
  cloud::CloudMessage received;
  for (auto& rec : tosendop) {
    if (!rec.second.first->receive(received)) {
      for (const auto& kvp : rec.second.second->kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
      }
    } else {
      for (const auto& kvp : received.kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value(kvp.value());
        tmp1->set_key(kvp.key());
      }
    }
  }
}
auto TXManager::HandlePutCoordinator(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandlePutCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_CLT_PUT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto txentry = transactionKeysMap.find(request.tx_id());
  if (txentry == transactionKeysMap.end()) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  auto txbentry = transactionBeginMap.find(request.tx_id());
  if (!txbentry->second) {
    std::unordered_map<SocketAddress,
                       std::pair<std::unique_ptr<Connection>,
                                 std::unique_ptr<cloud::CloudMessage>>>
        tosend;
    for (const auto& key : txentry->second) {
      auto peer = SocketAddress{routing->find_peer(key).value()};
      auto it = tosend.find(peer);
      if (it != tosend.end()) {
        auto* tmp = it->second.second->add_kvp();
        tmp->set_value("");
        tmp->set_key(key);
      } else {
        std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                    std::make_unique<cloud::CloudMessage>()};
        if (p.first->connect_failed) {
          response.set_success(false);
          response.set_message("ERROR");
          return;
        }
        p.second->set_success(true);
        p.second->set_message("OK");
        p.second->set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
        p.second->set_type(cloud::CloudMessage_Type_REQUEST);
        p.second->set_tx_id(request.tx_id());
        auto* tmp = p.second->add_kvp();
        tmp->set_key(key);
        tmp->set_value("");
        tosend.insert({peer, std::move(p)});
      }
    }
    for (const auto& s : tosend) {
      s.second.first->send(*s.second.second);
    }
    for (auto it = tosend.begin(); it != tosend.end();) {
      if (!it->second.first->receive(*it->second.second) ||
          !it->second.second->success()) {
        response.set_success(false);
        response.set_message("ERROR");
        it = tosend.erase(it);
      } else
        ++it;
    }
    if (!response.success()) {
      cloud::CloudMessage ack;
      ack.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
      ack.set_type(cloud::CloudMessage_Type_REQUEST);
      ack.set_tx_id(request.tx_id());
      for (auto& s : tosend) {
        s.second.first = std::make_unique<Connection>(s.first);
        s.second.first->send(ack);
      }
      for (const auto& r : tosend) {
        r.second.first->receive(ack);
      }
      return;
    }
    txbentry->second = true;
  }
  std::unordered_map<SocketAddress,
                     std::pair<std::unique_ptr<Connection>,
                               std::unique_ptr<cloud::CloudMessage>>>
      tosendop;
  for (const auto& kvp : request.kvp()) {
    if (!txentry->second.contains(kvp.key())) {
      response.set_message("ERROR");
      response.set_success(false);
      auto* tmp1 = response.add_kvp();
      tmp1->set_value("ERROR");
      tmp1->set_key(kvp.key());
      continue;
    }
    auto peer = SocketAddress{routing->find_peer(kvp.key()).value()};
    auto it = tosendop.find(peer);
    if (it != tosendop.end()) {
      auto* tmp = it->second.second->add_kvp();
      tmp->set_value(kvp.value());
      tmp->set_key(kvp.key());
    } else {
      std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                  std::make_unique<cloud::CloudMessage>()};
      if (p.first->connect_failed) {
        response.set_message("ERROR");
        response.set_success(false);
        auto* tmp1 = response.add_kvp();
        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
        continue;
      }
      p.second->set_success(true);
      p.second->set_message("OK");
      p.second->set_operation(cloud::CloudMessage_Operation_TX_PUT);
      p.second->set_type(cloud::CloudMessage_Type_REQUEST);
      p.second->set_tx_id(request.tx_id());
      auto* tmp = p.second->add_kvp();
      tmp->set_key(kvp.key());
      tmp->set_value(kvp.value());
      tosendop.insert({peer, std::move(p)});
    }
  }
  for (const auto& s : tosendop) {
    s.second.first->send(*s.second.second);
  }
  cloud::CloudMessage received;
  for (auto& rec : tosendop) {
    if (!rec.second.first->receive(received)) {
      response.set_message("ERROR");
      response.set_success(false);
      for (const auto& kvp : rec.second.second->kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
      }
    } else {
      for (const auto& kvp : received.kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value(kvp.value());
        tmp1->set_key(kvp.key());
      }
    }
  }
}
auto TXManager::HandleDeleteCoordinator(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleDeleteCoordinator\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_DELETE);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto txentry = transactionKeysMap.find(request.tx_id());
  if (txentry == transactionKeysMap.end()) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  auto txbentry = transactionBeginMap.find(request.tx_id());
  if (!txbentry->second) {
    std::unordered_map<SocketAddress,
                       std::pair<std::unique_ptr<Connection>,
                                 std::unique_ptr<cloud::CloudMessage>>>
        tosend;
    for (const auto& key : txentry->second) {
      auto peer = SocketAddress{routing->find_peer(key).value()};
      auto it = tosend.find(peer);
      if (it != tosend.end()) {
        auto* tmp = it->second.second->add_kvp();
        tmp->set_value("");
        tmp->set_key(key);
      } else {
        std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                    std::make_unique<cloud::CloudMessage>()};
        if (p.first->connect_failed) {
          response.set_success(false);
          response.set_message("ERROR");
          return;
        }
        p.second->set_success(true);
        p.second->set_message("OK");
        p.second->set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
        p.second->set_type(cloud::CloudMessage_Type_REQUEST);
        p.second->set_tx_id(request.tx_id());
        auto* tmp = p.second->add_kvp();
        tmp->set_key(key);
        tmp->set_value("");
        tosend.insert({peer, std::move(p)});
      }
    }
    for (const auto& s : tosend) {
      s.second.first->send(*s.second.second);
    }
    for (auto it = tosend.begin(); it != tosend.end();) {
      if (!it->second.first->receive(*it->second.second) ||
          !it->second.second->success()) {
        response.set_success(false);
        response.set_message("ERROR");
        it = tosend.erase(it);
      } else
        ++it;
    }
    if (!response.success()) {
      cloud::CloudMessage ack;
      ack.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
      ack.set_type(cloud::CloudMessage_Type_REQUEST);
      ack.set_tx_id(request.tx_id());
      for (auto& s : tosend) {
        s.second.first = std::make_unique<Connection>(s.first);
        s.second.first->send(ack);
      }
      for (const auto& r : tosend) {
        r.second.first->receive(ack);
      }
      return;
    }
    txbentry->second = true;
  }
  std::unordered_map<SocketAddress,
                     std::pair<std::unique_ptr<Connection>,
                               std::unique_ptr<cloud::CloudMessage>>>
      tosendop;
  for (const auto& kvp : request.kvp()) {
    if (!txentry->second.contains(kvp.key())) {
      auto* tmp1 = response.add_kvp();
      tmp1->set_value("ERROR");
      tmp1->set_key(kvp.key());
      continue;
    }
    auto peer = SocketAddress{routing->find_peer(kvp.key()).value()};
    auto it = tosendop.find(peer);
    if (it != tosendop.end()) {
      auto* tmp = it->second.second->add_kvp();
      tmp->set_value("");
      tmp->set_key(kvp.key());
    } else {
      std::pair p{std::make_unique<Connection>(SocketAddress(peer)),
                  std::make_unique<cloud::CloudMessage>()};
      if (p.first->connect_failed) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
        continue;
      }
      p.second->set_success(true);
      p.second->set_message("OK");
      p.second->set_operation(cloud::CloudMessage_Operation_TX_DELETE);
      p.second->set_type(cloud::CloudMessage_Type_REQUEST);
      p.second->set_tx_id(request.tx_id());
      auto* tmp = p.second->add_kvp();
      tmp->set_key(kvp.key());
      tmp->set_value("");
      tosendop.insert({peer, std::move(p)});
    }
  }
  for (const auto& s : tosendop) {
    s.second.first->send(*s.second.second);
  }
  cloud::CloudMessage received;
  for (auto& rec : tosendop) {
    if (!rec.second.first->receive(received)) {
      for (const auto& kvp : rec.second.second->kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value("ERROR");
        tmp1->set_key(kvp.key());
      }
    } else {
      for (const auto& kvp : received.kvp()) {
        auto* tmp1 = response.add_kvp();
        tmp1->set_value(kvp.value());
        tmp1->set_key(kvp.key());
      }
    }
  }
}
auto TXManager::HandleBeginParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleBeginParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_BEGIN);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  const auto& txid = request.tx_id();
  if (transactionPartitionsMap.contains(txid)) {
    response.set_message("ERROR");
    response.set_success(false);
    return;
  }
  std::string result;
  auto txpentry = transactionPartitionsMap.insert({txid, {}}).first;
  for (const auto& kvp : request.kvp()) {
    auto partid = routing->get_partition(kvp.key());
    auto target = partitions->find(partid);
    if (target != partitions->end()) {
      if (!txpentry->second.contains(partid)) {
        if (!get<0>(target->second->tx_begin(txid))) {
          response.set_message("ERROR");
          response.set_success(false);

          break;
        }
        txpentry->second.insert(partid);
      }
      lck.unlock();
      if (!get<0>(target->second->tx_get(txid, kvp.key(), result))) {
        if (!get<0>(target->second->tx_put(txid, kvp.key(), ""))) {
          response.set_message("ERROR");
          response.set_success(false);
          break;
        }
        target->second->tx_del(txid, kvp.key());
      }
      lck.lock();
    } else {
      response.set_message("ERROR");
      response.set_success(false);
      break;
    }
  }
  if (!response.success()) {
    for (const auto& partid : txpentry->second) {
      auto target = partitions->find(partid);
      if (target != partitions->end()) {
        target->second->tx_abort(txid);
      }
    }
    transactionPartitionsMap.erase(txpentry);
  }
}
auto TXManager::HandleCommitParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleCommitParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_COMMIT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  if (request.message() == "1") return;
  auto tocommit = transactionPartitionsMap.find(request.tx_id());
  if (tocommit == transactionPartitionsMap.end()) return;
  for (const auto& partid : tocommit->second) {
    auto kvscommit = partitions->find(partid);
    if (kvscommit == partitions->end() ||
        !get<0>(kvscommit->second->tx_commit(request.tx_id()))) {
      response.set_success(false);
      response.set_message("ERROR");
    }
  }
  transactionPartitionsMap.erase(tocommit);
}
auto TXManager::HandleAbortParticipant(const cloud::CloudMessage& request,
                                       cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleAbortParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_ABORT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  auto toabort = transactionPartitionsMap.find(request.tx_id());
  if (toabort == transactionPartitionsMap.end()) return;
  for (const auto& partid : toabort->second) {
    auto kvsabort = partitions->find(partid);
    if (kvsabort == partitions->end() ||
        !get<0>(kvsabort->second->tx_abort(request.tx_id()))) {
      response.set_success(false);
      response.set_message("ERROR");
    }
  }
  transactionPartitionsMap.erase(toabort);
}
auto TXManager::HandleGetParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleGetParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_GET);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  std::string result{};
  for (const auto& kvp : request.kvp()) {
    auto kvs = partitions->find(routing->get_partition(kvp.key()));
    auto* tmp1 = response.add_kvp();
    tmp1->set_key(kvp.key());
    if (kvs != partitions->end() &&
        get<0>(kvs->second->tx_get(request.tx_id(), kvp.key(), result))) {
      tmp1->set_value(result);
    } else {
      tmp1->set_value("ERROR");
    }
  }
}
auto TXManager::HandlePutParticipant(const cloud::CloudMessage& request,
                                     cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandlePutParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_TX_PUT);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  for (const auto& kvp : request.kvp()) {
    auto kvs = partitions->find(routing->get_partition(kvp.key()));
    auto* tmp1 = response.add_kvp();
    tmp1->set_key(kvp.key());
    if (kvs != partitions->end() &&
        get<0>(kvs->second->tx_put(request.tx_id(), kvp.key(), kvp.value()))) {
      tmp1->set_value("OK");
    } else {
      response.set_message("ERROR");
      response.set_success(false);
      tmp1->set_value("ERROR");
    }
  }
}
auto TXManager::HandleDeleteParticipant(const cloud::CloudMessage& request,
                                        cloud::CloudMessage& response) -> void {
  // TODO(you)
  std::unique_lock<std::mutex> lck(mtx);
  std::cout << "TXManager::HandleDeleteParticipant\n";
  response.set_message("OK");
  response.set_operation(cloud::CloudMessage_Operation_DELETE);
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  for (const auto& kvp : request.kvp()) {
    auto kvs = partitions->find(routing->get_partition(kvp.key()));
    auto* tmp1 = response.add_kvp();
    tmp1->set_key(kvp.key());
    if (kvs != partitions->end() &&
        get<0>(kvs->second->tx_del(request.tx_id(), kvp.key()))) {
      tmp1->set_value("OK");
    } else {
      tmp1->set_value("ERROR");
    }
  }
}

}  // namespace cloudlab
