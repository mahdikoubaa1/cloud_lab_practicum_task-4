#include "cloudlab/parser.hh"

#include "cloudlab/message/message_helper.hh"

namespace cloudlab {

auto Parser::HandleGet(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 3)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_GET;
  for (auto i = 2; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}
auto Parser::HandlePut(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 4) || ((size % 2) == 1)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_PUT;
  for (auto i = 2; i < size; i += 2)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), cmdl.pos_args().at(i + 1));
  return helper.GetMessage();
}
auto Parser::HandleDel(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 3)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_DELETE;
  for (auto i = 2; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}
auto Parser::HandleDirectGet(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 3)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_RAFT_DIRECT_GET;
  for (auto i = 2; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}
auto Parser::HandleJoin(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size != 3)) {
    isError = true;
    return {};
  }
  const std::string& addr = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_JOIN_CLUSTER;
  helper.address = addr;
  return helper.GetMessage();
}
auto Parser::HandleDropped(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size != 2)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_RAFT_DROPPED_NODE;
  return helper.GetMessage();
}
auto Parser::HandleLeader(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size != 2)) {
    isError = true;
    return {};
  }
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_RAFT_GET_LEADER;
  return helper.GetMessage();
}

auto Parser::HandleTxBegin(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 4)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_BEGIN;
  helper.txId = txId;
  for (auto i = 3; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}
auto Parser::HandleTxCommit(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 3)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_COMMIT;
  helper.txId = txId;
  return helper.GetMessage();
}
auto Parser::HandleTxAbort(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 3)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_ABORT;
  helper.txId = txId;
  return helper.GetMessage();
}
auto Parser::HandleTxGet(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 4)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_GET;
  helper.txId = txId;
  for (auto i = 3; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}
auto Parser::HandleTxPut(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 5) || ((size % 2) == 0)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_PUT;
  helper.txId = txId;
  for (auto i = 3; i < size; i += 2)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), cmdl.pos_args().at(i + 1));
  return helper.GetMessage();
}
auto Parser::HandleTxDel(const argh::parser& cmdl) -> cloud::CloudMessage {
  const auto& size = cmdl.pos_args().size();
  if ((size < 4)) {
    isError = true;
    return {};
  }
  const std::string txId = cmdl.pos_args().at(2);
  MessageHelper helper{};
  helper.type = cloud::CloudMessage_Type_REQUEST;
  helper.operation = cloud::CloudMessage_Operation_TX_CLT_DELETE;
  helper.txId = txId;
  for (auto i = 3; i < size; ++i)
    helper.pairs.emplace_back(cmdl.pos_args().at(i), "");
  return helper.GetMessage();
}

auto Parser::Parse(const argh::parser& cmdl) -> cloud::CloudMessage {
  if (cmdl.pos_args().size() < 2) {
    isError = true;
    return {};
  }
  if (parseMap.contains(cmdl.pos_args().at(1)) == false) {
    isError = true;
    return {};
  }
  return parseMap.at(cmdl.pos_args().at(1))(cmdl);
}

Parser::Parser() {
  parseMap =
      ParseMap{{"get",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleGet(cmdl);
                }},
               {"put",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandlePut(cmdl);
                }},
               {"del",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleDel(cmdl);
                }},
               {"direct_get",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleDirectGet(cmdl);
                }},
               {"join",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleJoin(cmdl);
                }},
               {"leader",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleLeader(cmdl);
                }},
               {"tx_begin",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxBegin(cmdl);
                }},
               {"tx_commit",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxCommit(cmdl);
                }},
               {"tx_abort",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxAbort(cmdl);
                }},
               {"tx_get",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxGet(cmdl);
                }},
               {"tx_put",
                [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxPut(cmdl);
                }},
               {"tx_del", [this](const auto& cmdl) -> cloud::CloudMessage {
                  return HandleTxDel(cmdl);
                }}};
}

}  // namespace cloudlab
