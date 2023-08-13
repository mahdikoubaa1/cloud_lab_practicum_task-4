#include "cloudlab/network/connection.hh"

#include "cloud.pb.h"

#include "cloudlab/argh.hh"
#include "cloudlab/parser.hh"
#include <fmt/core.h>

using namespace cloudlab;

auto main(int argc, char *argv[]) -> int {
  argh::parser cmdl({"-a", "--api"});
  cmdl.parse(argc, argv);

  std::string api_address;
  cmdl({"-a", "--api"}, "127.0.0.1:41000") >> api_address;

  Parser parser{};
  cloud::CloudMessage msg = parser.Parse(cmdl);
  if (parser.IsError()) {
    fmt::print("Usage: {} <operation> <args>\n", cmdl.pos_args().at(0));
    return 1;
  }

  Connection con{api_address};

  // send request
  con.send(msg);

  // receive reply
  con.receive(msg);

  switch (msg.operation()) {
    case cloud::CloudMessage_Operation_PUT:
    case cloud::CloudMessage_Operation_GET:
    case cloud::CloudMessage_Operation_DELETE:
    case cloud::CloudMessage_Operation_RAFT_DIRECT_GET:
      if (!msg.success()) {
        fmt::print("{}\n", msg.message());
      } else {
        for (const auto &kvp : msg.kvp()) {
          fmt::print("Key:\t{}\nValue:\t{}\n", kvp.key(), kvp.value());
        }
      }
      break;
    case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE:
      if (!msg.success()) {
        fmt::print("{}\n", msg.message());
      } else {
        for (const auto &kvp : msg.kvp()) {
          fmt::print("Dropped:\t{}\n", kvp.value());
        }
      }
      break;
    case cloud::CloudMessage_Operation_TX_CLT_BEGIN:
    case cloud::CloudMessage_Operation_TX_CLT_COMMIT:
    case cloud::CloudMessage_Operation_TX_CLT_ABORT:
    case cloud::CloudMessage_Operation_TX_CLT_GET:
    case cloud::CloudMessage_Operation_TX_CLT_PUT:
    case cloud::CloudMessage_Operation_TX_CLT_DELETE:
      assert(msg.type() == cloud::CloudMessage_Type_RESPONSE);
      fmt::print("Msg:\t{}\n", msg.message());
      for (const auto &kvp : msg.kvp())
        fmt::print("Key:\t{}\nValue:\t{}\n", kvp.key(), kvp.value());
      break;
    default:
      fmt::print("{}\n", msg.message());
      break;
  }
}
