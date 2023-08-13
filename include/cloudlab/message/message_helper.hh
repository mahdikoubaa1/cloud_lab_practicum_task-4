#ifndef CLOUDLAB_MESSAGE_HELPER_HH
#define CLOUDLAB_MESSAGE_HELPER_HH

#include "cloud.pb.h"

#include <algorithm>
#include <string>
#include <vector>
#include <fmt/core.h>

namespace cloudlab {
struct MessageHelper {
  using PairsVec = std::vector<std::pair<std::string, std::string>>;
  MessageHelper() = default;
  MessageHelper(const cloud::CloudMessage& msg);
  auto WriteTo(cloud::CloudMessage& msg) -> void;
  auto GetMessage() -> cloud::CloudMessage {
    cloud::CloudMessage msg{};
    WriteTo(msg);
    return msg;
  }
  auto WriteErrors(const bool& _success, const std::string& _message,
                   const PairsVec& _pairs) -> void {
    if (success) success = _success;
    if (message == "OK") message = _message;
    for (const auto& p : _pairs) {
      std::replace_if(
          pairs.begin(), pairs.end(),
          [&p](const auto& old) { return old.first == p.first; }, p);
    }
  }
  auto AddPairsFrom(const PairsVec& _pairs) -> void {
    for (const auto& [key, value] : _pairs) pairs.emplace_back(key, value);
  }
  auto AddPairsFrom(const cloud::CloudMessage& msg) -> void {
    PairsVec vec{};
    for (const auto& kvp : msg.kvp()) vec.emplace_back(kvp.key(), kvp.value());
    AddPairsFrom(vec);
  }
  auto AddPairsFrom(const MessageHelper& other) -> void {
    PairsVec vec{};
    for (const auto& [key, value] : other.pairs) vec.emplace_back(key, value);
    AddPairsFrom(vec);
  }
  auto AddPairsFrom(const std::vector<std::string>& keyVec) -> void {
    PairsVec vec{};
    for (const auto& key : keyVec) vec.emplace_back(key, "");
    AddPairsFrom(vec);
  }
  auto AddPair(const std::string& key, const std::string& value) -> void {
    pairs.emplace_back(key, value);
  }
  auto PrintPairs() -> void {
    for (const auto& [key, value] : pairs)
      fmt::print("key:\t{}\tvalue:\t{}\n", key, value);
  }

  cloud::CloudMessage_Type type{cloud::CloudMessage_Type_RESPONSE};
  cloud::CloudMessage_Operation operation{cloud::CloudMessage_Operation_TX_GET};
  bool success{true};
  std::string message{"OK"};
  std::string address{};
  std::string txId{};
  PairsVec pairs{};
};

}  // namespace cloudlab
#endif
