#include "cloudlab/message/message_helper.hh"
namespace cloudlab {
MessageHelper::MessageHelper(const cloud::CloudMessage& msg)
    : type{msg.type()},
      operation{msg.operation()},
      success{msg.success()},
      message{msg.message()},
      address{msg.address().address()},
      txId{msg.tx_id()} {
  AddPairsFrom(msg);
}
auto MessageHelper::WriteTo(cloud::CloudMessage& msg) -> void {
  msg.set_type(type);
  msg.set_operation(operation);
  msg.set_success(success);
  msg.set_message(message);
  msg.mutable_address()->set_address(address);
  msg.set_tx_id(txId);
  for (const auto& [key, value] : pairs) {
    auto* kvp = msg.add_kvp();
    kvp->set_key(key);
    kvp->set_value(value);
  }
}
}  // namespace cloudlab
