#ifndef CLOUDLAB_CONNECTION_HH
#define CLOUDLAB_CONNECTION_HH

#include "cloudlab/network/address.hh"
#include <memory>

namespace cloud {
class CloudMessage;
}

namespace cloudlab {

const auto max_message_size = 4096;

/**
 * Representation of a (TCP) network connection.
 */
class Connection {
 public:
  explicit Connection(const SocketAddress& address);

  explicit Connection(const std::string& address);

  //   explicit Connection(int fd) : fd{fd} {};

  explicit Connection(void* bev) : bev{bev} {};

  ~Connection();

  auto receive(cloud::CloudMessage& msg) const -> bool;

  auto send(const cloud::CloudMessage& msg) const -> bool;

  bool connect_failed{false};

 private:
  int fd{-1};
  void* bev{nullptr};
};
using ConnectionUPtr = std::unique_ptr<Connection>;
using ConnectionSPtr = std::shared_ptr<Connection>;

}  // namespace cloudlab

#endif  // CLOUDLAB_CONNECTION_HH
