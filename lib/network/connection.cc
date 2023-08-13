#include "cloudlab/network/connection.hh"
#include "cloudlab/network/address.hh"

#include "fmt/core.h"

#include "cloud.pb.h"
#include <event2/buffer.h>
#include <event2/bufferevent.h>

#include <netdb.h>
#include <unistd.h>

namespace cloudlab {

Connection::Connection(const SocketAddress& address) {
  addrinfo hints{}, *req = nullptr;
  memset(&hints, 0, sizeof(addrinfo));
  // std::cout << "in Connection::Connection" << std::endl;
  if (address.is_ipv4()) {
    hints.ai_family = AF_INET;
    hints.ai_addrlen = sizeof(struct sockaddr_in);
  } else {
    hints.ai_family = AF_INET6;
    hints.ai_addrlen = sizeof(struct sockaddr_in6);
  }

  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = IPPROTO_TCP;

  if (getaddrinfo(address.get_ip_address().c_str(),
                  std::to_string(address.get_port()).c_str(), &hints,
                  &req) != 0) {
    throw std::runtime_error("getaddrinfo() failed");
  }

  fd = socket(req->ai_family, req->ai_socktype, req->ai_protocol);
  if (fd == -1) {
    throw std::runtime_error("socket() failed");
  }

  // allow kernel to rebind address even when in TIME_WAIT state
  int yes = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
    throw std::runtime_error("setsockopt() failed");
  }
  // std::cout << "before connect" << std::endl;
  if (connect(fd, req->ai_addr, req->ai_addrlen) == -1) {
    // throw std::runtime_error("perform_connect() failed");
    connect_failed = true;
  }
}

Connection::Connection(const std::string& address)
    : Connection(SocketAddress{address}) {
}

Connection::~Connection() {
  close(fd);
}

auto Connection::receive(cloud::CloudMessage& msg) const -> bool {
  uint32_t size{}, read_bytes{};

  if (bev) {
    auto* input = bufferevent_get_input(static_cast<struct bufferevent*>(bev));
    read_bytes = evbuffer_remove(input, &size, 4);
  } else {
    read_bytes = read(fd, &size, 4);
  }

  if (read_bytes < 4) {
    if (read_bytes == 0) {
      // connection closed by other side -> we should close our connection as
      // well ... for now we just return here
      return false;
    }

    throw std::runtime_error(
        fmt::format("Received an incomplete message of {} bytes", read_bytes));
  }

  // convert size to host byte order
  size = ntohl(size);

  if (size > max_message_size) {
    throw std::runtime_error(
        "Connection received a message that exceeds the maximum message size");
  }

  auto buf = std::make_unique<uint8_t[]>(size);

  // read rest of the message
  if (bev) {
    auto* input = bufferevent_get_input(static_cast<struct bufferevent*>(bev));
    read_bytes = evbuffer_remove(input, buf.get(), size);
  } else {
    read_bytes = read(fd, buf.get(), size);
  }

  msg.ParseFromArray(buf.get(), size);

  return (read_bytes == size);
}

auto Connection::send(const cloud::CloudMessage& msg) const -> bool {
  // std::cout << "in Connection::send" << std::endl;
  uint32_t size = msg.ByteSizeLong();
  auto buffer = std::make_unique<uint8_t[]>(size + 4);

  // set first four bytes to size of message (in network byte order)
  uint32_t size_nb = htonl(size);
  memcpy(buffer.get(), &size_nb, 4);

  // serialize message
  msg.SerializeToArray(buffer.get() + 4, size);

  bool success{};
  // std::cout << "before if" << std::endl;
  // write everything out
  if (bev) {
    // std::cout << "inside if" << std::endl;
    auto fd = bufferevent_getfd(static_cast<struct bufferevent*>(bev));
    success = write(fd, buffer.get(), size + 4) == size + 4;

    /*
    success = bufferevent_write(static_cast<struct bufferevent*>(bev),
                                buffer.get(), size + 4) == 0;
    */
  } else {
    success = write(fd, buffer.get(), size + 4) == size + 4;
  }

  return success;
}

}  // namespace cloudlab
