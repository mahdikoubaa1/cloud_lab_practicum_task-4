#include "cloudlab/network/address.hh"
#include <algorithm>

#include "fmt/core.h"

#include <arpa/inet.h>
#include <netinet/in.h>

namespace cloudlab {

SocketAddress::SocketAddress(const std::string& address) {
  union {
    struct sockaddr_in v4;
    struct sockaddr_in6 v6;
  } buffer{};

  // split address string by last colon
  auto cut = address.find_last_of(":\\");

  if (cut != std::string::npos) {
    auto ip_address_str = address.substr(0, cut);
    auto port_str = address.substr(cut + 1);

    // 1. parse port
    try {
      port = static_cast<uint16_t>(std::stoi(port_str));
    } catch (std::invalid_argument&) {
      throw std::invalid_argument(fmt::format(
          "{} could not be parsed as <IP address>:<port>", address));
    }

    // 2. parse IP address

    // remove square brackets in case there are any
    ip_address_str.erase(
        std::remove(ip_address_str.begin(), ip_address_str.end(), '['),
        ip_address_str.end());
    ip_address_str.erase(
        std::remove(ip_address_str.begin(), ip_address_str.end(), ']'),
        ip_address_str.end());

    ip_address = ip_address_str;

    // valid IPv4 address?
    if (inet_pton(AF_INET, ip_address_str.c_str(), &buffer)) {
      type = IPAddressType::V4;
      return;
    }

    // valid IPv6 address?
    if (inet_pton(AF_INET6, ip_address_str.c_str(), &buffer)) {
      type = IPAddressType::V6;
      return;
    }
  }

  throw std::invalid_argument(
      fmt::format("{} could not be parsed as <IP address>:<port>", address));
}

auto SocketAddress::string() const -> std::string {
  const auto* pattern = (type == IPAddressType::V4) ? "{}:{}" : "[{}]:{}";

  return fmt::format("{}:{}", ip_address, port);
}

}  // namespace cloudlab