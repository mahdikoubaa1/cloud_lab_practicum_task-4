#ifndef CLOUDLAB_ADDRESS_HH
#define CLOUDLAB_ADDRESS_HH

#include <array>
#include <string>

namespace cloudlab {

enum class IPAddressType {
  V4,
  V6
};

/**
 * Representation of an IPv4 / IPv6 address plus port.
 */
class SocketAddress {
 public:
  explicit SocketAddress(const std::string& address);

  friend bool operator==(SocketAddress const& lhs, SocketAddress const& rhs) {
    return lhs.type == rhs.type && lhs.port == rhs.port &&
           lhs.ip_address == rhs.ip_address;
  }

  [[nodiscard]] auto is_ipv4() const -> bool {
    return type == IPAddressType::V4;
  }

  [[nodiscard]] auto get_ip_address() const -> std::string const& {
    return ip_address;
  }

  [[nodiscard]] auto get_port() const -> uint16_t {
    return port;
  }

  auto string() const -> std::string;

 private:
  IPAddressType type;
  std::string ip_address;
  uint16_t port;
};

}  // namespace cloudlab

namespace std {

template <>
struct hash<cloudlab::SocketAddress> {
  std::size_t operator()(const cloudlab::SocketAddress& k) const {
    using std::hash;
    using std::string;

    return hash<string>()(k.string());
  }
};

}  // namespace std

#endif  // CLOUDLAB_ADDRESS_HH
