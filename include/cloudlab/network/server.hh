#ifndef CLOUDLAB_SERVER_HH
#define CLOUDLAB_SERVER_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/spmc.hh"

#include <array>
#include <thread>
#include <unistd.h>

namespace cloudlab {

const auto num_workers = 4;

/**
 * A (TCP) network server class.
 */
class Server {
 public:
  Server(std::string address, ServerHandler& handler)
      : address{std::move(address)}, handler{handler} {
  }

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  Server(Server&&) = delete;
  Server& operator=(Server&&) = delete;

  ~Server() {
    for (auto& worker : workers) {
      worker.join();
    }
  }

  auto run() -> std::thread;

 private:
  static auto server(const std::string& address, SPMCQueue<void*>& bev_queue)
      -> void;

  static auto worker(ServerHandler& handler, SPMCQueue<void*>& bev_queue)
      -> void;

  const std::string address;

  std::array<std::thread, num_workers> workers;
  SPMCQueue<void*> bev_queue{};

  ServerHandler& handler;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_SERVER_HH
