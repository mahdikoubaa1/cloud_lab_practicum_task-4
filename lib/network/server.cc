#include "cloudlab/network/server.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/spmc.hh"

#include <cstring>
#include <iostream>
#include <thread>
#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <netdb.h>
#include <sys/socket.h>

namespace cloudlab {

auto Server::run() -> std::thread {
  // spawn workers
  for (auto i = 0; i < num_workers; i++) {
    workers[i] = std::thread(worker, std::ref(handler), std::ref(bev_queue));
  }

  // spawn server thread that handles incoming connections
  auto thread = std::thread(server, address, std::ref(bev_queue));

  // return thread handle
  return thread;
}

auto Server::server(const std::string &address, SPMCQueue<void *> &bev_queue)
    -> void {
  auto socket_address = SocketAddress{address};

  addrinfo hints{}, *req = nullptr;
  memset(&hints, 0, sizeof(addrinfo));

  if (socket_address.is_ipv4()) {
    hints.ai_family = AF_INET;
    hints.ai_addrlen = sizeof(struct sockaddr_in);
  } else {
    hints.ai_family = AF_INET6;
    hints.ai_addrlen = sizeof(struct sockaddr_in6);
  }

  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = IPPROTO_TCP;

  if (getaddrinfo(socket_address.get_ip_address().c_str(),
                  std::to_string(socket_address.get_port()).c_str(), &hints,
                  &req) != 0) {
    throw std::runtime_error{"getaddrinfo() failed"};
  }

  struct event_base *base{};
  struct evconnlistener *listener{};
  struct event *signal_event{};

  base = event_base_new();
  if (!base) {
    throw std::runtime_error{"could not initialize libevent\n"};
  }

  auto base_and_bev_queue = std::pair{base, &bev_queue};

  auto listen_handler = [](struct evconnlistener *, evutil_socket_t fd,
                           struct sockaddr *, int, void *user_data) {
    auto read_handler = [](struct bufferevent *bev, void *user_data) {
      auto *bev_queue = static_cast<SPMCQueue<void *> *>(user_data);

      // disable read event handler before passing event to worker thread s.t.
      // no more events are triggered before and during connection handling
      bufferevent_disable(bev, EV_READ);
      bev_queue->produce(bev);
    };

    auto event_handler = [](struct bufferevent *bev, short events, void *) {
      if (events & BEV_EVENT_EOF) {
        // fmt::print("connection closed.\n");
      } else if (events & BEV_EVENT_ERROR) {
        // fmt::print("got an error on the connection: {}\n", strerror(errno));
      }

      bufferevent_free(bev);
    };

    auto *base_and_bev_queue =
        static_cast<std::pair<struct event_base *, SPMCQueue<void *> *> *>(
            user_data);

    auto *base = base_and_bev_queue->first;
    auto *bev_queue = base_and_bev_queue->second;

    auto *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!bev) {
      throw std::runtime_error{"could not construct bufferevent"};
    }

    bufferevent_setcb(bev, read_handler, nullptr, event_handler, bev_queue);
    bufferevent_enable(bev, EV_READ);
  };

  listener = evconnlistener_new_bind(
      base, listen_handler, &base_and_bev_queue,
      LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE | LEV_OPT_THREADSAFE, -1,
      req->ai_addr, req->ai_addrlen);

  if (!listener) {
    throw std::runtime_error{"could not create a listener\n"};
  }

  event_base_dispatch(base);

  evconnlistener_free(listener);
  event_base_free(base);
}

auto Server::worker(ServerHandler &handler, SPMCQueue<void *> &bev_queue)
    -> void {
  while (true) {
    auto *bev = bev_queue.consume();

    // exit worker thread on nullptr
    if (!bev) return;
    Connection con{static_cast<void *>(bev)};
    handler.handle_connection(con);

    // re-enable event handler after connection handling
    bufferevent_enable(static_cast<struct bufferevent *>(bev), EV_READ);
  }
}

}  // namespace cloudlab
