#ifndef CLOUDLAB_HANDLER_HH
#define CLOUDLAB_HANDLER_HH

#include "cloudlab/network/connection.hh"

namespace cloudlab {

/**
 * Abstract class that defines a server handler.
 */
class ServerHandler {
 public:
  virtual ~ServerHandler() {
  }

  /**
   * Handler for new and existing connections. To be called whenever a new
   * message arrives.
   *
   * @param con     The connection
   */
  virtual auto handle_connection(Connection& con) -> void = 0;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_HANDLER_HH
