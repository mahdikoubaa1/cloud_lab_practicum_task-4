#ifndef CLOUDLAB_API_HH
#define CLOUDLAB_API_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/network/routing.hh"

namespace cloudlab {

/**
 * Handler for API requests.
 *
 * In cloudlab we will simply forward all requests to the P2P port ... in
 * the "real" world different authentication protocols and messages may be used
 * on those ports.
 */
class APIHandler : public ServerHandler {
 public:
  explicit APIHandler(Routing* _routing) : routing{_routing} {
  }

  auto handle_connection(Connection& con) -> void override;

 private:
  Routing* routing;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_API_HH
