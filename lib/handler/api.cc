#include "cloud.pb.h"

#include "cloudlab/handler/api.hh"

#include "fmt/core.h"

namespace cloudlab {

void APIHandler::handle_connection(Connection& con) {
  cloud::CloudMessage request{}, response{};

  if (!con.receive(request)) {
    return;
  }

  if (request.type() != cloud::CloudMessage_Type_REQUEST) {
    throw std::runtime_error("expected a request");
  }

  auto backend_address = routing->get_backend_address();

  Connection backend{backend_address};

  switch (request.operation()) {
    case cloud::CloudMessage_Operation_PUT:
    case cloud::CloudMessage_Operation_GET:
    case cloud::CloudMessage_Operation_DELETE:
    case cloud::CloudMessage_Operation_JOIN_CLUSTER:
    case cloud::CloudMessage_Operation_RAFT_GET_LEADER:
    case cloud::CloudMessage_Operation_RAFT_DIRECT_GET:
    case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE:
    case cloud::CloudMessage_Operation_TX_CLT_BEGIN:
    case cloud::CloudMessage_Operation_TX_CLT_COMMIT:
    case cloud::CloudMessage_Operation_TX_CLT_ABORT:
    case cloud::CloudMessage_Operation_TX_CLT_GET:
    case cloud::CloudMessage_Operation_TX_CLT_PUT:
    case cloud::CloudMessage_Operation_TX_CLT_DELETE:
    case cloud::CloudMessage_Operation_TX_BEGIN:
    case cloud::CloudMessage_Operation_TX_COMMIT:
    case cloud::CloudMessage_Operation_TX_ABORT:
    case cloud::CloudMessage_Operation_TX_GET:
    case cloud::CloudMessage_Operation_TX_PUT:
    case cloud::CloudMessage_Operation_TX_DELETE: {
      backend.send(request);
      backend.receive(response);
      break;
    }
    default:
      response.set_type(cloud::CloudMessage_Type_RESPONSE);
      response.set_operation(request.operation());
      response.set_success(false);
      response.set_message("Operation not supported");
      break;
  }

  con.send(response);
}

}  // namespace cloudlab
