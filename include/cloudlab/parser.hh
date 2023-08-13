#ifndef CLOUDLAB_PARSER_HH
#define CLOUDLAB_PARSER_HH

#include "cloud.pb.h"
#include "cloudlab/argh.hh"

#include <functional>
#include <map>
#include <optional>
#include <string>

namespace cloudlab {
class Parser {
 public:
  using ParseFunc = std::function<cloud::CloudMessage(argh::parser)>;
  using ParseMap = std::map<std::string, ParseFunc>;
  auto IsError() -> bool {
    return isError;
  }
  auto Parse(const argh::parser& cmdl) -> cloud::CloudMessage;
  Parser();

 private:
  auto HandleGet(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandlePut(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleDel(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleDirectGet(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleJoin(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleDropped(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleLeader(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxBegin(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxCommit(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxAbort(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxGet(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxPut(const argh::parser& cmdl) -> cloud::CloudMessage;
  auto HandleTxDel(const argh::parser& cmdl) -> cloud::CloudMessage;

  bool isError{false};
  ParseMap parseMap{};
};
}  // namespace cloudlab

#endif
