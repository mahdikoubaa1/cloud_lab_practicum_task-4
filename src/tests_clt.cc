#include "cloud.pb.h"
#include "cloudlab/argh.hh"
#include "cloudlab/network/connection.hh"

#include <algorithm>
#include <atomic>
#include <numeric>
#include <string>
#include <thread>
#include <vector>
#include <fmt/core.h>

using namespace cloudlab;

struct MessageHelper {
  using PairsVec = std::vector<std::pair<std::string, std::string>>;
  MessageHelper() = default;
  MessageHelper(const cloud::CloudMessage& msg)
      : type{msg.type()},
        operation{msg.operation()},
        success{msg.success()},
        message{msg.message()},
        address{msg.address().address()},
        txId{msg.tx_id()} {
    AddPairsFrom(msg);
  }
  auto WriteTo(cloud::CloudMessage& msg) const -> void {
    msg.set_type(type);
    msg.set_operation(operation);
    msg.set_success(success);
    msg.set_message(message);
    msg.mutable_address()->set_address(address);
    msg.set_tx_id(txId);
    for (const auto& [key, value] : pairs) {
      auto* kvp = msg.add_kvp();
      kvp->set_key(key);
      kvp->set_value(value);
    }
  }
  auto GetMessage() const -> cloud::CloudMessage {
    cloud::CloudMessage msg{};
    WriteTo(msg);
    return msg;
  }
  auto WriteErrors(const bool& _success, const std::string& _message,
                   const PairsVec& _pairs) -> void {
    if (success) success = _success;
    if (message == "OK") message = _message;
    for (const auto& p : _pairs) {
      std::replace_if(
          pairs.begin(), pairs.end(),
          [&p](const auto& old) { return old.first == p.first; }, p);
    }
  }
  auto AddPairsFrom(const PairsVec& _pairs) -> void {
    for (const auto& [key, value] : _pairs) pairs.emplace_back(key, value);
  }
  auto AddPairsFrom(const cloud::CloudMessage& msg) -> void {
    PairsVec vec{};
    for (const auto& kvp : msg.kvp()) vec.emplace_back(kvp.key(), kvp.value());
    AddPairsFrom(vec);
  }
  auto AddPairsFrom(const MessageHelper& other) -> void {
    PairsVec vec{};
    for (const auto& [key, value] : other.pairs) vec.emplace_back(key, value);
    AddPairsFrom(vec);
  }
  auto AddPairsFrom(const std::vector<std::string>& keyVec) -> void {
    PairsVec vec{};
    for (const auto& key : keyVec) vec.emplace_back(key, "");
    AddPairsFrom(vec);
  }
  auto AddPair(const std::string& key, const std::string& value) -> void {
    pairs.emplace_back(key, value);
  }
  auto PrintPairs() const -> void {
    for (const auto& [key, value] : pairs)
      fmt::print("key:\t{}\tvalue:\t{}\n", key, value);
  }

  cloud::CloudMessage_Type type{cloud::CloudMessage_Type_RESPONSE};
  cloud::CloudMessage_Operation operation{cloud::CloudMessage_Operation_TX_GET};
  bool success{true};
  std::string message{"OK"};
  std::string address{};
  std::string txId{};
  PairsVec pairs{};
};

bool IsNumber(const std::string& s) {
  return !s.empty() && std::find_if(s.begin(), s.end(), [](unsigned char c) {
                         return !std::isdigit(c);
                       }) == s.end();
}

auto CreateJoin(const std::string& clusterAddress) -> MessageHelper {
  MessageHelper h{};
  h.type = cloud::CloudMessage_Type_REQUEST;
  h.operation = cloud::CloudMessage_Operation_JOIN_CLUSTER;
  h.address = clusterAddress;
  return h;
}

auto CreateBegin(const std::string& name, const int& key) -> MessageHelper {
  MessageHelper h{};
  h.type = cloud::CloudMessage_Type_REQUEST;
  h.operation = cloud::CloudMessage_Operation_TX_CLT_BEGIN;
  h.txId = name;
  h.pairs.emplace_back(std::to_string(key), "");
  return h;
}
auto CreateCommit(const std::string& name) -> MessageHelper {
  MessageHelper h{};
  h.type = cloud::CloudMessage_Type_REQUEST;
  h.operation = cloud::CloudMessage_Operation_TX_CLT_COMMIT;
  h.txId = name;
  return h;
}
auto CreateGet(const std::string& name, const int& key) -> MessageHelper {
  MessageHelper h{};
  h.type = cloud::CloudMessage_Type_REQUEST;
  h.operation = cloud::CloudMessage_Operation_TX_CLT_GET;
  h.txId = name;
  h.pairs.emplace_back(std::to_string(key), "");
  return h;
}
auto CreatePut(const std::string& name, const int& key, const int& val)
    -> MessageHelper {
  MessageHelper h{};
  h.type = cloud::CloudMessage_Type_REQUEST;
  h.operation = cloud::CloudMessage_Operation_TX_CLT_PUT;
  h.txId = name;
  h.pairs.emplace_back(std::to_string(key), std::to_string(val));
  return h;
}

// const std::string& LeaderApiAddr = "127.0.0.1:40000";
const std::string& CLUSTER_ADDRESS = "127.0.0.1:41000";
// const std::string& PeerApiAddr = "127.0.0.1:42000";
// const std::string& PEER_ADDRESS = "127.0.0.1:43000";
const std::vector<std::string>& PEERS = {"127.0.0.1:43000"};

const int& COUNTERS_NUMBER = 10;
const int& THREADS_NUMBER_COUNTERS_TEST = 4;
const int& INCS_PER_THREAD_PER_COUNTER = 4;
const int& TOTAL_INCS_PER_COUNTER =
    THREADS_NUMBER_COUNTERS_TEST * INCS_PER_THREAD_PER_COUNTER;

std::vector<std::atomic<int>> FailedTx(COUNTERS_NUMBER);
std::atomic<uint64_t> cid{0};

auto IncrementCounters(const int& threadId) -> void {
  Connection con{CLUSTER_ADDRESS};
  for (auto key = 0; key < COUNTERS_NUMBER; ++key)
    for (auto i = 0; i < INCS_PER_THREAD_PER_COUNTER; ++i) {
      const std::string& txId = "inc_" + std::to_string(cid.fetch_add(1));
      {
        const MessageHelper& req = CreateBegin(txId, key);
        // fmt::print("req begin\n");
        // req.PrintPairs();

        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);

        // MessageHelper rep(_rep);
        // fmt::print("rep begin {}\n", rep.GetMessage().message());
        // req.PrintPairs();
      }
      int counterValue{};
      {
        const MessageHelper& req = CreateGet(txId, key);
        // fmt::print("req get\n");
        // req.PrintPairs();

        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);

        MessageHelper rep(_rep);
        // fmt::print("rep get {}\n", rep.GetMessage().message());
        // req.PrintPairs();

        if (rep.pairs.empty()) {
          fmt::print("Test Failed\n");
          fmt::print("Get request did not return a Key-Value Pair at {}:{}\n",
                     __FILE__, __LINE__);
          exit(EXIT_FAILURE);
        }
        const auto& [getKey, getVal] = rep.pairs.at(0);
        if (!IsNumber(getKey)) {
          fmt::print("Test Failed\n");
          fmt::print("Key {} is not number at {}:{}\n", getKey, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        if (!IsNumber(getVal)) {
          fmt::print("Test Failed\n");
          fmt::print("Value {} is not number at {}:{}\n", getVal, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        assert(key == std::stoi(getKey));
        counterValue = std::stoi(getVal);
        // counterValue = 0;
      }
      {
        const MessageHelper& req = CreatePut(txId, key, counterValue + 1);
        // fmt::print("req put\n");
        // req.PrintPairs();

        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);

        // MessageHelper rep(_rep);
        // fmt::print("rep put {}\n", rep.GetMessage().message());
        // req.PrintPairs();
      }
      {
        const MessageHelper& req = CreateCommit(txId);
        // fmt::print("req commit\n");
        // req.PrintPairs();

        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);

        MessageHelper rep(_rep);
        // fmt::print("rep commit {}\n", rep.GetMessage().message());
        // req.PrintPairs();

        if (rep.GetMessage().message() != "OK") {
          fmt::print("Commit for key {} Failed\n", key);
          FailedTx.at(key) += 1;
        }
      }
    }
}

auto CounterIncrementTest() -> void {
  {  // Join Cluster and Init Keys
    Connection con{CLUSTER_ADDRESS};
    for (const auto& peerAddr : PEERS) {
      const MessageHelper& req = CreateJoin(peerAddr);
      con.send(req.GetMessage());
      cloud::CloudMessage _rep{};
      con.receive(_rep);
      MessageHelper rep(_rep);
      if (rep.GetMessage().message() != "OK") {
        fmt::print("Test Failed\n");
        fmt::print("{} Failed to Join Cluster {} at {}:{}\n", peerAddr,
                   CLUSTER_ADDRESS, __FILE__, __LINE__);
        exit(EXIT_FAILURE);
      }
      fmt::print("{} Joined Cluster {}\n", peerAddr, CLUSTER_ADDRESS);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    fmt::print("Initializing Counters\n");
    std::vector<int> keys(COUNTERS_NUMBER);
    std::iota(std::begin(keys), std::end(keys), 0);
    for (const auto& key : keys) {
      const std::string& txId = "init_" + std::to_string(key);
      {
        const MessageHelper& req = CreateBegin(txId, key);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
      {
        const MessageHelper& req = CreatePut(txId, key, 0);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
      {
        const MessageHelper& req = CreateCommit(txId);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // Spawn threads to increment counters
  fmt::print("Threads Incrementing Counters\n");
  std::vector<std::thread> threads{};
  for (auto i = 0; i < THREADS_NUMBER_COUNTERS_TEST; ++i) {
    threads.emplace_back([i]() { IncrementCounters(i); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  threads.clear();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // Get Final Counter Values
  fmt::print("Getting Results from KVS\n");
  std::vector<std::pair<int, int>> results(COUNTERS_NUMBER);
  std::generate(std::begin(results), std::end(results),
                []() -> std::pair<int, int> {
                  static int n = -1;
                  ++n;
                  return {n, 0};
                });
  {
    Connection con{CLUSTER_ADDRESS};
    for (auto& [key, val] : results) {
      const std::string& txId = "result_" + std::to_string(key);
      {
        const MessageHelper& req = CreateBegin(txId, key);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
      {
        const MessageHelper& req = CreateGet(txId, key);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);

        if (rep.pairs.empty()) {
          fmt::print("Test Failed\n");
          fmt::print("Get request did not return a Key-Value Pair at {}:{}\n",
                     __FILE__, __LINE__);
          exit(EXIT_FAILURE);
        }
        const auto& [getKey, getVal] = rep.pairs.at(0);
        if (!IsNumber(getKey)) {
          fmt::print("Test Failed\n");
          fmt::print("Key {} is not number at {}:{}\n", getKey, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        if (!IsNumber(getVal)) {
          fmt::print("Test Failed\n");
          fmt::print("Value {} is not number at {}:{}\n", getVal, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        assert(key == std::stoi(getKey));
        val = std::stoi(getVal);
      }
      {
        const MessageHelper& req = CreateCommit(txId);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // Compare Results with expected final counter values
  fmt::print("Checking Results\n");
  std::sort(std::begin(results), std::end(results),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  fmt::print("TOTAL_INCS_PER_COUNTER {}\n", TOTAL_INCS_PER_COUNTER);
  fmt::print("FailedTx\n");
  for (auto i = 0u; i < std::size(FailedTx); ++i)
    fmt::print("{}\t-\t{}\n", i, FailedTx.at(i));
  fmt::print("Results\n");
  for (const auto& [key, val] : results) fmt::print("{}\t-\t{}\n", key, val);

  for (const auto& [key, val] : results) {
    if (val != (TOTAL_INCS_PER_COUNTER - FailedTx.at(key))) {
      fmt::print("Test Failed\n");
      fmt::print("Key Mismatch key:{} val:{} at {}:{}\n", key, val, __FILE__,
                 __LINE__);
      exit(EXIT_FAILURE);
    }
  }

  fmt::print("Test Passed\n");
}

const int& THREADS_NUMBER_NO_SHARED_TEST = 4;
const int& KEYS_NUMBER = 1000;
const int& FINAL_VALUE = 10;
const int& THREAD_SLICE = KEYS_NUMBER / THREADS_NUMBER_NO_SHARED_TEST;

auto WriteKeys(const int& threadId) -> void {
  Connection con{CLUSTER_ADDRESS};
  const int& startKey = threadId * THREAD_SLICE;
  const int& endKey = startKey + THREAD_SLICE;
  for (auto key = startKey; key < endKey; ++key) {
    const std::string& txId = "put_" + std::to_string(key);
    {
      const MessageHelper& req = CreateBegin(txId, key);
      //   fmt::print("req begin\n");
      //   req.PrintPairs();

      con.send(req.GetMessage());
      cloud::CloudMessage _rep{};
      con.receive(_rep);

      //   MessageHelper rep(_rep);
      //   fmt::print("rep begin {}\n", rep.GetMessage().message());
      //   req.PrintPairs();
    }
    for (auto val = 1; val <= FINAL_VALUE; ++val) {
      const MessageHelper& req = CreatePut(txId, key, val);
      //   fmt::print("req put\n");
      //   req.PrintPairs();

      con.send(req.GetMessage());
      cloud::CloudMessage _rep{};
      con.receive(_rep);

      //   MessageHelper rep(_rep);
      //   fmt::print("rep put {}\n", rep.GetMessage().message());
      //   req.PrintPairs();
    }
    {
      const MessageHelper& req = CreateCommit(txId);
      //   fmt::print("req commit\n");
      //   req.PrintPairs();

      con.send(req.GetMessage());
      cloud::CloudMessage _rep{};
      con.receive(_rep);

      //   MessageHelper rep(_rep);
      //   fmt::print("rep commit {}\n", rep.GetMessage().message());
      //   req.PrintPairs();
    }
  }
}

auto NoSharedKeysTest() -> void {
  {  // Join Cluster and Init Keys
    Connection con{CLUSTER_ADDRESS};
    for (const auto& peerAddr : PEERS) {
      const MessageHelper& req = CreateJoin(peerAddr);
      con.send(req.GetMessage());
      cloud::CloudMessage _rep{};
      con.receive(_rep);
      MessageHelper rep(_rep);
      if (rep.GetMessage().message() != "OK") {
        fmt::print("Test Failed\n");
        fmt::print("{} Failed to Join Cluster {} at {}:{}\n", peerAddr,
                   CLUSTER_ADDRESS, __FILE__, __LINE__);
        exit(EXIT_FAILURE);
      }
      fmt::print("{} Joined Cluster {}\n", peerAddr, CLUSTER_ADDRESS);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }

  // Spawn threads to write key slices
  fmt::print("Threads Writing on their key slices\n");
  std::vector<std::thread> threads{};
  for (auto i = 0u; i < THREADS_NUMBER_NO_SHARED_TEST; ++i) {
    threads.emplace_back([i]() { WriteKeys(i); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  threads.clear();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // Get Final Counter Values
  fmt::print("Getting Results from KVS\n");
  std::vector<std::pair<int, int>> results(KEYS_NUMBER);
  std::generate(std::begin(results), std::end(results),
                []() -> std::pair<int, int> {
                  static int n = -1;
                  ++n;
                  return {n, 0};
                });
  {
    Connection con{CLUSTER_ADDRESS};
    for (auto& [key, val] : results) {
      const std::string& txId = "result_" + std::to_string(key);
      {
        const MessageHelper& req = CreateBegin(txId, key);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
      {
        const MessageHelper& req = CreateGet(txId, key);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);

        if (rep.pairs.empty()) {
          fmt::print("Test Failed\n");
          fmt::print("Get request did not return a Key-Value Pair at {}:{}\n",
                     __FILE__, __LINE__);
          exit(EXIT_FAILURE);
        }
        const auto& [getKey, getVal] = rep.pairs.at(0);
        if (!IsNumber(getKey)) {
          fmt::print("Test Failed\n");
          fmt::print("Key {} is not number at {}:{}\n", getKey, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        if (!IsNumber(getVal)) {
          fmt::print("Test Failed\n");
          fmt::print("Value {} is not number at {}:{}\n", getVal, __FILE__,
                     __LINE__);
          exit(EXIT_FAILURE);
        }
        assert(key == std::stoi(getKey));
        val = std::stoi(getVal);
      }
      {
        const MessageHelper& req = CreateCommit(txId);
        con.send(req.GetMessage());
        cloud::CloudMessage _rep{};
        con.receive(_rep);
        MessageHelper rep(_rep);
      }
    }
    // Compare Results with expected final counter values
    fmt::print("Checking Results\n");
    std::sort(std::begin(results), std::end(results),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    fmt::print("FINAL_VALUE {}\n", FINAL_VALUE);
    fmt::print("Results\n");
    for (const auto& [key, val] : results) fmt::print("{}\t-\t{}\n", key, val);

    for (const auto& [key, val] : results) {
      if (val != FINAL_VALUE) {
        fmt::print("Test Failed\n");
        fmt::print("Key Mismatch key:{} val:{} at {}:{}\n", key, val, __FILE__,
                   __LINE__);
        exit(EXIT_FAILURE);
      }
    }

    fmt::print("Test Passed\n");
  }
}
auto main(int argc, char* argv[]) -> int {
  //   CounterIncrementTest();
  //   NoSharedKeysTest();
  //   return 0;

  if (argc != 2) {
    fmt::print("Usage: tests_clt 1 OR tests_clt 2\n");
    exit(EXIT_FAILURE);
  }
  const std::string selection = std::string(argv[1]);
  if (selection == std::string("1")) {
    CounterIncrementTest();
  } else if (selection == std::string("2")) {
    NoSharedKeysTest();
  } else {
    fmt::print("Usage: tests_clt 1 OR tests_clt 2\n");
    exit(EXIT_FAILURE);
  }
  return 0;
}