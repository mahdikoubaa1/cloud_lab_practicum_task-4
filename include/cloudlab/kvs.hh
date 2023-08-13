#ifndef CLOUDLAB_KVS_HH
#define CLOUDLAB_KVS_HH

#include <filesystem>
#include <map>
#include <memory>
#include <shared_mutex>
#include <tuple>
#include <vector>

namespace rocksdb {
class DB;
class TransactionDB;
class Transaction;
}  // namespace rocksdb

namespace cloudlab {

/**
 * The key-value store. We use rocksdb for the actual key-value operations.
 */
class KVS {
 public:
  using TransactionPtr = rocksdb::Transaction*;
  KVS(const std::string& _path);
  ~KVS();

  auto get(const std::string& key, std::string& result) -> bool;

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer)
      -> bool;

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool;

  auto clear() -> bool;

  auto tx_begin(const std::string& txId) -> std::tuple<bool, std::string>;
  auto tx_commit(const std::string& txId) -> std::tuple<bool, std::string>;
  auto tx_abort(const std::string& txId) -> std::tuple<bool, std::string>;
  auto tx_get(const std::string& txId, const std::string& key,
              std::string& result) -> std::tuple<bool, std::string>;
  auto tx_put(const std::string& txId, const std::string& key,
              const std::string& value) -> std::tuple<bool, std::string>;
  auto tx_del(const std::string& txId, const std::string& key)
      -> std::tuple<bool, std::string>;

 private:
  std::filesystem::path path;
  std::unique_ptr<rocksdb::TransactionDB> db{};
  std::map<std::string, TransactionPtr> transactions;

  // we use a readers-writer lock s.t. multiple threads may read at the same
  // time while only one thread may modify data in the KVS
  std::shared_timed_mutex mtx{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_KVS_HH
