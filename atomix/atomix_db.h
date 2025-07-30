//
//  atomix_db.h
//  YCSB-cpp
//
//  Copyright (c) 2024 YCSB contributors. All rights reserved.
//

#ifndef YCSB_C_ATOMIX_DB_H_
#define YCSB_C_ATOMIX_DB_H_

#include <iostream>
#include <string>
#include <memory>
#include <mutex>
#include <vector>
#include <functional>
#include <future>
#include <thread>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <array>

#include "core/db.h"
#include "utils/properties.h"

// Include generated protobuf files
#include "atomix.pb.h"
#include "atomix.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace ycsbc {

// Object pool for frequently allocated gRPC objects
template<typename T>
class ObjectPool {
public:
    ObjectPool(size_t initial_size = 10) {
        for (size_t i = 0; i < initial_size; ++i) {
            pool_.push(std::make_unique<T>());
        }
    }
    
    std::unique_ptr<T> acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pool_.empty()) {
            return std::make_unique<T>();
        }
        auto obj = std::move(pool_.front());
        pool_.pop();
        return obj;
    }
    
    void release(std::unique_ptr<T> obj) {
        if (!obj) return;
        // Reset the object to clean state
        obj->Clear();
        std::lock_guard<std::mutex> lock(mutex_);
        pool_.push(std::move(obj));
    }
    
private:
    std::queue<std::unique_ptr<T>> pool_;
    std::mutex mutex_;
};

class AtomixDB : public DB {
 public:
  AtomixDB();
  ~AtomixDB();

  void Init();
  void Cleanup();

  DB::Status Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields, std::vector<DB::Field> &result);

  DB::Status Scan(const std::string &table, const std::string &key, int record_count,
                  const std::vector<std::string> *fields, std::vector<std::vector<DB::Field>> &result);

  DB::Status Update(const std::string &table, const std::string &key, std::vector<DB::Field> &values);

  DB::Status Insert(const std::string &table, const std::string &key, std::vector<DB::Field> &values);

  DB::Status Delete(const std::string &table, const std::string &key);

 private:
  // Configuration properties
  static constexpr const char* FRONTEND_ADDRESS_PROPERTY = "atomix.frontend.address";
  static constexpr const char* FRONTEND_ADDRESS_DEFAULT = "127.0.0.1:50057";
  static constexpr const char* NAMESPACE_PROPERTY = "atomix.namespace";
  static constexpr const char* NAMESPACE_DEFAULT = "ycsb";
  static constexpr const char* KEYSPACE_PROPERTY = "atomix.keyspace";
  static constexpr const char* KEYSPACE_DEFAULT = "default";
  static constexpr const char* NUM_KEYS_PROPERTY = "atomix.keyspace.num_keys";
  static constexpr const char* NUM_KEYS_DEFAULT = "1";
  static constexpr const char* CONNECTION_TIMEOUT_PROPERTY = "atomix.connection.timeout";
  static constexpr const char* CONNECTION_TIMEOUT_DEFAULT = "30";
  static constexpr const char* REQUEST_TIMEOUT_PROPERTY = "atomix.request.timeout";
  static constexpr const char* REQUEST_TIMEOUT_DEFAULT = "10";
  static constexpr const char* NUM_CONNECTIONS_PROPERTY = "atomix.num.connections";
  static constexpr const char* NUM_CONNECTIONS_DEFAULT = "10";
  static constexpr const char* USE_READ_MODIFY_WRITE_PROPERTY = "atomix.use.read.modify.write";
  static constexpr const char* USE_READ_MODIFY_WRITE_DEFAULT = "false";
  static constexpr const char* BATCH_SIZE_PROPERTY = "atomix.batch.size";
  static constexpr const char* BATCH_SIZE_DEFAULT = "1";

  // Connection pool for better concurrency
  struct ConnectionPool {
    std::vector<std::shared_ptr<grpc::Channel>> channels;
    std::vector<std::unique_ptr<frontend::Frontend::Stub>> stubs;
    std::atomic<size_t> current_index{0};
    std::mutex mutex;
  };
  
  static std::unique_ptr<ConnectionPool> connection_pool_;
  static std::mutex connection_mutex_;
  static bool connection_initialized_;

  // Object pools for frequently allocated objects
  static std::unique_ptr<ObjectPool<frontend::GetRequest>> get_request_pool_;
  static std::unique_ptr<ObjectPool<frontend::PutRequest>> put_request_pool_;
  static std::unique_ptr<ObjectPool<frontend::StartTransactionRequest>> start_txn_request_pool_;
  static std::unique_ptr<ObjectPool<frontend::CommitRequest>> commit_request_pool_;
  static std::unique_ptr<ObjectPool<frontend::ReadModifyWriteRequest>> read_modify_write_request_pool_;

  // Configuration
  std::string frontend_address_;
  std::string namespace_;
  std::string keyspace_name_;
  std::unique_ptr<frontend::Keyspace> keyspace_;
  int num_keys_;
  int connection_timeout_;
  int request_timeout_;
  int num_connections_;
  bool use_read_modify_write_;
  int batch_size_;
  
  // Properties are inherited from base DB class

  // Thread-local transaction management
  static thread_local std::string thread_transaction_id_;
  static thread_local std::unique_ptr<frontend::Keyspace> thread_keyspace_;
  static thread_local size_t thread_connection_index_;

  // Helper methods
  void LoadConfiguration();
  void InitializeConnectionPool();
  void CreateKeyspaceIfNotExists();
  void StartTransaction();
  void CommitTransaction();
  void AbortTransaction();
  
  // Optimized gRPC methods
  DB::Status ReadModifyWrite(const std::string &table, const std::string &key, 
                            const std::vector<DB::Field> &values, std::vector<DB::Field> &result);
  DB::Status BatchReadModifyWrite(const std::vector<std::string> &keys,
                                 const std::vector<std::vector<DB::Field>> &values,
                                 std::vector<std::vector<DB::Field>> &results);
  
  // Get next connection from pool (round-robin)
  std::unique_ptr<frontend::Frontend::Stub>& GetNextStub();
  
  std::string SerializeFields(const std::vector<DB::Field> &values);
  void DeserializeFields(const std::string &data, std::vector<DB::Field> &values);
  std::string KeyToBytes(const std::string &key);
  std::string BytesToKey(const std::string &bytes);
  
  grpc::Status MakeGrpcCall(const std::function<grpc::Status()>& call);
};

DB *NewAtomixDB();

} // ycsbc

#endif // YCSB_C_ATOMIX_DB_H_ 