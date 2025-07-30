//
//  atomix_db.cc
//  YCSB-cpp
//
//

#include "atomix_db.h"
#include "../core/db_factory.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <vector>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>

namespace ycsbc {

// Static member definitions
std::unique_ptr<AtomixDB::ConnectionPool> AtomixDB::connection_pool_ = nullptr;
std::mutex AtomixDB::connection_mutex_;
bool AtomixDB::connection_initialized_ = false;

// Object pool definitions
std::unique_ptr<ObjectPool<frontend::GetRequest>> AtomixDB::get_request_pool_ = nullptr;
std::unique_ptr<ObjectPool<frontend::PutRequest>> AtomixDB::put_request_pool_ = nullptr;
std::unique_ptr<ObjectPool<frontend::StartTransactionRequest>> AtomixDB::start_txn_request_pool_ = nullptr;
std::unique_ptr<ObjectPool<frontend::CommitRequest>> AtomixDB::commit_request_pool_ = nullptr;
std::unique_ptr<ObjectPool<frontend::ReadModifyWriteRequest>> AtomixDB::read_modify_write_request_pool_ = nullptr;

// Thread-local member definition
thread_local std::string AtomixDB::thread_transaction_id_;
thread_local std::unique_ptr<frontend::Keyspace> AtomixDB::thread_keyspace_;
thread_local size_t AtomixDB::thread_connection_index_ = 0;

AtomixDB::AtomixDB() {
  // Initialize member variables
  frontend_address_ = FRONTEND_ADDRESS_DEFAULT;
  namespace_ = NAMESPACE_DEFAULT;
  keyspace_name_ = KEYSPACE_DEFAULT;
  num_keys_ = std::stoi(NUM_KEYS_DEFAULT);
  connection_timeout_ = std::stoi(CONNECTION_TIMEOUT_DEFAULT);
  request_timeout_ = std::stoi(REQUEST_TIMEOUT_DEFAULT);
  num_connections_ = std::stoi(NUM_CONNECTIONS_DEFAULT);
  use_read_modify_write_ = (USE_READ_MODIFY_WRITE_DEFAULT == std::string("true"));
  batch_size_ = std::stoi(BATCH_SIZE_DEFAULT);
  thread_transaction_id_.clear();
  props_ = nullptr;
}

AtomixDB::~AtomixDB() {
  Cleanup();
}

void AtomixDB::Init() {
  try {
    // Properties should be set via SetProps() before Init() is called
    if (!props_) {
      std::cerr << "[ERROR] Properties not set. Call SetProps() before Init()." << std::endl;
      return;
    }
    LoadConfiguration();
    
    // Initialize connection pool and object pools
    InitializeConnectionPool();
    
    // Create keyspace if it doesn't exist
    CreateKeyspaceIfNotExists();
    std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // Reduced from 4s to 100ms
    std::cout << "[INFO] Atomix client initialized successfully" << std::endl;
    std::cout << "[INFO] Frontend address: " << frontend_address_ << std::endl;
    std::cout << "[INFO] Keyspace: " << namespace_ << "." << keyspace_name_ << std::endl;
    std::cout << "[INFO] Using read_modify_write: " << (use_read_modify_write_ ? "true" : "false") << std::endl;
    std::cout << "[INFO] Connection pool size: " << num_connections_ << std::endl;
    
  } catch (const std::exception& e) {
    std::cerr << "[ERROR] Failed to initialize Atomix client: " << e.what() << std::endl;
    throw;
  }
}

void AtomixDB::Cleanup() {
  try {
    // Abort any open transaction
    if (!thread_transaction_id_.empty()) {
      AbortTransaction();
    }
    
    std::cout << "[INFO] Atomix client cleaned up successfully" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "[WARNING] Error during cleanup: " << e.what() << std::endl;
  }
}

DB::Status AtomixDB::Read(const std::string &table, const std::string &key,
                          const std::vector<std::string> *fields, std::vector<DB::Field> &result) {
  try {
    if (use_read_modify_write_) {
      // Use read_modify_write for better performance
      // std::vector<DB::Field> empty_values;
      // return ReadModifyWrite(table, key, empty_values, result);
      return DB::Status::kOK;
    } else {
      // Traditional approach with separate calls
      auto& stub = GetNextStub();
      if (!stub) {
        std::cerr << "[ERROR] gRPC stub not initialized" << std::endl;
        return DB::Status::kError;
      }
      
      // Start transaction if not already started
      if (thread_transaction_id_.empty()) {
        StartTransaction();
      }
      
      // Get request from pool
      auto request = get_request_pool_->acquire();
      request->set_transaction_id(thread_transaction_id_);
      frontend::Keyspace* request_keyspace = request->mutable_keyspace();
      request_keyspace->set_namespace_(namespace_);
      request_keyspace->set_name(keyspace_name_);
      
      // Parse key as int and convert to bytes
      int key_int = std::stoi(key);
      char bytes[4];
      bytes[0] = (key_int >> 24) & 0xFF;
      bytes[1] = (key_int >> 16) & 0xFF;
      bytes[2] = (key_int >> 8) & 0xFF;
      bytes[3] = key_int & 0xFF;
      request->set_key(std::string(bytes, 4));
      
      // Execute get operation with timeout
      frontend::GetResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(request_timeout_ * 1000));
      grpc::Status grpc_status = stub->Get(&context, *request, &response);
      
      // Release request back to pool
      get_request_pool_->release(std::move(request));
      
      if (grpc_status.ok() && response.status() == "Get request processed successfully") {
        // Parse response
        result.clear();
        if (!response.value().empty()) {
          DeserializeFields(response.value(), result);
        }
        return DB::Status::kOK;
      } else {
        std::cerr << "[ERROR] Get operation failed: " << grpc_status.error_message() << std::endl;
        return DB::Status::kError;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "[ERROR] Error during read operation: " << e.what() << std::endl;
    return DB::Status::kError;
  }
}

DB::Status AtomixDB::Scan(const std::string &table, const std::string &key, int record_count,
                          const std::vector<std::string> *fields, std::vector<std::vector<DB::Field>> &result) {
  // std::cout << "[WARNING] Scan operation not implemented for Atomix" << std::endl;
  return DB::Status::kNotImplemented;
}

DB::Status AtomixDB::Update(const std::string &table, const std::string &key, std::vector<DB::Field> &values) {
  try {
    if (use_read_modify_write_) {
      // Use read_modify_write for better performance
      std::vector<DB::Field> result;
      return ReadModifyWrite(table, key, values, result);
    } else {
      // Traditional approach with separate calls
      auto& stub = GetNextStub();
      if (!stub) {
        std::cerr << "[ERROR] gRPC stub not initialized" << std::endl;
        return DB::Status::kError;
      }
      
      // Start transaction if not already started
      if (thread_transaction_id_.empty()) {
        StartTransaction();
      }
      
      // Get request from pool
      auto request = put_request_pool_->acquire();
      request->set_transaction_id(thread_transaction_id_);
      frontend::Keyspace* request_keyspace = request->mutable_keyspace();
      request_keyspace->set_namespace_(namespace_);
      request_keyspace->set_name(keyspace_name_);
      
      // Parse key as int and convert to bytes
      int key_int = std::stoi(key);
      char bytes[4];
      bytes[0] = (key_int >> 24) & 0xFF;
      bytes[1] = (key_int >> 16) & 0xFF;
      bytes[2] = (key_int >> 8) & 0xFF;
      bytes[3] = key_int & 0xFF;
      request->set_key(std::string(bytes, 4));
      
      char value[4];
      value[0] = 0;
      value[1] = 0;
      value[2] = 0;
      value[3] = 0;
      request->set_value(std::string(value, 4));
      
      // Execute put operation with timeout
      frontend::PutResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(request_timeout_ * 1000));
      grpc::Status grpc_status = stub->Put(&context, *request, &response);
      
      // Release request back to pool
      put_request_pool_->release(std::move(request));

      if (grpc_status.ok() && response.status() == "Put request processed successfully") {
        // Commit the transaction after successful put
        CommitTransaction();
        return DB::Status::kOK;
      } else {
        std::cerr << "[ERROR] Put operation failed: " << grpc_status.error_message() << std::endl;
        return DB::Status::kError;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "[ERROR] Error during update operation: " << e.what() << std::endl;
    return DB::Status::kError;
  }
}

DB::Status AtomixDB::Insert(const std::string &table, const std::string &key, std::vector<DB::Field> &values) {
  // For Atomix, insert is the same as update
  return Update(table, key, values);
}

DB::Status AtomixDB::Delete(const std::string &table, const std::string &key) {
  std::cout << "[WARNING] Delete operation not implemented for Atomix" << std::endl;
  return DB::Status::kNotImplemented;
}

DB::Status AtomixDB::ReadModifyWrite(const std::string &table, const std::string &key, 
                                    const std::vector<DB::Field> &values, std::vector<DB::Field> &result) {
  try {
    auto& stub = GetNextStub();
    if (!stub) {
      std::cerr << "[ERROR] gRPC stub not initialized" << std::endl;
      return DB::Status::kError;
    }
    
    // Get request from pool
    auto request = read_modify_write_request_pool_->acquire();
    
    // Set keyspace
    frontend::Keyspace* request_keyspace = request->mutable_keyspace();
    request_keyspace->set_namespace_(namespace_);
    request_keyspace->set_name(keyspace_name_);
    
    // Add key
    int key_int = std::stoi(key);
    char bytes[4];
    bytes[0] = (key_int >> 24) & 0xFF;
    bytes[1] = (key_int >> 16) & 0xFF;
    bytes[2] = (key_int >> 8) & 0xFF;
    bytes[3] = key_int & 0xFF;
    request->add_keys(std::string(bytes, 4));
    
    // Set value
    char value[4];
    value[0] = 0;
    value[1] = 0;
    value[2] = 0;
    value[3] = 0;
    request->set_value(std::string(value, 4));
    
    // Execute read_modify_write operation
    frontend::ReadModifyWriteResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(request_timeout_ * 1000));
    grpc::Status grpc_status = stub->ReadModifyWrite(&context, *request, &response);
    
    // Release request back to pool
    read_modify_write_request_pool_->release(std::move(request));
    
    if (grpc_status.ok() && response.status() == "ReadModifyWrite request processed successfully") {
      // For now, return empty result since read_modify_write doesn't return the read value
      result.clear();
      return DB::Status::kOK;
    } else {
      std::cerr << "[ERROR] ReadModifyWrite operation failed: " << grpc_status.error_message() << std::endl;
      return DB::Status::kError;
    }
  } catch (const std::exception& e) {
    std::cerr << "[ERROR] Error during read_modify_write operation: " << e.what() << std::endl;
    return DB::Status::kError;
  }
}

std::unique_ptr<frontend::Frontend::Stub>& AtomixDB::GetNextStub() {
  if (!connection_pool_) {
    static std::unique_ptr<frontend::Frontend::Stub> null_stub = nullptr;
    return null_stub;
  }
  
  // Round-robin connection selection
  size_t index = thread_connection_index_++ % connection_pool_->stubs.size();
  return connection_pool_->stubs[index];
}

void AtomixDB::InitializeConnectionPool() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  if (connection_initialized_) {
    return;
  }
  
  // Create connection pool
  connection_pool_ = std::make_unique<ConnectionPool>();
  
  // Create optimized channel arguments
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(2 * 1024 * 1024);  // 2MB
  args.SetMaxSendMessageSize(2 * 1024 * 1024);     // 2MB
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);  // 30s keepalive
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000); // 5s timeout
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  args.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 300000); // 5min
  args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1000); // Allow more concurrent streams
  
  // Create multiple connections
  for (int i = 0; i < num_connections_; ++i) {
    auto channel = grpc::CreateCustomChannel(
      frontend_address_, 
      grpc::InsecureChannelCredentials(),
      args
    );
    
    if (!channel) {
      std::cerr << "[ERROR] Failed to create gRPC channel " << i << std::endl;
      continue;
    }
    
    auto stub = frontend::Frontend::NewStub(channel);
    if (!stub) {
      std::cerr << "[ERROR] Failed to create gRPC stub " << i << std::endl;
      continue;
    }
    
    connection_pool_->channels.push_back(channel);
    connection_pool_->stubs.push_back(std::move(stub));
  }
  
  // Initialize object pools
  get_request_pool_ = std::make_unique<ObjectPool<frontend::GetRequest>>(num_connections_ * 2);
  put_request_pool_ = std::make_unique<ObjectPool<frontend::PutRequest>>(num_connections_ * 2);
  start_txn_request_pool_ = std::make_unique<ObjectPool<frontend::StartTransactionRequest>>(num_connections_);
  commit_request_pool_ = std::make_unique<ObjectPool<frontend::CommitRequest>>(num_connections_);
  read_modify_write_request_pool_ = std::make_unique<ObjectPool<frontend::ReadModifyWriteRequest>>(num_connections_ * 2);
  
  connection_initialized_ = true;
  std::cout << "[INFO] Created " << connection_pool_->stubs.size() << " gRPC connections" << std::endl;
}

void AtomixDB::LoadConfiguration() {
  if (!props_) {
    throw std::runtime_error("Properties not set");
  }
  
  frontend_address_ = props_->GetProperty(FRONTEND_ADDRESS_PROPERTY, FRONTEND_ADDRESS_DEFAULT);
  namespace_ = props_->GetProperty(NAMESPACE_PROPERTY, NAMESPACE_DEFAULT);
  keyspace_name_ = props_->GetProperty(KEYSPACE_PROPERTY, KEYSPACE_DEFAULT);
  num_keys_ = std::stoi(props_->GetProperty(NUM_KEYS_PROPERTY, NUM_KEYS_DEFAULT));
  connection_timeout_ = std::stoi(props_->GetProperty(CONNECTION_TIMEOUT_PROPERTY, CONNECTION_TIMEOUT_DEFAULT));
  request_timeout_ = std::stoi(props_->GetProperty(REQUEST_TIMEOUT_PROPERTY, REQUEST_TIMEOUT_DEFAULT));
  num_connections_ = std::stoi(props_->GetProperty(NUM_CONNECTIONS_PROPERTY, NUM_CONNECTIONS_DEFAULT));
  use_read_modify_write_ = (props_->GetProperty(USE_READ_MODIFY_WRITE_PROPERTY, USE_READ_MODIFY_WRITE_DEFAULT) == std::string("true"));
  batch_size_ = std::stoi(props_->GetProperty(BATCH_SIZE_PROPERTY, BATCH_SIZE_DEFAULT));
}

void AtomixDB::CreateKeyspaceIfNotExists() {
  try {
    // Check if stub is initialized
    auto& stub = GetNextStub();
    if (!stub) {
      std::cerr << "[ERROR] gRPC stub not initialized in CreateKeyspaceIfNotExists" << std::endl;
      return;
    }
    
    // Create thread-local keyspace object
    thread_keyspace_ = std::make_unique<frontend::Keyspace>();
    thread_keyspace_->set_namespace_(namespace_);
    thread_keyspace_->set_name(keyspace_name_);
    
    // Build key ranges for the keyspace
    std::vector<frontend::KeyRangeRequest> ranges;
    for (int i = 0; i < num_keys_; i++) {
      frontend::KeyRangeRequest range;
      // Use 1-byte keys for lower and upper bounds
      std::string lower(1, static_cast<char>(i));
      std::string upper(1, static_cast<char>(i + 1));
      range.set_lower_bound_inclusive(lower);
      range.set_upper_bound_exclusive(upper);
      ranges.push_back(range);
    }

    // Build the primary region and zone
    frontend::Region region;
    region.set_name("test-region");
    frontend::Zone zone;
    *zone.mutable_region() = region;
    zone.set_name("a");

    // Build the CreateKeyspaceRequest
    frontend::CreateKeyspaceRequest request;
    request.set_namespace_(namespace_);
    request.set_name(keyspace_name_);
    *request.mutable_primary_zone() = zone;
    for (const auto& range : ranges) {
      *request.add_base_key_ranges() = range;
    }

    // Try to create keyspace with timeout
    frontend::CreateKeyspaceResponse response;
    
    // Set up context with timeout
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    grpc::Status grpc_status = stub->CreateKeyspace(&context, request, &response);
    
    if (grpc_status.ok()) {
      std::cout << "[INFO] Created keyspace: " << response.keyspace_id() << std::endl;
    } else {
      // Check if it's a connection error (server not running)
      if (grpc_status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        std::cout << "[WARNING] Cannot connect to Atomix server at " << frontend_address_ << std::endl;
        std::cout << "[WARNING] Please ensure the Atomix server is running" << std::endl;
      } else {
        // Keyspace might already exist, which is fine
        std::cout << "[DEBUG] Keyspace creation failed (might already exist): " << grpc_status.error_message() << std::endl;
      }
    }
  } catch (const std::exception& e) {
    std::cout << "[DEBUG] Keyspace creation failed (might already exist): " << e.what() << std::endl;
  }
}

void AtomixDB::StartTransaction() {
  // std::lock_guard<std::mutex> lock(transaction_mutex_);
  
  if (!thread_transaction_id_.empty()) {
    return; // Transaction already started
  }
  
  try {
    // Create StartTransactionRequest
    auto request = start_txn_request_pool_->acquire();
    request->set_allocated_keyspace(thread_keyspace_.release());
    
    // Execute start transaction with timeout
    frontend::StartTransactionResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    grpc::Status grpc_status = GetNextStub()->StartTransaction(&context, *request, &response);
    
    // Restore keyspace ownership
    thread_keyspace_.reset(request->release_keyspace());
    start_txn_request_pool_->release(std::move(request));
    
    if (grpc_status.ok() && response.status() == "Start transaction request processed successfully") {
      thread_transaction_id_ = response.transaction_id();
      // std::cout << "[DEBUG] Started transaction: " << thread_transaction_id_ << std::endl;
    } else {
      // std::cerr << "[ERROR] Failed to start transaction: " << grpc_status.error_message() << std::endl;
      // std::cerr << "[ERROR] Response status: " << response.status() << std::endl;
      throw std::runtime_error("Failed to start transaction: " + response.status());
    }
  } catch (const std::exception& e) {
    // std::cerr << "[ERROR] Error starting transaction: " << e.what() << std::endl;
    throw;
  }
}

void AtomixDB::CommitTransaction() {
  // std::lock_guard<std::mutex> lock(transaction_mutex_);
  
  if (thread_transaction_id_.empty()) {
    return; // No transaction to commit
  }
  
  // std::cout << "[INFO] Committing transaction" << std::endl;
  try {
    // Create CommitRequest
    auto request = commit_request_pool_->acquire();
    request->set_transaction_id(thread_transaction_id_);
    
    // Execute commit transaction
    frontend::CommitResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    grpc::Status grpc_status = GetNextStub()->Commit(&context, *request, &response);
    
    commit_request_pool_->release(std::move(request));

    if (grpc_status.ok() && response.status() == "Commit request processed successfully") {
      // std::cout << "[DEBUG] Committed transaction: " << thread_transaction_id_ << std::endl;
      thread_transaction_id_.clear();
    } else {
      throw std::runtime_error("Failed to commit transaction: " + response.status());
    }
  } catch (const std::exception& e) {
    // std::cerr << "[ERROR] Error committing transaction: " << e.what() << std::endl;
    throw;
  }
}

void AtomixDB::AbortTransaction() {
  // std::lock_guard<std::mutex> lock(transaction_mutex_);
  
  if (thread_transaction_id_.empty()) {
    return; // No transaction to abort
  }
  
  // TODO: Implement transaction abort after protobuf generation
  // std::cout << "[DEBUG] Aborted transaction: " << thread_transaction_id_ << std::endl;
  thread_transaction_id_.clear();
}

std::string AtomixDB::SerializeFields(const std::vector<DB::Field> &values) {
  // Simple serialization: field1=value1;field2=value2
  std::stringstream ss;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) ss << ";";
    ss << values[i].name << "=" << values[i].value;
  }
  return ss.str();
}

void AtomixDB::DeserializeFields(const std::string &data, std::vector<DB::Field> &values) {
  values.clear();
  
  std::stringstream ss(data);
  std::string item;
  while (std::getline(ss, item, ';')) {
    size_t pos = item.find('=');
    if (pos != std::string::npos) {
      DB::Field field;
      field.name = item.substr(0, pos);
      field.value = item.substr(pos + 1);
      values.push_back(field);
    }
  }
}

std::string AtomixDB::KeyToBytes(const std::string &key) {
  // Simple key conversion: convert string key to bytes
  return key;
}

std::string AtomixDB::BytesToKey(const std::string &bytes) {
  // Simple key conversion: convert bytes back to string key
  return bytes;
}

grpc::Status AtomixDB::MakeGrpcCall(const std::function<grpc::Status()>& call) {
  return call();
}

ycsbc::DB *NewAtomixDB() {
  return new AtomixDB();
}

// Register the Atomix database
const bool registered = DBFactory::RegisterDB("atomix", NewAtomixDB);

} // ycsbc 