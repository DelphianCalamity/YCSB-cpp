# Atomix Database Binding for YCSB-cpp

This directory contains the YCSB-cpp binding for Atomix Database, a distributed transactional key-value store.

## Overview

The Atomix binding provides a transactional interface to Atomix Database through gRPC. Each operation (read, write, delete) is performed within a transaction context that is automatically managed by the client.

## Features

- **Transactional Operations**: All operations are performed within transactions
- **Automatic Transaction Management**: Transactions are automatically started, committed, and aborted
- **gRPC Communication**: Uses gRPC for efficient communication with the Atomix frontend
- **Keyspace Management**: Automatically creates keyspaces if they don't exist

## Configuration

The following properties can be configured:

| Property | Default | Description |
|----------|---------|-------------|
| `atomix.frontend.address` | `127.0.0.1:50057` | The address of the Atomix frontend server |
| `atomix.namespace` | `ycsb` | The namespace to use for keyspaces |
| `atomix.keyspace` | `default` | The keyspace name to use for operations |
| `atomix.keyspace.num_keys` | `50` | Number of keys for keyspace creation |
| `atomix.connection.timeout` | `30` | Connection timeout in seconds |
| `atomix.request.timeout` | `10` | Request timeout in seconds |

## Building

### Prerequisites

1. **Protocol Buffers**: Install protobuf compiler and C++ runtime
   ```bash
   # Ubuntu/Debian
   sudo apt-get install protobuf-compiler libprotobuf-dev
   
   # CentOS/RHEL
   sudo yum install protobuf-compiler protobuf-devel
   
   # macOS
   brew install protobuf
   ```

2. **gRPC**: Install gRPC C++ library
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libgrpc++-dev
   
   # CentOS/RHEL
   sudo yum install grpc-devel
   
   # macOS
   brew install grpc
   ```

### Build Steps

1. **Generate Protocol Buffer Files**:
   ```bash
   cd YCSB-cpp/atomix
   protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` atomix.proto
   ```

2. **Build YCSB-cpp with Atomix Support**:
   ```bash
   cd YCSB-cpp
   mkdir build && cd build
   cmake -DBIND_ATOMIX=ON ..
   make
   ```

## Usage

### Basic Usage

```bash
# Load data
./ycsb -db atomix -P workloads/workloada -P atomix/atomix.properties

# Run workload
./ycsb -db atomix -P workloads/workloada -P atomix/atomix.properties
```

### Custom Configuration

Create a properties file (e.g., `my_atomix.properties`):

```properties
atomix.frontend.address=localhost:50057
atomix.namespace=ycsb
atomix.keyspace=test_keyspace
atomix.keyspace.num_keys=100
```

Then use it:

```bash
./ycsb -db atomix -P workloads/workloada -P my_atomix.properties
```

## Limitations

- **Scan Operations**: Scan operations are not currently implemented (returns `NOT_IMPLEMENTED`)
- **Delete Operations**: Delete operations are not currently implemented (returns `NOT_IMPLEMENTED`)
- **Field Selection**: Only the first field in a multi-field operation is used
- **Transaction Scope**: Each operation uses its own transaction (no batching across operations)

## Example Workload

Here's an example workload configuration for testing:

```properties
# Workload configuration
workload=site.ycsb.workloads.CoreWorkload
recordcount=1000
operationcount=10000

# Atomix configuration
atomix.frontend.address=localhost:50057
atomix.namespace=ycsb
atomix.keyspace=test_keyspace
atomix.keyspace.num_keys=1000

# Operation distribution
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
```

This configuration will perform 50% reads and 50% updates on 1000 records with 10,000 total operations.

## Troubleshooting

### Connection Issues

If you encounter connection issues:

1. Ensure the Atomix frontend server is running
2. Check that the `atomix.frontend.address` property is correct
3. Verify network connectivity between the client and server

### Transaction Errors

If you encounter transaction errors:

1. Check the Atomix server logs for detailed error messages
2. Ensure the keyspace exists and is accessible
3. Verify that the namespace and keyspace names are correct

### Build Issues

If you encounter build issues:

1. Ensure protobuf and gRPC are properly installed
2. Check that the protobuf files were generated correctly
3. Verify that the CMake configuration includes the Atomix binding

## Implementation Status

This is a work-in-progress implementation. The current version includes:

- [x] Basic database interface implementation
- [x] Configuration management
- [x] Mock transaction management
- [x] Basic read/update operations (mock)
- [ ] gRPC integration
- [ ] Protocol buffer integration
- [ ] Real transaction management
- [ ] Scan operations
- [ ] Delete operations
- [ ] Error handling and retry logic

## Contributing

To contribute to this binding:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This binding is licensed under the Apache License, Version 2.0. 