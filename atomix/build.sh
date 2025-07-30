#!/bin/bash

# Build script for Atomix YCSB-cpp binding

set -e

echo "Building Atomix YCSB-cpp binding..."

# Check if protoc is available
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc (Protocol Buffers compiler) not found"
    echo "Please install protobuf-compiler:"
    echo "  Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "  CentOS/RHEL: sudo yum install protobuf-compiler"
    echo "  macOS: brew install protobuf"
    exit 1
fi

# Check if grpc_cpp_plugin is available
if ! command -v grpc_cpp_plugin &> /dev/null; then
    echo "Error: grpc_cpp_plugin not found"
    echo "Please install gRPC:"
    echo "  Ubuntu/Debian: sudo apt-get install libgrpc++-dev"
    echo "  CentOS/RHEL: sudo yum install grpc-devel"
    echo "  macOS: brew install grpc"
    exit 1
fi

# Generate Protocol Buffer files
echo "Generating Protocol Buffer files..."
/usr/bin/protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` --experimental_allow_proto3_optional atomix.proto

if [ $? -eq 0 ]; then
    echo "Protocol Buffer files generated successfully"
    echo "Generated files:"
    ls -la *.pb.h *.pb.cc *.grpc.pb.h *.grpc.pb.cc 2>/dev/null || echo "No generated files found"
else
    echo "Error: Failed to generate Protocol Buffer files"
    exit 1
fi

# Go back to YCSB-cpp root
cd ..

# Create build directory
mkdir -p build
cd build

# Configure with CMake
echo "Configuring with CMake..."
cmake -DBIND_ATOMIX=ON ..

# Build
echo "Building YCSB-cpp with Atomix support..."
make -j$(nproc)

echo "Build completed successfully!"
echo "You can now run YCSB-cpp with Atomix:"
echo "  ./ycsb -db atomix -P workloads/workloada -P atomix/atomix.properties" 