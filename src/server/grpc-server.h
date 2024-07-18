#pragma once

#include <iostream>
#include <algorithm>
#include <string>
#include <memory>
#include <deque>
#include <mutex>

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "src/proto-src/lseqDb.grpc.pb.h"
#include "src/proto-src/lseqDb.pb.h"

#include "src/db/dbConnector.hpp"

class LSeqDatabaseImpl final : public lseqdb::LSeqDatabase::Service {
public:
    LSeqDatabaseImpl(const YAMLConfig& config, dbConnector* database);
public:
    grpc::Status GetValue(grpc::ServerContext* context, const lseqdb::ReplicaKey* request, lseqdb::Value* response) override;
    grpc::Status Put(grpc::ServerContext* context, const lseqdb::PutRequest* request, lseqdb::LSeq* response) override;
    grpc::Status SeekGet(grpc::ServerContext* context, const lseqdb::SeekGetRequest* request, lseqdb::DBItems* response) override;
    grpc::Status GetReplicaEvents(grpc::ServerContext* context, const lseqdb::EventsRequest* request, lseqdb::DBItems* response) override;

public:
    grpc::Status GetConfig(grpc::ServerContext* context, const ::google::protobuf::Empty*, lseqdb::Config* response) override;

public:
    grpc::Status SyncGet_(grpc::ServerContext* context, const lseqdb::SyncGetRequest* request, lseqdb::LSeq* response) override;
    grpc::Status SyncPut_(grpc::ServerContext* context, const lseqdb::DBItems* request, ::google::protobuf::Empty* response) override;
private:
    std::deque<std::mutex> syncMxs_;

private:
    dbConnector* db;
    const YAMLConfig& cfg;
};

void RunServer(const YAMLConfig& config, dbConnector* database);
void SyncLoop(const YAMLConfig& config, dbConnector* database);
