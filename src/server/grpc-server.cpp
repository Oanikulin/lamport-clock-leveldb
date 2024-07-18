#include "grpc-server.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <stdexcept>
#include <numeric>
#include <algorithm>
#include <random>
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

using lseqdb::LSeqDatabase;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::ServerContext;
using grpc::Status;

using lseqdb::Config;
using lseqdb::DBItems;
using lseqdb::EventsRequest;
using lseqdb::LSeq;
using lseqdb::PutRequest;
using lseqdb::ReplicaKey;
using lseqdb::SeekGetRequest;
using lseqdb::SyncGetRequest;
using lseqdb::Value;

LSeqDatabaseImpl::LSeqDatabaseImpl(const YAMLConfig& config, dbConnector* database) : db(database), cfg(config) {
    syncMxs_.resize(config.getMaxReplicaId());
}

Status LSeqDatabaseImpl::GetValue(ServerContext* context, const ReplicaKey* request, Value* response) {
    pureReplyValue res;
    if (!request->has_replica_id()) {
        res = db->get(request->key());
    } else {
        res = db->get(request->key(), request->replica_id());
    }
    if (!res.response_status.ok()) {
        return {grpc::StatusCode::UNAVAILABLE, res.response_status.ToString()};
    }
    response->set_lseq(res.lseq);
    response->set_value(res.value);
    return Status::OK;
}

Status LSeqDatabaseImpl::Put(ServerContext* context, const PutRequest* request, LSeq* response) {
    auto res = db->put(request->key(), request->value());
    if (!res.response_status.ok()) {
        return {grpc::StatusCode::ABORTED, res.response_status.ToString()};
    }
    response->set_lseq(res.lseq);
    return Status::OK;
}

Status LSeqDatabaseImpl::SeekGet(ServerContext* context, const SeekGetRequest* request, DBItems* response) {
    replyBatchFormat res;
    int limit = request->has_limit() ? static_cast<int>(request->limit()) : -1;
    const auto& lseq = request->lseq();
    if (request->has_key()) {
        res = db->getValuesForKey(request->key(), dbConnector::lseqToSeq(lseq), std::stoi(dbConnector::lseqToReplicaId(lseq)), limit, dbConnector::LSEQ_COMPARE::GREATER);
    } else {
        res = db->getByLseq(lseq, limit, dbConnector::LSEQ_COMPARE::GREATER);
    }

    if (!res.response_status.ok()) {
        return {grpc::StatusCode::UNAVAILABLE, res.response_status.ToString()};
    }
    for (const auto& item : res.values) {
        auto proto_item = response->add_items();
        proto_item->set_lseq(item.lseq);
        proto_item->set_key(dbConnector::stampedKeyToRealKey(item.key));
        proto_item->set_value(item.value);
    }
    return Status::OK;
}

Status LSeqDatabaseImpl::GetReplicaEvents(ServerContext* context, const EventsRequest* request, DBItems* response) {
    SeekGetRequest req;
    if (request->has_limit()) {
        req.set_limit(request->limit());
    }
    if (request->has_key()) {
        req.set_key(request->key());
    }
    if (request->has_lseq()) {
        req.set_lseq(request->lseq());
    } else {
        req.set_lseq(dbConnector::generateLseqKey(0, request->replica_id()));
    }
    return SeekGet(context, &req, response);
}

Status LSeqDatabaseImpl::GetConfig(ServerContext* context, const ::google::protobuf::Empty*, Config* response) {
    response->set_self_replica_id(cfg.getId());
    response->set_max_replica_id(cfg.getMaxReplicaId());
    return Status::OK;
}

Status LSeqDatabaseImpl::SyncGet_(ServerContext* context, const SyncGetRequest* request, LSeq* response) {
    auto seq = db->sequenceNumberForReplica(request->replica_id());
    response->set_lseq(dbConnector::generateLseqKey(seq, request->replica_id()));
    return Status::OK;
}

Status LSeqDatabaseImpl::SyncPut_(ServerContext* context, const DBItems* request, ::google::protobuf::Empty* response) {
    std::lock_guard<std::mutex> lockGuard(syncMxs_[request->replica_id()]);
    batchValues batch;
    batch.reserve(request->items_size());
    for (const auto& item : request->items()) {
        batch.emplace_back(batchValue{item.lseq(), item.key(), item.value()});
    }
    auto res = db->putBatch(batch);
    if (!res.ok()) {
        return {grpc::StatusCode::ABORTED, res.ToString()};
    }
    return Status::OK;
}

void RunServer(const YAMLConfig& config, dbConnector* database) {
    std::string server_address = "0.0.0.0:" + std::to_string(config.getGRPCConfig().port);
    LSeqDatabaseImpl service(config, database);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

std::string GetMaxLSeqFromRemoteReplica(const std::unique_ptr<LSeqDatabase::Stub>& client, size_t replicaId) {
    ClientContext context;
    SyncGetRequest request;
    LSeq response;

    request.set_replica_id(static_cast<int32_t>(replicaId));

    Status status = client->SyncGet_(&context, request, &response);
    if (!status.ok()) {
        std::cerr << status.error_message() << std::endl;
        return "";
    }
    return response.lseq();
}

DBItems DumpBatch(dbConnector* database, const std::string& lseq) {
    auto res = database->getByLseq(lseq);
    if (!res.response_status.ok()) {
        return {};
    }
    DBItems batch;
    for (const auto& item : res.values) {
        auto proto_item = batch.add_items();
        proto_item->set_lseq(item.lseq);
        proto_item->set_key(item.key);
        proto_item->set_value(item.value);
    }
    return batch;
}

bool SendNewBatch(const std::unique_ptr<LSeqDatabase::Stub>& client, const DBItems& batch) {
    ClientContext context;
    google::protobuf::Empty response;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(500);
    context.set_deadline(deadline);

    Status status = client->SyncPut_(&context, batch, &response);
    if (!status.ok()) {
        std::cerr << status.error_message() << std::endl;
        return false;
    }
    return true;
}

void SyncLoop(const YAMLConfig& config, dbConnector* database) {
    const auto& replicas = config.getReplicas();
    std::vector<size_t> replicaIds(replicas.size());
    std::vector<size_t> syncOrder(config.getMaxReplicaId());
    std::iota(replicaIds.begin(), replicaIds.end(), static_cast<size_t>(0));
    std::iota(syncOrder.begin(), syncOrder.end(), static_cast<size_t>(0));

    // Shuffle replicas for sync to reduce per-node load
    std::random_device rd;
    std::mt19937 rnd(rd());

    std::shuffle(replicaIds.begin(), replicaIds.end(), rnd);
    std::shuffle(syncOrder.begin(), syncOrder.end(), rnd);

    // iterate over neighboring replicas in random order
    for (auto i : replicaIds) {
        // Connect to remote replica;
        auto channel = grpc::CreateChannel(replicas[i], grpc::InsecureChannelCredentials());

        std::unique_ptr<LSeqDatabase::Stub> client(LSeqDatabase::NewStub(channel));

        // iterate over known replicas in random order
        for (auto id : syncOrder) {
            auto maxSeq = database->sequenceNumberForReplica(id);
            if (maxSeq < 1) {
                // no data from replica=id
                continue;
            }
            auto remoteLSeq = GetMaxLSeqFromRemoteReplica(client, id);
            if (remoteLSeq.empty()) {
                // error
                std::cerr << "Failed to get maxLSeq(" << id << ") from " << replicas[i] << std::endl;
                continue;
            }
            auto remoteSeq = dbConnector::lseqToSeq(remoteLSeq);
            if (maxSeq <= remoteSeq) {
                // No new data
                continue;
            } else {
                std::cout << "Trying to sync data from " << id << " with " << replicas[i] << "\n";
                std::cout << "Current localMaxSeq(" << id << ")=" << maxSeq << "; "
                          << "remoteMaxSeq(" << replicas[i] << ", " << id << ")=" << remoteSeq << std::endl;
            }

            auto newBatch = DumpBatch(database, remoteLSeq);
            if (newBatch.items_size()) {
                newBatch.set_replica_id(id);
                if (!SendNewBatch(client, newBatch)) {
                    std::cerr << "Failed to send batch to " << replicas[i] << std::endl;
                } else {
                    std::cout << "Data has been successfully synchronized" << std::endl;
                }
                std::this_thread::sleep_for(100ms);
            }
        }
    }
}