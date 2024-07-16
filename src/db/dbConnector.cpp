#include "dbConnector.hpp"

#include <chrono>
#include <string>
#include <iostream>
#include <iomanip>
#include <thread>
#include <utility>
#include <vector>
#include <variant>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "src/utils/yamlConfig.hpp"
#include "src/db/comparator.hpp"
#include "src/db/fullKey.hpp"

using namespace std::chrono_literals;

dbConnector::dbConnector(YAMLConfig config)
{
    selfId = config.getId();
    seqCount = std::vector<std::atomic<leveldb::SequenceNumber>>(config.getMaxReplicaId());
    std::string filename = config.getDbFile();
    leveldb::Options options;
    options.create_if_missing = true;
    options.comparator = &(leveldb::GLOBAL_COMPARATOR);
    db = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, filename, &db);
    assert(status.ok());
    for (int i = 0; i < config.getMaxReplicaId(); ++i) {
        seqCount[i] = getMaxSeqForReplica(i);
    }
}

dbConnector::~dbConnector()
{
    delete db;
}

leveldb::SequenceNumber dbConnector::getMaxSeqForReplica(int id) {
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(options));
    leveldb::SequenceNumber seq = 0;
    for (it->Seek(generateLseqKey(0, id));
         it->Valid();
         it->Next())
    {
        std::string lseq = it->key().ToString();
        if (lseq[0] != '#')
            break;
        if (lseq[0] == '#' && std::stoi(lseqToReplicaId(lseq)) != id)
            break;
        seq = lseqToSeq(lseq);
    }
    db->ReleaseSnapshot(options.snapshot);
    return seq;
}

leveldb::SequenceNumber dbConnector::sequenceNumberForReplica(int id) {
    return seqCount[id].load(std::memory_order_acquire);
}

replyFormat dbConnector::put(std::string key, std::string value) {
    std::string realKey = generateNormalKey(key, selfId);
    auto [seq, s] = db->PutSequence(leveldb::WriteOptions(), realKey, value);
    if (!s.ok()) {
        return {"", s};
    }
    leveldb::WriteBatch batch;
    batch.Put(generateGetseqKey(realKey), generateLseqKey(seq, selfId));
    batch.Put(generateLseqKey(seq, selfId), realKey);
    batch.Put(FullKey(key, seq, selfId).getFullKey(), value);
    leveldb::Status st = db->Write(leveldb::WriteOptions(), &batch);
    if (!st.ok()) {
        return {"", st};
    }
        
    seqCount[selfId].store(seq, std::memory_order_release);
    return {generateLseqKey(seq, selfId), st};
}

replyFormat dbConnector::remove(std::string key) {
    std::string realKey = generateNormalKey(std::move(key), selfId);
    auto [seq, s] = db->DeleteSequence(leveldb::WriteOptions(), realKey);
    if (!s.ok()) {
        return {"", s};
    }
    leveldb::Status intermediateStatus = db->Put(leveldb::WriteOptions(), generateGetseqKey(realKey), generateLseqKey(seq, selfId));
    if (!intermediateStatus.ok()) {
        return {"", intermediateStatus};
    }
    auto [secondSeq, st] = db->DeleteSequence(leveldb::WriteOptions(), generateLseqKey(seq, selfId));

    {
        seqCount[selfId].store(secondSeq, std::memory_order_release);
    }
    return {generateLseqKey(seq, selfId), st};
}

pureReplyValue dbConnector::get(std::string key) {
    int cnt = 0;
    std::string realKey = generateNormalKey(std::move(key), selfId);
    std::string lseqKey = generateGetseqKey(realKey);
    std::string subSearchKey;

    while (true)
    {
        ++cnt;
        leveldb::ReadOptions options;
        options.snapshot = db->GetSnapshot();
        //MAXREAD constant
        if (cnt > 100) {
            return {"", leveldb::Status::IOError("Unsuccessfully read. Retry when db load decreases"), std::string()};
        }
        std::string value;
        auto [seq, s] = db->GetSequence(options, realKey, &value);
        if (!s.ok()) {
            return {"", s, std::string()};
        }
        s = db->Get(options, lseqKey, &subSearchKey);
        if (!s.ok() && !s.IsNotFound()) {
            return {"", s, ""};
        }
        std::string requestedKey;
        s = db->Get(options, subSearchKey, &requestedKey);
        if (!s.ok() && !s.IsNotFound()) {
            return {"", s, ""};
        }
        if (requestedKey != realKey) {
            std::this_thread::sleep_for(100ms);
            continue;
        }
        return {subSearchKey, s, value};
    }
}

pureReplyValue dbConnector::get(std::string key, int id) {
    if (id == selfId)
        return get(std::move(key));
    std::string res;
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    std::string realKey = generateNormalKey(std::move(key), id);
    auto [seq, s] = db->GetSequence(options, realKey, &res);
    if (!s.ok()) {
        return {"", s, ""};
    }
    std::string lseq;
    s = db->Get(options, generateGetseqKey(realKey), &lseq);
    return {lseq, s, res};
}

//UNSAFE, this method can override existing value
//Should never be called with unchecked value
//Argument should contain precise keys from another replica
leveldb::Status dbConnector::putBatch(const batchValues& keyValuePairs) {
    leveldb::WriteBatch batch;
    for (const auto& [lseq, key, value] : keyValuePairs) {
        batch.Put(lseq, key);
        batch.Put(key, value);
        batch.Put(generateGetseqKey(key), lseq);
        int replicaId = std::stoi(lseqToReplicaId(lseq));
        leveldb::SequenceNumber seq = lseqToSeq(lseq);
        batch.Put(FullKey(stampedKeyToRealKey(key), seq, replicaId).getFullKey(), value);
        uint64_t current_id = seqCount[replicaId].load(std::memory_order_acquire);
        while(current_id < seq &&
            ! seqCount[replicaId].compare_exchange_weak(current_id, seq, std::memory_order_acq_rel))
        {}
    }
    leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
    return s;
}

replyBatchFormat dbConnector::getByLseq(leveldb::SequenceNumber seq, int id, int limit, LSEQ_COMPARE isGreater) {
    return getByLseq(generateLseqKey(seq, id), limit, isGreater);
}

replyBatchFormat dbConnector::getValuesForKey(const std::string& key, leveldb::SequenceNumber seq, int id, int limit, LSEQ_COMPARE isGreater) {
    batchValues res;
    int cnt = -1;
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(options));
    FullKey searchValue = (isGreater == LSEQ_COMPARE::GREATER ? FullKey(key, seq + 1, id) : FullKey(key, seq, id));
    for (it->Seek(searchValue.getFullKey());
         it->Valid() && cnt <= limit;
         it->Next())
    {
        FullKey currentKey(it->key().ToString());
        if (currentKey.getKey() != key)
            break;
        int replicaId = currentKey.getReplicaId();
        if (limit != -1)
            ++cnt;
        res.push_back({generateLseqKey(currentKey.getSeq(), replicaId), generateNormalKey(key, replicaId), it->value().ToString()});
    }
    leveldb::Status status = it->status();
    db->ReleaseSnapshot(options.snapshot);
    return {status, res};
}

replyBatchFormat dbConnector::getAllValuesForKey(const std::string& key, int id, int limit, LSEQ_COMPARE isGreater) {
    return getValuesForKey(key, 0, id, limit, isGreater);
}

replyBatchFormat dbConnector::getByLseq(std::string lseq, int limit, LSEQ_COMPARE isGreater) {
    batchValues res;
    int cnt = -1;
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(options));
    if (isGreater == LSEQ_COMPARE::GREATER)
        lseq = generateLseqKey(lseqToSeq(lseq) + 1, std::stoi(lseqToReplicaId(lseq)));
    for (it->Seek(lseq);
         it->Valid() && cnt <= limit;
         it->Next())
    {
        if (!(lseqToReplicaId(it->key().ToString()) == lseqToReplicaId(lseq))) {
            break;
        }
        if (limit != -1)
            ++cnt;
        int replicaId = std::stoi(lseqToReplicaId(it->key().ToString()));
        std::string realKey = FullKey(stampedKeyToRealKey(it->value().ToString()), lseqToSeq(it->key().ToString()), replicaId).getFullKey();//it->value().ToString();
        std::string realValue;
        auto s = db->Get(options, realKey, &realValue);
        if (!s.ok()) {
            db->ReleaseSnapshot(options.snapshot);
            return {s, res};
        }
        res.push_back({it->key().ToString(), it->value().ToString(), realValue});
    }
    leveldb::Status status = it->status();
    db->ReleaseSnapshot(options.snapshot);
    return {status, res};
}

std::string dbConnector::generateLseqKey(leveldb::SequenceNumber seq, int id) {
    std::string lseqNumber = idToString(id);
    lseqNumber[0] = '#';
    return  lseqNumber + seqToString(seq);
}

std::string dbConnector::stampedKeyToRealKey(const std::string& stampedKey) {
    return stampedKey.substr(10);
}

std::string dbConnector::generateNormalKey(std::string key, int id) {
    return idToString(id) + std::move(key);
}

std::string dbConnector::idToString(int id) {
    std::stringstream ss;
    //id - up to 256^3
    ss << std::setw(10) << std::setfill('0') << id;
    return ss.str();
}

std::string dbConnector::seqToString(leveldb::SequenceNumber seq) {
    std::stringstream ss;
    //id - up to 256^3
    ss << std::setw(15) << std::setfill('0') << seq;
    return ss.str();
}

std::string dbConnector::generateGetseqKey(std::string realKey) {
    realKey[0] = '@';
    return realKey;
}

std::string dbConnector::lseqToReplicaId(const std::string& lseq) {
    return lseq.substr(1, 9);
}

leveldb::SequenceNumber dbConnector::lseqToSeq(const std::string& lseq) {
    return std::stoll(lseq.substr(10));
}