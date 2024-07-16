#pragma once

#include <string>
#include <mutex>
#include <utility>
#include <vector>
#include <variant>

#include "leveldb/db.h"
#include "src/utils/yamlConfig.hpp"
#include "src/db/comparator.hpp"

//TODO maybe refactor to use different structs
using lseqType = std::string;
using keyType = std::string;
using valueType = std::string;

using replyFormat = std::pair<lseqType, leveldb::Status>;
using pureReplyValue = std::tuple<lseqType, leveldb::Status, valueType>;
using batchValues = std::vector<std::tuple<lseqType, keyType, valueType>>;
using replyBatchFormat = std::pair<leveldb::Status, batchValues>;


class dbConnector {
public:

    enum class LSEQ_COMPARE {GREATER_EQUAL, GREATER};

    explicit dbConnector(YAMLConfig config);

    dbConnector(const dbConnector&) = delete;

    dbConnector(dbConnector&&) = delete;

    ~dbConnector();

    replyFormat put(std::string key, std::string value);

    replyFormat remove(std::string key);

    pureReplyValue get(std::string key);

    pureReplyValue get(std::string key, int id);

    leveldb::Status putBatch(const batchValues& keyValuePairs);

    replyBatchFormat getByLseq(leveldb::SequenceNumber seq, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    replyBatchFormat getByLseq(std::string lseq, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    replyBatchFormat getValuesForKey(const std::string& key, leveldb::SequenceNumber seq, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    replyBatchFormat getAllValuesForKey(const std::string& key, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    leveldb::SequenceNumber sequenceNumberForReplica(int id);

    static std::string generateGetseqKey(std::string realKey);

    static std::string generateLseqKey(leveldb::SequenceNumber seq, int id);

    static std::string stampedKeyToRealKey(const std::string& stampedKey);

    static std::string generateNormalKey(std::string key, int id);

    static std::string idToString(int id);

    static std::string lseqToReplicaId(const std::string& lseq);

    static leveldb::SequenceNumber lseqToSeq(const std::string& lseq);

    static std::string seqToString(leveldb::SequenceNumber seq);

protected:

    leveldb::SequenceNumber getMaxSeqForReplica(int id);

private:
    static_assert(std::is_same_v<leveldb::SequenceNumber, uint64_t>, "Refusing to build with different underlying sequence number");
    std::vector<std::atomic<leveldb::SequenceNumber>> seqCount;
    leveldb::DB* db{};

    int selfId;

};