#pragma once

#include <iomanip>
#include <sstream>
#include <string>

#include "leveldb/db.h"

class FullKey {
public:
    static constexpr int kSeqNumberLength = 15;
    static constexpr int kReplicaIdLength = 10;

    FullKey(const std::string& fullKey) : key(fullKey) {
        keyLength = static_cast<int>(fullKey.size()) - 1 - kReplicaIdLength - kSeqNumberLength;
        raw_key = key.substr(1, keyLength);
    }

    FullKey(const std::string& key, leveldb::SequenceNumber seq, int id) {
        std::stringstream ss;
        ss << std::setw(kSeqNumberLength) << std::setfill('0') << seq;
        ss << std::setw(kReplicaIdLength) << std::setfill('0') << id;
        this->key = std::string("!") + key + ss.str();
        keyLength = key.size();
        raw_key = key;
    }

    const std::string& getFullKey() const {
        return key;
    }

    const std::string& getKey() const {
        return raw_key;
    }

    leveldb::SequenceNumber getSeq() const {
        return std::stoll(key.substr(1 + keyLength, kSeqNumberLength));
    }

    int getReplicaId() const {
        return std::stoi(key.substr(1 + keyLength + kSeqNumberLength));
    }

private:

    std::string key;
    std::string raw_key;
    int keyLength;
};
