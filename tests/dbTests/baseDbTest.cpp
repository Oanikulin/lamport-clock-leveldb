#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <filesystem>

#include "src/utils/yamlConfig.hpp"
#include "src/utils/grpcConfig.hpp"
#include "src/db/dbConnector.hpp"
#include "leveldb/db.h"

class baseDbTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string fileName = config.getDbFile();
        std::filesystem::remove_all(fileName);
    }

    void TearDown() override {
        std::string fileName = config.getDbFile();
        std::filesystem::remove_all(fileName);
    }

    YAMLConfig config = YAMLConfig("resources/config.yaml");
    dbConnector db = dbConnector(config);
};

TEST_F(baseDbTest, baseCorrectness) {
    EXPECT_TRUE(db.put("testval", "testdata").response_status.ok());

    //get should return same seq for same values
    EXPECT_EQ(db.get("testval").lseq, db.get("testval").lseq);

    //get on nonexistent key
    EXPECT_TRUE(db.get("asghsqag").response_status.IsNotFound());
}

TEST_F(baseDbTest, putSeqResult) {
    std::string firstLseq = db.put("aasgas", "bsagas").lseq;
    EXPECT_EQ(dbConnector::lseqToSeq(firstLseq), db.sequenceNumberForReplica(2));
    std::string secondLseq = db.put("gsagaa", "bsagas").lseq;
    EXPECT_EQ(dbConnector::lseqToSeq(secondLseq), db.sequenceNumberForReplica(2));
    EXPECT_TRUE(firstLseq < secondLseq);
}

TEST_F(baseDbTest, putsAndGets) {
    EXPECT_TRUE(db.put("a", "b").response_status.ok());
    EXPECT_TRUE(db.get("a").response_status.ok());
    EXPECT_EQ(db.get("a").value, "b");
    EXPECT_TRUE(db.put("c", "d").response_status.ok());

    EXPECT_EQ(db.get("a").value, "b");
    EXPECT_EQ(db.get("c").value, "d");
}

TEST_F(baseDbTest, putSameValues) {
    EXPECT_TRUE(db.put("a2", "b2").response_status.ok());
    EXPECT_EQ(db.get("a2").value, "b2");
    EXPECT_TRUE(db.put("a2", "d2").response_status.ok());

    EXPECT_EQ(db.get("a2").value, "d2");
}

TEST_F(baseDbTest, putAndDelete) {
    EXPECT_TRUE(db.put("a3", "b3").response_status.ok());
    EXPECT_EQ(db.get("a3").value, "b3");
    EXPECT_TRUE(db.remove("a3").response_status.ok());

    EXPECT_TRUE(db.get("a3").response_status.IsNotFound());

    EXPECT_TRUE(db.put("a3", "c3").response_status.ok());
    EXPECT_EQ(db.get("a3").value, "c3");
}

TEST_F(baseDbTest, getLSEQKey) {
    std::string lseq = db.put("a4", "b4").lseq;
    EXPECT_EQ(db.get("a4").lseq, lseq);

    lseq = db.put("a4", "c4").lseq;
    EXPECT_EQ(db.get("a4").lseq, lseq);
}

TEST_F(baseDbTest, multithreadPutsAndGets) {
    constexpr int kKeyPerThreadCount = 100;
    constexpr int kThreadCount = 8;
    constexpr int kKeyCount = kKeyPerThreadCount * kThreadCount;

    std::vector<std::string> keys(kKeyCount);
    std::vector<std::string> values(kKeyCount);
    std::vector<std::string> lseqs(kKeyCount);
    for (int i = 0; i < kKeyCount; ++i) {
        keys[i] = std::to_string(i);
        values[i] = std::to_string(2 * i);
    }
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < kThreadCount; ++i) {
        std::thread writer([this, i, &keys, &values, &lseqs]() {
            for (int j = i * kKeyPerThreadCount; j < (i + 1) * kKeyPerThreadCount; ++j) {
                auto put_result = db.put(keys[j], values[j]);
                EXPECT_TRUE(put_result.response_status.ok());
                lseqs[j] = put_result.lseq;
            }
        });
        writer_threads.emplace_back(std::move(writer));
    }

    for (auto& thread : writer_threads) {
        thread.join();
    }

    std::vector<std::thread> reader_threads;
    for (int i = 0; i < kThreadCount; ++i) {
        std::thread reader([this, i, &keys, &values, &lseqs]() {
            for (int j = i * kKeyPerThreadCount; j < (i + 1) * kKeyPerThreadCount; ++j) {
                auto get_result = db.get(keys[j]);
                EXPECT_EQ(get_result.lseq, lseqs[j]);
                EXPECT_TRUE(get_result.response_status.ok());
                EXPECT_EQ(get_result.value, values[j]);
            }
        });
        reader_threads.emplace_back(std::move(reader));
    }

    for (auto& thread : reader_threads) {
        thread.join();
    }
}

TEST_F(baseDbTest, multithreadRemovalsAndGets) {
    constexpr int kKeyPerThreadCount = 100;
    constexpr int kThreadCount = 8;
    constexpr int kKeyCount = kKeyPerThreadCount * kThreadCount;

    std::vector<std::string> keys(kKeyCount);
    std::vector<std::string> values(kKeyCount);
    std::vector<std::string> lseqs(kKeyCount);
    for (int i = 0; i < kKeyCount; ++i) {
        keys[i] = "ff" + std::to_string(i);
        values[i] = "ff" + std::to_string(2 * i);
        auto put_result = db.put(keys[i], values[i]);
        EXPECT_TRUE(put_result.response_status.ok());
        lseqs[i] = put_result.lseq;
    }
    std::vector<std::thread> remove_threads;
    for (int i = 0; i < kThreadCount; ++i) {
        std::thread remover([this, i, &keys, &values, &lseqs]() {
            for (int j = i * kKeyPerThreadCount; j < (i + 1) * kKeyPerThreadCount; j += 2) {
                auto remove_result = db.remove(keys[j]);
                EXPECT_TRUE(remove_result.response_status.ok());
            }
        });
        remove_threads.emplace_back(std::move(remover));
    }

    for (auto& thread : remove_threads) {
        thread.join();
    }

    bool is_expected = false;
    for (int i = 0; i < kKeyCount; ++i) {
        auto get_result = db.get(keys[i]);
        if (is_expected) {
            if (!get_result.response_status.ok()) {
                std::cerr << get_result.response_status.ToString() << std::endl;
            }
            EXPECT_TRUE(get_result.response_status.ok());
            EXPECT_EQ(get_result.lseq, lseqs[i]);
            EXPECT_EQ(get_result.value, values[i]);
        } else {
            EXPECT_TRUE(get_result.response_status.IsNotFound());
        }
        is_expected ^= (true);
    }
}
