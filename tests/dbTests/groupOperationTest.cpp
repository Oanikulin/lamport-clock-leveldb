#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <tuple>

#include "src/utils/yamlConfig.hpp"
#include "src/utils/grpcConfig.hpp"
#include "src/db/dbConnector.hpp"
#include "leveldb/db.h"

class groupOperationTest : public ::testing::Test {
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

TEST_F(groupOperationTest, baseGroupInsert) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(12, 1), dbConnector::generateNormalKey("ab", 1), "val"},
        {dbConnector::generateLseqKey(15, 1), dbConnector::generateNormalKey("ab2", 1), "val2"},
        {dbConnector::generateLseqKey(16, 1), dbConnector::generateNormalKey("abc", 1), "valc"},
        {dbConnector::generateLseqKey(19, 3), dbConnector::generateNormalKey("ab", 3), "val3"}
    }).ok());
    EXPECT_EQ(db.get("ab", 1).value, "val");
    EXPECT_EQ(db.get("ab2", 1).value, "val2");
    EXPECT_EQ(db.get("abc", 1).value, "valc");
    EXPECT_EQ(db.get("ab", 3).value, "val3");
    EXPECT_EQ(16, db.sequenceNumberForReplica(1));
    EXPECT_EQ(19, db.sequenceNumberForReplica(3));
    EXPECT_TRUE(db.get("ab", 2).response_status.IsNotFound());
}

TEST_F(groupOperationTest, lseqSeekNormalPut) {
    std::string firstLseq = db.put("valuekey", "valuevalue").lseq;

    replyBatchFormat repl = db.getByLseq(dbConnector::lseqToSeq(firstLseq), 2, 1);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 1);
    EXPECT_EQ(repl.values[0].value, "valuevalue");

    repl = db.getByLseq(firstLseq, 1);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 1);
    EXPECT_EQ(repl.values[0].value, "valuevalue");

    repl = db.getByLseq(firstLseq);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_GE(repl.values.size(), 1);
    EXPECT_EQ(repl.values[0].value, "valuevalue");

    std::string secondLseq = db.put("valuekey2", "valuevalue2").lseq;

    repl = db.getByLseq(dbConnector::lseqToSeq(firstLseq), 2, 2);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 2);
    EXPECT_EQ(repl.values[0].value, "valuevalue");
    EXPECT_EQ(repl.values[1].value, "valuevalue2");

    repl = db.getByLseq(firstLseq, 2);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 2);
    EXPECT_EQ(repl.values[0].value, "valuevalue");
    EXPECT_EQ(repl.values[1].value, "valuevalue2");

    repl = db.getByLseq(secondLseq, 1);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 1);
    EXPECT_EQ(repl.values[0].value, "valuevalue2");

    repl = db.getByLseq(0, 2, 1);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_GE(repl.values.size(), 2);
}

TEST_F(groupOperationTest, lseqSeek) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(100, 2), dbConnector::generateNormalKey("abc", 2), "val"},
        {dbConnector::generateLseqKey(200, 2), dbConnector::generateNormalKey("abc2", 2), "val2"},
        {dbConnector::generateLseqKey(300, 2), dbConnector::generateNormalKey("abcc", 2), "val3"},
        {dbConnector::generateLseqKey(400, 2), dbConnector::generateNormalKey("abcd", 2), "val4"}
    }).ok());
    replyBatchFormat repl = db.getByLseq(100, 2);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 4);
    EXPECT_EQ(repl.values[0].value, "val");
    EXPECT_EQ(repl.values[1].value, "val2");
    EXPECT_EQ(repl.values[2].value, "val3");
    EXPECT_EQ(repl.values[3].value, "val4");

    repl = db.getByLseq(101, 2);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 3);
    EXPECT_EQ(repl.values[0].value, "val2");
    EXPECT_EQ(repl.values[1].value, "val3");
    EXPECT_EQ(repl.values[2].value, "val4");

    repl = db.getByLseq(100, 2, -1, dbConnector::LSEQ_COMPARE::GREATER);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 3);
    EXPECT_EQ(repl.values[0].value, "val2");
    EXPECT_EQ(repl.values[1].value, "val3");
    EXPECT_EQ(repl.values[2].value, "val4");

    repl = db.getByLseq(100, 1);
    EXPECT_TRUE(repl.response_status.ok());
    for (auto res : repl.values) {
        std::cerr <<res.lseq << " " << res.key << " " << res.value << std::endl;
    }
    EXPECT_EQ(repl.values.size(), 0);
}

TEST_F(groupOperationTest, groupKeyGet) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(1000, 2), dbConnector::generateNormalKey("abcde", 2), "val"},
        {dbConnector::generateLseqKey(2000, 2), dbConnector::generateNormalKey("abcde", 2), "val2"},
        {dbConnector::generateLseqKey(3000, 2), dbConnector::generateNormalKey("abcde", 2), "val3"},
        {dbConnector::generateLseqKey(1200, 2), dbConnector::generateNormalKey("abcf", 2), "val4"},
        {dbConnector::generateLseqKey(1500, 3), dbConnector::generateNormalKey("abcde", 2), "val5"}
    }).ok());

    replyBatchFormat repl = db.getAllValuesForKey("abcde", 0);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 4);
    EXPECT_EQ(repl.values[0].value, "val");
    EXPECT_EQ(repl.values[1].value, "val5");
    EXPECT_EQ(repl.values[2].value, "val2");
    EXPECT_EQ(repl.values[3].value, "val3");

    repl = db.getValuesForKey("abcde", 1001, 12);
    EXPECT_TRUE(repl.response_status.ok());
    EXPECT_EQ(repl.values.size(), 3);
    EXPECT_EQ(repl.values[0].value, "val5");
    EXPECT_EQ(repl.values[1].value, "val2");
    EXPECT_EQ(repl.values[2].value, "val3");
}