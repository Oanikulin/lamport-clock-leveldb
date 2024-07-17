#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>
#include <tuple>
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