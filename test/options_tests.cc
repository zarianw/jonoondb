#include "gtest/gtest.h"
#include "database.h"

using namespace jonoondb_api;

TEST(Options, Ctor_Deafult) {
  Options opt;
  ASSERT_FALSE(opt.GetCompressionEnabled());
  ASSERT_TRUE(opt.GetCreateDBIfMissing());
  ASSERT_TRUE(opt.GetSynchronous()); 
}

TEST(Options, Copy_Ctor) {
  Options opt1;
  opt1.SetMaxDataFileSize(12345);
  Options opt2(opt1);  
  ASSERT_EQ(opt1.GetCompressionEnabled(), opt2.GetCompressionEnabled());
  ASSERT_EQ(opt1.GetCreateDBIfMissing(), opt2.GetCreateDBIfMissing());
  ASSERT_EQ(opt1.GetSynchronous(), opt2.GetSynchronous());
  ASSERT_EQ(opt1.GetMaxDataFileSize(), opt2.GetMaxDataFileSize());
}

TEST(Options, Copy_Assignment) {
  Options opt1;
  opt1.SetMaxDataFileSize(12345);
  Options opt2;
  opt2 = opt1;
  ASSERT_EQ(opt1.GetCompressionEnabled(), opt2.GetCompressionEnabled());
  ASSERT_EQ(opt1.GetCreateDBIfMissing(), opt2.GetCreateDBIfMissing());
  ASSERT_EQ(opt1.GetSynchronous(), opt2.GetSynchronous());
  ASSERT_EQ(opt1.GetMaxDataFileSize(), opt2.GetMaxDataFileSize());
}

TEST(Options, Move_Ctor) {
  Options opt1;
  opt1.SetMaxDataFileSize(12345);
  auto compression = opt1.GetCompressionEnabled();
  auto creeateDB = opt1.GetCreateDBIfMissing();
  auto sync = opt1.GetSynchronous();
  auto maxSize = opt1.GetMaxDataFileSize();

  Options opt2(std::move(opt1));
  ASSERT_EQ(opt2.GetCompressionEnabled(), compression);
  ASSERT_EQ(opt2.GetCreateDBIfMissing(), creeateDB);
  ASSERT_EQ(opt2.GetSynchronous(), sync);
  ASSERT_EQ(opt2.GetMaxDataFileSize(), maxSize);
}

TEST(Options, Move_Assignment) {
  Options opt1;
  opt1.SetMaxDataFileSize(12345);
  auto compression = opt1.GetCompressionEnabled();
  auto creeateDB = opt1.GetCreateDBIfMissing();
  auto sync = opt1.GetSynchronous();
  auto maxSize = opt1.GetMaxDataFileSize();

  Options opt2;
  opt2 = std::move(opt1);
  ASSERT_EQ(opt2.GetCompressionEnabled(), compression);
  ASSERT_EQ(opt2.GetCreateDBIfMissing(), creeateDB);
  ASSERT_EQ(opt2.GetSynchronous(), sync);
  ASSERT_EQ(opt2.GetMaxDataFileSize(), maxSize);
}