#include <string>
#include "gtest/gtest.h"
#include "status.h"

using namespace std;
using namespace jonoondb_api;

TEST(Status, Constructor1) {
  Status status;
  ASSERT_STREQ(status.GetMessage(), "OK");
  ASSERT_TRUE(status.OK());
}

TEST(Status, Constructor2) {
  string errorMsg = "Error Message.";
  Status status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}

TEST(Status, CopyConstructor) {
  string errorMsg = "Error Message.";
  Status status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);

  //Now copy the object
  Status status2(status);

  //Now verify both objects
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());

  ASSERT_STREQ(status2.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status2.InvalidArgument());
}

TEST(Status, AssignmentOperator) {
  string errorMsg = "Error Message.";
  Status status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);

  //Do the assignment
  Status status2;
  status2 = status;

  //Now verify both objects
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());

  ASSERT_STREQ(status2.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status2.InvalidArgument());
}

TEST(Status, MoveAssignmentOperator) {
  string errorMsg = "Error Message.";

  Status status;
  //Assign Status from a rvalue object
  status = Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                  __FILE__, "", __LINE__);

  //Now verify
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}

Status GetStatusRValue(const string& errorMsg) {
  Status sts = Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                      __FILE__, "", __LINE__);

  return sts;
}

TEST(Status, MoveConstructor) {
  string errorMsg = "Error Message.";
  //Construct Status from a rvalue object			
  Status status = GetStatusRValue(errorMsg);

  //Now verify
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}
