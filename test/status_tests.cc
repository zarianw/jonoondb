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

TEST(Status, GetFunctionName) {
  Status sts(kStatusInvalidArgumentCode, "check", __FILE__, "FunctionName",
            __LINE__);    
  ASSERT_STREQ("FunctionName", sts.GetFunctionName());
}

TEST(Status, GetLineNumber) {
  Status sts(kStatusInvalidArgumentCode, "get", __FILE__, "LineNumber", 86);  
  ASSERT_EQ(86, sts.GetLineNumber());  
}

TEST(Status, GenericError) {
  Status sts(kStatusGenericErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.GenericError());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.GenericError());      
}

TEST(Status, MissingDatabaseFile) {
  Status sts(kStatusMissingDatabaseFileCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.MissingDatabaseFile());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.MissingDatabaseFile());
}

TEST(Status, MissingDatabaseFolder) {
  Status sts(kStatusMissingDatabaseFolderCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.MissingDatabaseFolder());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.MissingDatabaseFolder());
}

TEST(Status, InvalidArgument) {
  Status sts(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.InvalidArgument());

  Status sts2(kStatusInvalidOperationCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.InvalidArgument());
}

TEST(Status, FailedToOpenMetadataDatabaseFile) {
  Status sts(kStatusFailedToOpenMetadataDatabaseFileCode, "IS", __FILE__,
    "OK", __LINE__);
  ASSERT_TRUE(sts.FailedToOpenMetadataDatabaseFile());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.FailedToOpenMetadataDatabaseFile());
}

TEST(Status, OutOfMemoryError) {
  Status sts(kStatusOutOfMemoryErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.OutOfMemoryError());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.OutOfMemoryError());
}

TEST(Status, DuplicateKeyError) {
  Status sts(kStatusDuplicateKeyErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.DuplicateKeyError());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.DuplicateKeyError());
}

TEST(Status, KeyNotFound) {
  Status sts(kStatusKeyNotFoundCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.KeyNotFound());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.KeyNotFound());
}

TEST(Status, FileIOError) {
  Status sts(kStatusFileIOErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.FileIOError());
  
  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.FileIOError());
}

TEST(Status, APIMisuseError) {
  Status sts(kStatusAPIMisuseErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.APIMisuseError());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.APIMisuseError());
}

TEST(Status, CollectionAlreadyExist) {
  Status sts(kStatusCollectionAlreadyExistCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.CollectionAlreadyExist());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.CollectionAlreadyExist());
}

TEST(Status, IndexAlreadyExist) {
  Status sts(kStatusIndexAlreadyExistCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.IndexAlreadyExist());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.IndexAlreadyExist());
}

TEST(Status, CollectionNotFound) {
  Status sts(kStatusCollectionNotFoundCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.CollectionNotFound());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.CollectionNotFound());
}


TEST(Status, SchemaParseError) {
  Status sts(kStatusSchemaParseErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.SchemaParseError());

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.SchemaParseError());
}

TEST(Status, IndexOutOfBound) {
  Status sts(kStatusIndexOutOfBoundErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.IndexOutOfBound());      

  Status sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.IndexOutOfBound());
}


TEST(Status, GetCode){
  //test starting with error code 1  and go onward to test all the errors
  string errorMsg = "Error Message.";

  Status status1(kStatusGenericErrorCode, errorMsg.c_str(),
		__FILE__, "", __LINE__);
  ASSERT_EQ(status1.GetCode(), kStatusGenericErrorCode);


  Status status2(kStatusInvalidArgumentCode, errorMsg.c_str(),
		__FILE__, "", __LINE__);
  ASSERT_EQ(status2.GetCode(), kStatusInvalidArgumentCode);

  Status status3(kStatusMissingDatabaseFileCode, errorMsg.c_str(),
		__FILE__, "", __LINE__);
  ASSERT_EQ(status3.GetCode(), kStatusMissingDatabaseFileCode);
}

TEST(Status, NotOperatorOnEmptyConstructor) {
  Status status;
  ASSERT_FALSE(!status); // on empty params OK is true so the negation will result in false
}

TEST(Status, NotOperatorOnNonEmptyConstructor) {
  string errorMsg = "Error Message.";
  Status status(kStatusInvalidArgumentCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_TRUE(!status); // on non empty params OK is false as there is some error, so the negation will return true
}


TEST(Status, GetMessage) {
  string errorMsg = "Error Message.";

  Status status1(kStatusGenericErrorCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_STREQ(status1.GetMessage(), errorMsg.c_str());

  //This is the case where status is OK and returns OK when get message is called i.e. empt contructor
  Status status2;
  ASSERT_STREQ(status2.GetMessage(), "OK");

}



TEST(Status, GetSourceFileNameOnSomeValidData) {
  string errorMsg = "Error Message.";
  string dummySourceFileName = "/home/user/my/db";

  Status status1(kStatusGenericErrorCode, errorMsg.c_str(),
    dummySourceFileName.c_str(), "", __LINE__);
  ASSERT_STREQ(status1.GetSourceFileName(), "/home/user/my/db");
}

TEST(Status, GetSourceFileNameOnEmptyData) {
  Status status1;
  ASSERT_STREQ(status1.GetSourceFileName(), ""); // should be empty as m_statusdata is nullptr
}
