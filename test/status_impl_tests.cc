#include <string>
#include "gtest/gtest.h"
#include "status_impl.h"

using namespace std;
using namespace jonoondb_api;

TEST(StatusImpl, Constructor1) {
  StatusImpl status;
  ASSERT_STREQ(status.GetMessage(), "OK");
  ASSERT_TRUE(status.OK());
}

TEST(StatusImpl, Constructor2) {
  string errorMsg = "Error Message.";
  StatusImpl status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}

TEST(StatusImpl, CopyConstructor) {
  string errorMsg = "Error Message.";
  StatusImpl status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);

  //Now copy the object
  StatusImpl status2(status);

  //Now verify both objects
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());

  ASSERT_STREQ(status2.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status2.InvalidArgument());
}

TEST(StatusImpl, AssignmentOperator) {
  string errorMsg = "Error Message.";
  StatusImpl status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                __FILE__, "", __LINE__);

  //Do the assignment
  StatusImpl status2;
  status2 = status;

  //Now verify both objects
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());

  ASSERT_STREQ(status2.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status2.InvalidArgument());
}

TEST(StatusImpl, MoveAssignmentOperator) {
  string errorMsg = "Error Message.";

  StatusImpl status;
  //Assign StatusImpl from a rvalue object
  status = StatusImpl(kStatusInvalidArgumentCode, errorMsg.c_str(),
                  __FILE__, "", __LINE__);

  //Now verify
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}

StatusImpl GetStatusRValue(const string& errorMsg) {
  StatusImpl sts = StatusImpl(kStatusInvalidArgumentCode, errorMsg.c_str(),
                      __FILE__, "", __LINE__);

  return sts;
}

TEST(StatusImpl, MoveConstructor) {
  string errorMsg = "Error Message.";
  //Construct StatusImpl from a rvalue object			
  StatusImpl status = GetStatusRValue(errorMsg);

  //Now verify
  ASSERT_STREQ(status.GetMessage(), errorMsg.c_str());
  ASSERT_TRUE(status.InvalidArgument());
}

TEST(StatusImpl, GetFunctionName) {
  StatusImpl sts(kStatusInvalidArgumentCode, "check", __FILE__, "FunctionName",
            __LINE__);    
  ASSERT_STREQ("FunctionName", sts.GetFunctionName());
}

TEST(StatusImpl, GetLineNumber) {
  StatusImpl sts(kStatusInvalidArgumentCode, "get", __FILE__, "LineNumber", 86);  
  ASSERT_EQ(86, sts.GetLineNumber());  
}

TEST(StatusImpl, GenericError) {
  StatusImpl sts(kStatusGenericErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.GenericError());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.GenericError());      
}

TEST(StatusImpl, MissingDatabaseFile) {
  StatusImpl sts(kStatusMissingDatabaseFileCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.MissingDatabaseFile());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.MissingDatabaseFile());
}

TEST(StatusImpl, MissingDatabaseFolder) {
  StatusImpl sts(kStatusMissingDatabaseFolderCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.MissingDatabaseFolder());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.MissingDatabaseFolder());
}

TEST(StatusImpl, InvalidArgument) {
  StatusImpl sts(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.InvalidArgument());

  StatusImpl sts2(kStatusInvalidOperationCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.InvalidArgument());
}

TEST(StatusImpl, FailedToOpenMetadataDatabaseFile) {
  StatusImpl sts(kStatusFailedToOpenMetadataDatabaseFileCode, "IS", __FILE__,
    "OK", __LINE__);
  ASSERT_TRUE(sts.FailedToOpenMetadataDatabaseFile());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.FailedToOpenMetadataDatabaseFile());
}

TEST(StatusImpl, OutOfMemoryError) {
  StatusImpl sts(kStatusOutOfMemoryErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.OutOfMemoryError());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.OutOfMemoryError());
}

TEST(StatusImpl, DuplicateKeyError) {
  StatusImpl sts(kStatusDuplicateKeyErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.DuplicateKeyError());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.DuplicateKeyError());
}

TEST(StatusImpl, KeyNotFound) {
  StatusImpl sts(kStatusKeyNotFoundCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.KeyNotFound());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.KeyNotFound());
}

TEST(StatusImpl, FileIOError) {
  StatusImpl sts(kStatusFileIOErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.FileIOError());
  
  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.FileIOError());
}

TEST(StatusImpl, APIMisuseError) {
  StatusImpl sts(kStatusAPIMisuseErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.APIMisuseError());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.APIMisuseError());
}

TEST(StatusImpl, CollectionAlreadyExist) {
  StatusImpl sts(kStatusCollectionAlreadyExistCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.CollectionAlreadyExist());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.CollectionAlreadyExist());
}

TEST(StatusImpl, IndexAlreadyExist) {
  StatusImpl sts(kStatusIndexAlreadyExistCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.IndexAlreadyExist());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.IndexAlreadyExist());
}

TEST(StatusImpl, CollectionNotFound) {
  StatusImpl sts(kStatusCollectionNotFoundCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.CollectionNotFound());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.CollectionNotFound());
}


TEST(StatusImpl, SchemaParseError) {
  StatusImpl sts(kStatusSchemaParseErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.SchemaParseError());

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.SchemaParseError());
}

TEST(StatusImpl, IndexOutOfBound) {
  StatusImpl sts(kStatusIndexOutOfBoundErrorCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_TRUE(sts.IndexOutOfBound());      

  StatusImpl sts2(kStatusInvalidArgumentCode, "IS", __FILE__, "OK", __LINE__);
  ASSERT_FALSE(sts2.IndexOutOfBound());
}


TEST(StatusImpl, GetCode){
  //test starting with error code 1  and go onward to test all the errors
  string errorMsg = "Error Message.";

  StatusImpl status1(kStatusGenericErrorCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_EQ(status1.GetCode(), kStatusGenericErrorCode);


  StatusImpl status2(kStatusInvalidArgumentCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_EQ(status2.GetCode(), kStatusInvalidArgumentCode);

  StatusImpl status3(kStatusMissingDatabaseFileCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_EQ(status3.GetCode(), kStatusMissingDatabaseFileCode);
}

TEST(StatusImpl, NotOperatorOnEmptyConstructor) {
  StatusImpl status;
  // on empty params OK is true so the negation will result in false
  ASSERT_FALSE(!status);
}

TEST(StatusImpl, NotOperatorOnNonEmptyConstructor) {
  string errorMsg = "Error Message.";
  StatusImpl status(kStatusInvalidArgumentCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  // on non empty params OK is false as there is some error,
  // so the negation will return true
  ASSERT_TRUE(!status);
}

TEST(StatusImpl, GetMessage) {
  string errorMsg = "Error Message.";

  StatusImpl status1(kStatusGenericErrorCode, errorMsg.c_str(),
    __FILE__, "", __LINE__);
  ASSERT_STREQ(status1.GetMessage(), errorMsg.c_str());

  //This is the case where status is OK and returns OK
  // when get message is called i.e. empty constructor
  StatusImpl status2;
  ASSERT_STREQ(status2.GetMessage(), "OK");
}

TEST(StatusImpl, GetSourceFileNameOnSomeValidData) {
  string errorMsg = "Error Message.";
  string dummySourceFileName = "/home/user/my/db";

  StatusImpl status1(kStatusGenericErrorCode, errorMsg.c_str(),
    dummySourceFileName.c_str(), "", __LINE__);
  ASSERT_STREQ(status1.GetSourceFileName(), "/home/user/my/db");
}

TEST(StatusImpl, GetSourceFileNameOnEmptyData) {
  StatusImpl status1;
  // should be empty as m_statusdata is nullptr
  ASSERT_STREQ(status1.GetSourceFileName(), "");
}
