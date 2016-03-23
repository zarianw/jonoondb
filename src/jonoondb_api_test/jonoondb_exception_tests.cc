#include <string>
#include "gtest/gtest.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;

TEST(JonoonDBException, DefaultConstructor) {
  JonoonDBException ex;
  ASSERT_STREQ(ex.what(), "");
  ASSERT_STREQ(ex.GetFunctionName(), "");
  ASSERT_STREQ(ex.GetSourceFileName(), "");
  ASSERT_EQ(ex.GetLineNumber(), 0);
}

TEST(JonoonDBException, Constructor2) {
  JonoonDBException ex("Error Message.", "srcfile", "funcName", 10);
  ASSERT_STREQ(ex.what(), "Error Message.");
  ASSERT_STREQ(ex.GetFunctionName(), "funcName");
  ASSERT_STREQ(ex.GetSourceFileName(), "srcfile");
  ASSERT_EQ(ex.GetLineNumber(), 10);
}

TEST(JonoonDBException, CopyConstructor) {
  JonoonDBException ex("Error Message.", "srcfile", "funcName", 10);

  //Now copy the object
  JonoonDBException ex2(ex);

  //Now verify both objects
  ASSERT_STREQ(ex.what(), ex2.what());
  ASSERT_STREQ(ex.GetFunctionName(), ex2.GetFunctionName());
  ASSERT_STREQ(ex.GetSourceFileName(), ex2.GetSourceFileName());
  ASSERT_EQ(ex.GetLineNumber(), ex2.GetLineNumber());
}

TEST(JonoonDBException, AssignmentOperator) {
  JonoonDBException ex("Error Message.", "srcfile", "funcName", 10);

  //Do the assignment
  JonoonDBException ex2;
  ex2 = ex;

  //Now verify both objects
  ASSERT_STREQ(ex.what(), ex2.what());
  ASSERT_STREQ(ex.GetFunctionName(), ex2.GetFunctionName());
  ASSERT_STREQ(ex.GetSourceFileName(), ex2.GetSourceFileName());
  ASSERT_EQ(ex.GetLineNumber(), ex2.GetLineNumber());
}

TEST(JonoonDBException, MoveAssignmentOperator) {
  JonoonDBException ex;
  //Assign ex from a rvalue object
  ex = JonoonDBException("Error Message.", "srcfile", "funcName", 10);
  ASSERT_STREQ(ex.what(), "Error Message.");
  ASSERT_STREQ(ex.GetFunctionName(), "funcName");
  ASSERT_STREQ(ex.GetSourceFileName(), "srcfile");
  ASSERT_EQ(ex.GetLineNumber(), 10);
}

JonoonDBException GetStatusRValue() {
  auto ex = JonoonDBException("Error Message.", "srcfile", "funcName", 10);
  return ex;
}

TEST(JonoonDBException, MoveConstructor) {
  //Construct ex from a rvalue object			
  JonoonDBException ex = GetStatusRValue();

  //Now verify
  ASSERT_STREQ(ex.what(), "Error Message.");
  ASSERT_STREQ(ex.GetFunctionName(), "funcName");
  ASSERT_STREQ(ex.GetSourceFileName(), "srcfile");
  ASSERT_EQ(ex.GetLineNumber(), 10);
}

TEST(JonoonDBException, SQLException) {
  //Construct ex from a rvalue object			
  try {
    throw SQLException("Error Message.", "srcfile", "funcName", 10);
  } catch (JonoonDBException& ex) {
    // we should get the exception here as JonoonDBException is the base class
    //Now verify
    ASSERT_STREQ(ex.what(), "Error Message.");
    ASSERT_STREQ(ex.GetFunctionName(), "funcName");
    ASSERT_STREQ(ex.GetSourceFileName(), "srcfile");
    ASSERT_EQ(ex.GetLineNumber(), 10);
  }
}