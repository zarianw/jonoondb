#pragma once

#include <exception>
#include <string>
#include <sstream>

namespace jonoondb_api {

class JonoonDBException : public std::exception {
public:
  JonoonDBException() : m_lineNum(0) {}
  JonoonDBException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) : m_message(msg),
    m_fileName(srcFileName), m_funcName(funcName), m_lineNum(lineNum) {
  }

  const char* what() const noexcept { return m_message.c_str(); }
  const char* GetSourceFileName() const { return m_fileName.c_str(); }
  const char* GetFunctionName() const { return m_funcName.c_str(); }
  std::size_t GetLineNumber() const { return m_lineNum; }
  std::string to_string() {
    std::ostringstream ss;
    ss << "ExceptionType: " << GetType() << ", Message: " << m_message << ", SourceFile: " << m_fileName
      << ", Function: " << m_funcName << ", Line: " << m_lineNum << ".";

    return ss.str();
  }
  
private:
  virtual std::string GetType() {
    return "JonoonDBException";
  }
  std::string m_message;
  std::string m_fileName;
  std::string m_funcName;
  size_t m_lineNum;
  
};

class InvalidArgumentException : public JonoonDBException {
public:
  InvalidArgumentException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) : 
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "InvalidArgumentException";
  }
};


class MissingDatabaseFileException : public JonoonDBException {
public:
  MissingDatabaseFileException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "MissingDatabaseFileException";
  }
};

class MissingDatabaseFolderException : public JonoonDBException {
public:
  MissingDatabaseFolderException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "MissingDatabaseFolderException";
  }
};

class OutOfMemoryException : public JonoonDBException {
public:
  OutOfMemoryException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "OutOfMemoryException";
  }
};

class DuplicateKeyException : public JonoonDBException {
public:
  DuplicateKeyException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "DuplicateKeyException";
  }
};

class CollectionAlreadyExistException : public JonoonDBException {
public:
  CollectionAlreadyExistException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "CollectionAlreadyExistException";
  }
};

class IndexAlreadyExistException : public JonoonDBException {
public:
  IndexAlreadyExistException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "IndexAlreadyExistException";
  }
};

class CollectionNotFoundException : public JonoonDBException {
public:
  CollectionNotFoundException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "CollectionNotFoundException";
  }
};

class SchemaParseException : public JonoonDBException {
public:
  SchemaParseException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "SchemaParseException";
  }
};

class IndexOutOfBoundException : public JonoonDBException {
public:
  IndexOutOfBoundException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "IndexOutOfBoundException";
  }
};

class SQLException : public JonoonDBException {
public:
  SQLException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "SQLException";
  }
};

class FileIOException : public JonoonDBException {
public:
  FileIOException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "FileIOException";
  }
};

class MissingDocumentException : public JonoonDBException {
public:
  MissingDocumentException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
private:
  virtual std::string GetType() override {
    return "MissingDocumentException";
  }
};

} // jonoondb_api
