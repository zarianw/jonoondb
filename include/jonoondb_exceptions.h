#pragma once

#include <exception>
#include <string>

namespace jonoondb_api {

class JonoonDBException : public std::exception {
public:
  JonoonDBException() : m_lineNum(0) {}
  JonoonDBException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) : m_message(msg),
    m_fileName(srcFileName), m_funcName(funcName), m_lineNum(lineNum) {
  }

  // Todo Add noexcept for these functions once we move to vs 2015
  const char* what() const throw() { return m_message.c_str(); }
  const char* GetSourceFileName() const { return m_fileName.c_str(); }
  const char* GetFunctionName() const { return m_funcName.c_str(); }
  std::size_t GetLineNumber() const { return m_lineNum; }
private:
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
};


class MissingDatabaseFileException : public JonoonDBException {
public:
  MissingDatabaseFileException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class MissingDatabaseFolderException : public JonoonDBException {
public:
  MissingDatabaseFolderException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class OutOfMemoryException : public JonoonDBException {
public:
  OutOfMemoryException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class DuplicateKeyException : public JonoonDBException {
public:
  DuplicateKeyException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class CollectionAlreadyExistException : public JonoonDBException {
public:
  CollectionAlreadyExistException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class IndexAlreadyExistException : public JonoonDBException {
public:
  IndexAlreadyExistException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class CollectionNotFoundException : public JonoonDBException {
public:
  CollectionNotFoundException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class SchemaParseException : public JonoonDBException {
public:
  SchemaParseException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class IndexOutOfBoundException : public JonoonDBException {
public:
  IndexOutOfBoundException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class SQLException : public JonoonDBException {
public:
  SQLException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

class FileIOException : public JonoonDBException {
public:
  FileIOException(const std::string& msg, const std::string& srcFileName,
    const std::string& funcName, std::size_t lineNum) :
    JonoonDBException(msg, srcFileName, funcName, lineNum) {
  }
};

} // jonoondb_api
