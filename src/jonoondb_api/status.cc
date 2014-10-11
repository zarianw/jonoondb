#include <string>
#include "status.h"

using namespace std;

using namespace jonoondb_api;

Status::Status()
{
	m_statusData = nullptr;
}

Status::Status(const Status& other)
{
	if(this != &other)
	{
		m_statusData = nullptr;

		if(other.m_statusData)
		{
			size_t size;
			memcpy(&size, other.m_statusData, sizeof(size_t));
			m_statusData = new char[size];

			memcpy(m_statusData, other.m_statusData, size);
		}		
	}
}

Status::Status(Status&& other)
{
	if(this != &other)
	{
		m_statusData = other.m_statusData;
		
		other.m_statusData = nullptr;
	}
}

Status::Status(const char code, const char* message, const size_t messageLength)
{
	size_t sizetLengthInBytes = sizeof(size_t);
	size_t size = sizetLengthInBytes + 1 + messageLength + 1; //StatusDataSize + Code + Message + NullCharacter
	m_statusData = new char[size];

	memcpy(m_statusData, &size, sizetLengthInBytes);
	memcpy(m_statusData + sizetLengthInBytes, &code, 1);
	memcpy(m_statusData + sizetLengthInBytes + 1, message, messageLength + 1);		
}

Status& Status::operator=(Status&& other)
{
	if(this != &other)
	{
		m_statusData = other.m_statusData;
		
		other.m_statusData = nullptr;
	}

	return *this;
}

Status& Status::operator=(const Status& other)
{
	if(this != &other)
	{
		delete m_statusData;
		m_statusData = nullptr;

		if(other.m_statusData)
		{
			size_t size;
			memcpy(&size, other.m_statusData, sizeof(size_t));
			m_statusData = new char[size];

			memcpy(m_statusData, other.m_statusData, size);	
		}			
	}

	return *this;
}

Status::~Status()
{
	delete m_statusData;
	m_statusData = nullptr;
}

bool Status::OK() const
{
	if(m_statusData == nullptr)
	{
		return true;
	}

	return false;
}

bool Status::InvalidArgument() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::InvalidArgumentCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::MissingDatabaseFile() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::MissingDatabaseFileCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::MissingDatabaseFolder() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::MissingDatabaseFolderCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::FailedToOpenMetadataDatabaseFile() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::FailedToOpenMetadataDatabaseFileCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::OutOfMemoryError() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::OutOfMemoryErrorCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::DuplicateKeyError() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::DuplicateKeyErrorCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::GenericError() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::GenericErrorCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::KeyNotFound() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::KeyNotFoundCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::FileIOError() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::FileIOErrorCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::APIMisuseError() const
{
	if(m_statusData != nullptr)
	{
		//Read Code
		if(!memcmp(m_statusData + sizeof(size_t), &Status::APIMisuseErrorCode, 1))
		{
			return true; 
		}		
	}

	return false;
}

bool Status::CollectionAlreadyExist() const
{
  if (m_statusData != nullptr)
  {
    //Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &Status::CollectionAlreadyExistCode, 1))
    {
      return true;
    }
  }

  return false;
}

bool Status::IndexAlreadyExist() const
{
  if (m_statusData != nullptr)
  {
    //Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &Status::IndexAlreadyExistCode, 1))
    {
      return true;
    }
  }

  return false;
}

const char* Status::c_str() const
{
	if(m_statusData == nullptr)
	{
		static const string OKStr = "OK";
		return OKStr.c_str();
	}

	return &m_statusData[sizeof(size_t) + 1];
}