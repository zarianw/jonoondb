#include "options.h"
#include "constants.h"

using namespace jonoondb_api;

struct Options::OptionsData
{
	bool CreateDBIfMissing;	
	size_t MaxDataFileSize;
	bool CompressionEnabled;	
	bool Synchronous;
};

Options::Options()
{
	m_optionsData = new OptionsData();
	m_optionsData->CreateDBIfMissing = false;
	m_optionsData->MaxDataFileSize = MAX_DATA_FILE_SIZE;
	m_optionsData->CompressionEnabled = true;
	m_optionsData->Synchronous = true;
}

Options::Options(Options&& other)
{
	if(this != &other)
	{
		m_optionsData = other.m_optionsData;
		other.m_optionsData = nullptr;
	}
}

Options::Options(bool createDBIfMissing, size_t maxDataFileSize, bool compressionEnabled, bool synchronous)
{
	m_optionsData = new OptionsData();
	m_optionsData->CreateDBIfMissing = createDBIfMissing;	
	m_optionsData->MaxDataFileSize = maxDataFileSize;
	m_optionsData->CompressionEnabled = compressionEnabled;
	m_optionsData->Synchronous = synchronous;
}

Options::~Options()
{
	delete m_optionsData;
	m_optionsData = nullptr;
}

void Options::SetCreateDBIfMissing(bool value)
{
	m_optionsData->CreateDBIfMissing = value;
}

bool Options::GetCreateDBIfMissing() const
{
	return m_optionsData->CreateDBIfMissing;
}

void Options::SetCompressionEnabled(bool value)
{
	m_optionsData->CompressionEnabled = value;
}

bool Options::GetCompressionEnabled() const
{
	return m_optionsData->CompressionEnabled;
}

void Options::SetMaxDataFileSize(size_t value)
{
	m_optionsData->MaxDataFileSize = value;
}

size_t Options::GetMaxDataFileSize() const
{
	return m_optionsData->MaxDataFileSize;
}

void Options::SetSynchronous(bool value)
{
	m_optionsData->Synchronous = value;
}

bool Options::GetSynchronous() const
{
	return m_optionsData->Synchronous;
}