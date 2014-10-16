#pragma once

#include <cstddef>

namespace jonoondb_api
{
		class Options
		{
		public:
			//Default constructor that sets all the options to their default value
			Options();
			Options(bool createDBIfMissing, size_t maxDataFileSize, bool compressionEnabled, bool synchronous);
			Options(Options&& other);

			~Options();

			void SetCreateDBIfMissing(bool value);
			bool GetCreateDBIfMissing() const;

			void SetCompressionEnabled(bool value);
			bool GetCompressionEnabled() const;
			
			void SetMaxDataFileSize(size_t value);
			size_t GetMaxDataFileSize() const;		

			void SetSynchronous(bool value);
			bool GetSynchronous() const;
			
		private:
			struct OptionsData;
			OptionsData* m_optionsData;
		};

} // jonoondb_api
