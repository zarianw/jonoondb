## JonoonDB
Database for developers by developers.

JonoonDB is a reliable persistent document store written in C++. It is currently under active development. The key features will include:

* State of the art indexing technology.
* Extreme performance.
* Modular design. We can't claim to be the database for the developers if JonoonDB is not hackable. You can provide custom implementations for the core components in the database.
* SQL support for querying documents.

## Development Workflow
All active development happens on the feature branches. Changes from feature branches are merged into develop branch after feature completion. At the end of each iteration develop branch is merged into master branch after further review and testing. Our philosophy is that master is the golden copy and should always be deployable.

## Supported Operating Systems
JonoonDB is available and supported only as a 64bit library. JonoonDB is supported on the following operating systems:

* Windows
* Linux
* MacOS X

## Supported Compilers
JonoonDB requires a C++ compiler that atleast supports C++14 standard. It has been tested with the following compilers:

* MSVC VS2015 SP2
* GCC 5.2.1
* Clang (Default version on OS X El Capitan). Should work with a version that atleast supports C++14 e.g Clang 3.4 and above.


## Build Instructions
Before building JonoonDB, you have to build/install its 3rd party dependencies.

### Build/Install 3rd party dependencies
1. Download Boost version 1.60.0 from [Boost's website](http://www.boost.org). Unpack\Unzip the downloaded boost release. On the command line, go to the root of the unpacked tree. Run either .\bootstrap.bat (on Windows), or ./bootstrap.sh (on Linux operating systems). Next execute the following command to build boost in 64bit.

    ```
    b2 install address-model=64 --prefix=64bit (On Windows)
    ./b2 install address-model=64 --prefix=64bit (On Linux and MacOS X)
    ```
  
2. Download and install cmake version 3.5 or higher from [cmake's website](http://www.cmake.org/download/). 

### Clone and build JonoonDB
Assuming your install directories for 3rd Parties are as follows:

Boost = D:/software/boost_1_60_0/64bit

You are all set to clone and build JonoonDB. Using cmake you can generate project files for Visual Studio, Eclipse CDT and other IDEs. After that you can build using your IDE of choice. You can also generate makefile if you don't want to use IDEs.

#### Building from IDE
Execute the commands given below to clone and generate the solution file for VS 2015:
```sh
git clone https://<your_username>@bitbucket.org/zarianw/jonoondb.git
cd jonoondb/
mkdir build
cd build
cmake .. -G "Visual Studio 14 Win64" -Dgtest_force_shared_crt=ON -DBOOST_ROOT=D:/software/boost_1_60_0/64bit
```

Please note that -G "Visual Studio 14 Win64" command line argument shown above generates the files for VS 2015 with 64bit build configuration. Inorder to generate project files for Eclipse CDT4 on Linux use -G "Eclipse CDT4 - Unix Makefiles". You can list all the available cmake generators by typing cmake --help.

#### Building from terminal on Linux
```sh
git clone https://<your_username>@bitbucket.org/zarianw/jonoondb.git
cd jonoondb/
mkdir build
cd build
cmake .. -G "Unix Makefiles" -Dgtest_force_shared_crt=ON -DBOOST_ROOT=/path/to/boost
make
```

If you want to run unittests from terminal then you can type "ctest -V" from inside the build directory.

## C++ style guide 
We use [google's c++ style guide](https://google.github.io/styleguide/cppguide.html) for the most part. The places where we differ are documented below:

1. Instance/Member variables should being with "m_". It works with intellisense better as you are trying to use the member variables.
2. Variable names such as parameters, local function variables, instance variables should use camelCase instead of snake_case.
3. Struct variable are named exactly like class variables except that they don't have "m_" prefix.
4. Prefer using C++ headers for C functions and types e.g. prefer <cstring> over <string.h>, <cstdint> over <stdint.h> etc.
5. Use #pragma once instead of header guards because all major compilers support it.
6. We use exceptions for error handling instead of error codes.