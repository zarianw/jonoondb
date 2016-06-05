## JonoonDB
It's a row store...It's a column store...NO! It's JonoonDB.

Please visit [JonoonDB's Project Page](http://zarianw.github.io/jonoondb) for more information.

## Supported Operating Systems
JonoonDB is available and supported only as a 64bit library. JonoonDB is supported on the following operating systems:

* Windows
* Linux
* MacOS X

## Supported Compilers
JonoonDB requires a C++ compiler that atleast supports C++14 standard. It should work with the following compilers:

* MSVC VS2015 SP2 and above
* GCC 5.2.1 and above
* Clang 3.4 and above.

## Build Instructions
Before building JonoonDB, you have to build/install its 3rd party dependencies.

### Build/Install 3rd party dependencies
1. Download and install cmake version 3.5 or higher from [cmake's website](http://www.cmake.org/download/). 

2. Download Boost version 1.60.0 from [Boost's website](http://www.boost.org). Unpack\Unzip the downloaded boost release. On the command line, go to the root of the unpacked tree. Next execute the following commands to build boost in 64bit.

```
(On Windows)
bootstrap.bat
b2 install address-model=64 --prefix=64bit
(On Linux and Mac OS X)
./bootstrap.sh
./b2 install address-model=64 --prefix=64bit
```

### Clone and build JonoonDB
Assuming your install directories for 3rd Parties are as follows:

Boost = D:/software/boost_1_60_0/64bit

You are all set to clone and build JonoonDB. Using cmake you can generate project files for Visual Studio, Eclipse CDT and other IDEs. After that you can build using your IDE of choice. You can also generate makefile if you don't want to use IDEs.

#### Building from IDE
Execute the commands given below to clone and generate the solution file for VS 2015:
```sh
git clone https://github.com/zarianw/jonoondb.git
cd jonoondb/
mkdir build
cd build
cmake .. -G "Visual Studio 14 Win64" -Dgtest_force_shared_crt=ON -DBOOST_ROOT=D:/software/boost_1_60_0/64bit
```

Please note that -G "Visual Studio 14 Win64" command line argument shown above generates the files for VS 2015 with 64bit build configuration. Inorder to generate project files for Eclipse CDT4 on Linux use -G "Eclipse CDT4 - Unix Makefiles". You can list all the available cmake generators by typing cmake --help.

#### Building from terminal on Linux
```sh
git clone https://github.com/zarianw/jonoondb.git
cd jonoondb/
mkdir build
cd build
cmake .. -G "Unix Makefiles" -Dgtest_force_shared_crt=ON -DBOOST_ROOT=/path/to/boost
make
```

If you want to run unittests from terminal then you can type "ctest -V" from inside the build directory.
