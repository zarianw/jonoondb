## JonoonDB
Database for developers by developers.

JonoonDB is a reliable persistent document store written in C++. It is currently under active development. The key features will include:

* State of the art indexing technology.
* Extreme performance.
* Modular design. We can't claim to be the database for the developers if JonoonDB is not hackable. You can provide custom implementations for the core components in the database.
* SQL support for querying documents.

## Development Workflow
All active development happens on the develop branch. Changes from develop are merged into master branch after further review and testing. Our philosophy is that master is the golden copy and should always be deployable. If you plan to make contributions then fork the develop branch and submit pull requests on that branch.

## Build Instructions

JonoonDB uses features from C++ 11. So make sure your C++ compiler supports C++ 11. JonoonDB has been testd with MSVC VS2013 compiler on Windows 7 and g++ version 4.8 compiler on Ubuntu 14.04. Before building JonoonDB, you have to build its 3rd party dependencies.

### Build 3rd party dependencies
1. Download Boost version 1.54.0 or higher from [Boost's website](http://www.boost.org) and install it by using the instructions provided at their [installation page](http://www.boost.org/doc/libs/1_55_0/doc/html/bbv2/installation.html).
2. Download and install cmake version 2.8.0 or higher from [cmake's website](http://www.cmake.org/download/). 
3. Download flatbuffers from their [Github page](https://github.com/google/flatbuffers) and build it by following the instrutcions given at [flatbuffers build page](http://google.github.io/flatbuffers/md__building.html).
4. Install Google Test version 1.7.0 or higher by following the instructions at [Google Test's website](https://code.google.com/p/googletest/)

### Clone and build JonoonDB
Assuming your install directories for 3rd Parties are as follows:

Boost = D:/software/boost_1_54_0, GoogleTest = E:/code/gtest-1.7.0, Flatbuffers = E:/code/flatbuffers

You are all set to clone and build JonoonDB. Using cmake you can generate project files for Visual Studio, Eclipse CDT and other IDEs. After that you can build using your IDE of choice. Execute the commands given below to clone and generate the solution file for VS 2013:

```sh
git clone https://github.com/zarianw/jonoondb.git jonoondb -b master
cd jonoondb/
cmake -G "Visual Studio 12" -DGTEST_PATH=E:/code/gtest-1.7.0 -Dgtest_force_shared_crt=ON -DBOOST_ROOT=D:/software/boost_1_54_0 -DFLATBUFFERS_PATH=E:/code/flatbuffers
```

Please note that -G "Visual Studio 12" command line argument shown above generates the files for VS 2013. Inorder to generate project files for Eclipse CDT4 use -G "Eclipse CDT4 - Unix Makefiles". You can list all the available cmake generators by typing cmake --help.
