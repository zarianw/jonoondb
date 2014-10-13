# Build Instructions

JonoonDB uses features from C++ 11. So make sure your C++ compiler supports C++ 11. JonoonDB has been testd with MSVC VS2013 compiler on Windows 7 and g++ version 4.8 compiler on Ubuntu 14.04. Before building JonoonDB, you have to build its 3rd party dependencies.

## Build 3rd party dependencies
1. Download Boost version 1.55.0 or higher from [Boost's website](http://www.boost.org) and install it by using the instructions provided at their [installation page](http://www.boost.org/doc/libs/1_55_0/doc/html/bbv2/installation.html).
2. Download and install cmake version 3.0.0 or higher from [cmake's website](http://www.cmake.org/download/). 
3. Download flatbuffers from their [Github page](https://github.com/google/flatbuffers) and build it by following the instrutcions given at [flatbuffers build page](http://google.github.io/flatbuffers/md__building.html).
4. Install Google Test version 1.7.0 or higher by following the instructions at [Google Test's website](https://code.google.com/p/googletest/)

## Clone and build JonoonDB
Assuming your install directories for 3rd Parties are as follows:

Boost = D:/Software/boost_1_55_0

GoogleTest = E:/code/gtest-1.7.0

Flatbuffers = E:/code/flatbuffers

Now you are all set to clone and build JonoonDB. Using cmake you can generate project files for Visual Studio, Eclipse CDT and other IDEs. After that you can build using your IDE of choice. Execute the commands given below:


```sh
git clone https://github.com/zarianw/jonoondb.git jonoondb
cd jonoondb/build
cmake -DGTEST_PATH=E:/code/gtest-1.7.0 CMakeLists.txt -Dgtest_force_shared_crt=ON -DBOOST_ROOT=D:/Software/boost_1_55_0 -DFLATBUFFERS_PATH=E:/code/flatbuffers
```
