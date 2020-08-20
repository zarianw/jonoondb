#!/bin/bash

set -x
set -e

wget https://github.com/Kitware/CMake/releases/download/v3.18.1/cmake-3.18.1-Linux-x86_64.tar.gz
tar xzf cmake-3.18.1-Linux-x86_64.tar.gz > cmake_log.out
mv cmake-3.18.1-Linux-x86_64 cmake-install
PATH=$(pwd)/cmake-install:$(pwd)/cmake-install/bin:$PATH