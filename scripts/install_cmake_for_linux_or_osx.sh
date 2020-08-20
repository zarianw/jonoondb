#!/bin/bash

set -x
set -e

rm -rf cmake-3.18.1
if [ ! -e cmake-3.18.1 ]; then
  wget https://github.com/Kitware/CMake/releases/download/v3.18.1/cmake-3.18.1.tar.gz
  tar xzf cmake-3.18.1.tar.gz > cmake_log.out
fi

cd cmake-3.18.1
./configure >> cmake_log.out
sudo make install -j 2 >> cmake_log.out
cd ..
