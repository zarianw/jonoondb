#!/bin/bash

set -x
set -e
$(./bootstrap.sh --with-libraries=filesystem,system,thread,chrono,program_options >> boost.out && ./b2 install cxxflags="-fPIC" cflags="-fPIC" address-model=64 --prefix=64bit -j 2 >> boost.out)
