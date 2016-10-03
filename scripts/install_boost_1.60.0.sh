set -x
set -e
rm -rf boost_1_60_0
if [ ! -e boost_1_60_0 ]; then
  wget https://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz
  tar xzf boost_1_60_0.tar.gz > boost.out
  $(cd boost_1_60_0 && ./bootstrap.sh --with-libraries=filesystem,system,thread,chrono,program_options >> boost.out && ./b2 install address-model=64 --prefix=64bit >> boost.out)
fi

