# Mini_dropBox

## Dependencies

This project relies on four dependencies:

* inih
  * A header-only config file handle (https://github.com/jtilly/inih)
* spdlog
  * A fast thread-safe logging library (https://github.com/gabime/spdlog)
* rpclib
  * A simple c++ RPC library (http://rpclib.net/)
* picosha2
  * The PicoSHA2 SHA-256 hash generator (https://github.com/okdshin/PicoSHA2)

## environment prep

You must use the same compiler to build the rpc library as you use
for the rest of your project, otherwise you will get a series of errors.
Thus to start working on the project, you need to re-build the rpc
library.

To rebuild the library, do the following:

1. cd dependencies/src
1. tar -xzf cmake-3.13.2-Linux-x86_64.tar.gz
1. tar -xzf rpclib.tar.gz
1. cd rpclib
1. mkdir build
1. cd build
1. ../../cmake-3.13.2-Linux-x86_64/bin/cmake ..
1. ../../cmake-3.13.2-Linux-x86_64/bin/cmake --build .
  * (note the dot after --build)
1. cp librpc.a ../../../lib/

OK you should be set.  Proceed to testing your starter code.


## Usage

Run server in a window

* ./ssd myconfig.ini

In another window, run the client:

* ./ss myconfig.ini

## Ref article:
http://storageconference.us/2010/Papers/MSST/Shvachko.pdf



