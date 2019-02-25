#ifndef SURFSTORESERVER_HPP
#define SURFSTORESERVER_HPP

#include "SurfStoreTypes.hpp"
#include "inih/INIReader.h"
#include "logger.hpp"

using namespace std;

class SurfStoreServer
{
  public:
    SurfStoreServer(INIReader &t_config);

    void launch();

    const int NUM_THREADS = 8;

  protected:
    INIReader &config;
    int port;
    FileInfoMap fim;
    HashDataMap hdm;
};

#endif // SURFSTORESERVER_HPP
