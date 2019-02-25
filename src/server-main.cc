#include <iostream>
#include <thread>
#include <sysexits.h>
#include <stdlib.h>

#include "inih/INIReader.h"
#include "rpc/server.h"

#include "logger.hpp"
#include "SurfStoreServer.hpp"

using namespace std;

int main(int argc, char **argv)
{
    initLogging();
    auto log = logger();

    // Handle the command-line argument
    if (argc != 2)
    {
        cerr << "Usage: " << argv[0] << " [config_file]" << endl;
        return EX_USAGE;
    }

    // Read in the configuration file
    INIReader config(argv[1]);

    if (config.ParseError() < 0)
    {
        cerr << "Error parsing config file " << argv[1] << endl;
        return EX_CONFIG;
    }

    if (config.GetBoolean("ssd", "enabled", true))
    {
        log->info("Surfstore server enabled");
        SurfStoreServer *ssd = new SurfStoreServer(config);
        ssd->launch();
    }
    else
    {
        log->info("SurfStore server disabled");
    }

    return 0;
}
