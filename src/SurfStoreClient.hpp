#ifndef SURFSTORECLIENT_HPP
#define SURFSTORECLIENT_HPP

#include <string>
#include <list>

#include "inih/INIReader.h"
#include "rpc/client.h"

#include "logger.hpp"
#include "SurfStoreTypes.hpp"

using namespace std;

class SurfStoreClient
{
  public:
    SurfStoreClient(INIReader &t_config);
    ~SurfStoreClient();

    void sync(); // sync the base_dir with the cloud

    const uint64_t RPC_TIMEOUT = 100; // milliseconds

  protected:
    INIReader &config;
    string serverhost;
    int serverport;
    string base_dir;
    int blocksize;

    rpc::client *c;

    // helper functions to get/set from the local index file
    FileInfo get_local_fileinfo(string filename);
    void set_local_fileinfo(string filename, FileInfo finfo);

    // helper functions to get/set blocks to/from local files
    list<string> get_blocks_from_file(string filename);
    void create_file_from_blocklist(string filename, list<string>& blocks);
    void create_file_from_hashlist(string filename, list<string>& hashlist);
    void remote2local(string remote_filename, list<string>& remote_hashlist, int remotev);
    void upload_data(string filename, list<string>& hashlist);
};

#endif // SURFSTORECLIENT_HPP
