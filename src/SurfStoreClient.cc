#include <sysexits.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <list>
#include <algorithm>
#include <fstream>
#include <map>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>

#include <sys/types.h>
#include <dirent.h>
#include <assert.h>

#include "rpc/server.h"
#include "picosha2/picosha2.h"

#include "logger.hpp"
#include "SurfStoreTypes.hpp"
#include "SurfStoreClient.hpp"

//for setting up a timer to simulate uploading a largefile  
#include <chrono>
#include <thread>

using namespace std;

bool fileExists(const char* filename) {
    struct stat buf;
    return (stat(filename, &buf) == 0);
}

const list<string> DELETED_HASHLIST = { "0" };

// constructor to set up a server using the config file 
SurfStoreClient::SurfStoreClient(INIReader &t_config)
    : config(t_config), c(nullptr)
{
    auto log = logger();

    // pull the address and port for the server
    string serverconf = config.Get("ssd", "server", "");
    if (serverconf == "")
    {
        log->error("The server was not found in the config file");
        exit(EX_CONFIG);
    }
    size_t idx = serverconf.find(":");
    if (idx == string::npos)
    {
        log->error("Config line {} is invalid", serverconf);
        exit(EX_CONFIG);
    }
    serverhost = serverconf.substr(0, idx);
    serverport = strtol(serverconf.substr(idx + 1).c_str(), nullptr, 0);
    if (serverport <= 0 || serverport > 65535)
    {
        log->error("The port provided is invalid: {}", serverconf);
        exit(EX_CONFIG);
    }

    base_dir = config.Get("ss", "base_dir", "");
    blocksize = config.GetInteger("ss", "blocksize", 4096);

    log->info("Launching SurfStore client");
    log->info("Server host: {}", serverhost);
    log->info("Server port: {}", serverport);

    c = new rpc::client(serverhost, serverport);
}

// destructor
SurfStoreClient::~SurfStoreClient()
{
    if (c)
    {
        delete c;
        c = nullptr;
    }
}

// client calls sync() to sync files to server, serverl conditions might occurs
/*
    1   client creates new files/ modify files, server files not changed, upload all files 
    2   clinet creates new files/ modify files, server files changed, abandon local changes, download files from the server
    3   client no changes, server files changed, download files from the server
*/ 
void SurfStoreClient::sync()
{
    auto log = logger();
    log->info("====== scanning local files in directory {} ======", base_dir);

    DIR* dirp = opendir(base_dir.c_str());
    struct dirent * dp;
    map<string,list<string>> newfile_hashmap, modfile_hashmap; // keep track of files there are either new or modified

    // The client should first scan the base directory
    while ((dp = readdir(dirp)) != NULL) {
        string filename = dp->d_name;

        // skip index.txt and any file starting with .
        if (filename == "index.txt" || filename[0] == '.') { continue; }

        list<string> new_hashlist;   // create a hashlist for each file
        list<string> blocks = get_blocks_from_file(filename);

        // for each file, compute that file’s hash list.
        // The last buffer less than the buffer size?
        for (const string& block : blocks) {
            string blockhash = picosha2::hash256_hex_string(block);
            new_hashlist.push_back(blockhash);
        }

        // The client should then consult the local index file and compare the results,
        // to see whether (1) there are now new files in the base directory
        // that aren’t in the index file, or (2) files that are in the index file,
        // but have changed since the last time the client was executed
        // (i.e., the hash list is different).
        FileInfo local_index = get_local_fileinfo(filename); // consult local index file
        int localv = get<0>(local_index);
        list<string> local_hashlist = get<1>(local_index);

        if (localv == -1) { // new file do not exist in local index file or fail to open the index file
            newfile_hashmap[filename] = new_hashlist; // new files in the base directory that aren’t in the index file
        } else {
            // files that are in the index file, but have changed since the last
            // time the client was executed
            if (local_hashlist != new_hashlist) { // '=' for equality checking. See https://stackoverflow.com/a/16422594
                modfile_hashmap[filename] = new_hashlist;
            }
        } // end if (localv == -1)
    } // end while ((dp = readdir(dirp)) != NULL)
    closedir(dirp);
    log->info("====== finish scanning local files in directory {} ======", base_dir);

    // Next, the client should connect to the server and download an updated FileInfoMap.
    // For the purposes of this discussion, let’s call this the “remote index.”
    log->info("====== connect to the server and download an updated FileInfoMap ======");
    FileInfoMap remote_index = c->call("get_fileinfo_map").as<FileInfoMap>();

    // The client should now compare the local index (and any changes to local
    // files not reflected in the local index) with the remote index. A few things might result.
    for(const auto& key_val : remote_index){
        //get the file name of the remote_index
        string remote_filename = key_val.first;
        FileInfo remote_fileinfo = key_val.second; // a tuple
        FileInfo local_index = get_local_fileinfo(remote_filename); // the local fileinfo of the file name
        int remotev = get<0>(remote_fileinfo);
        int localv = get<0>(local_index);
        list<string> remote_hashlist = get<1>(remote_fileinfo);
        list<string> local_hashlist = get<1>(local_index); // hash list of the filename

        // First, it is possible that the remote index refers to a file
        // not present in the local index or in the base directory.
        if(localv == -1){
            // In this case, the client should download the blocks associated
            // with that file, reconstitute that file in the base directory, and
            // add the updated FileInfo information to the local index.
            remote2local(remote_filename, remote_hashlist, remotev);
        } else { 

            // locally deleted fil 
            // filename entry exists in both remote and local index
            if (!fileExists( (base_dir + "/" + remote_filename).c_str())) { 
                // To represent a “tombstone” record, we will set the file’s
                // hash list to a single hash value of “0” (zero).
                int newv = localv + 1;
                FileInfo new_finfo = make_tuple(newv, DELETED_HASHLIST);
                bool success = c->call("update_file", remote_filename, new_finfo).as<bool>();

                if (success) { // upload local index if delete request succcesses
                    set_local_fileinfo(remote_filename, new_finfo);
                } else { 

                    //download
                    remote2local(remote_filename, remote_hashlist, remotev);
                }
            }
           
            //file not modifed , remote version > local version

            // Imagine that for a file like cat.jpg, the local index shows that file
            // at version 3, and we compare the hash list in the local index with
            // the file contents, and confirm that there are no local modifications
            // to the file. We then look at the remote index, and see that the
            // version on the server is larger, for example 4.
            else if (modfile_hashmap.find(remote_filename) == modfile_hashmap.end()) { 
                if (remotev > localv) {
                    // In this case, the client should download any needed
                    // blocks from the server to bring cat.jpg up to version 4,
                    // then reconstitute cat.jpg to become version 4 of that file,
                    // and finally the client should update its local index,
                    // brining that entry to version 4. At this point, the changes from
                    // the cloud have been merged into the local file.
                    
                    //download
                    remote2local(remote_filename, remote_hashlist, remotev);
                }
            } 


            //file locally modified , try to upload block first
            // if change FileInfoMap but still uploading, then other client can not download 
            else { 

                // Consider the opposite case: the client sees that its local index
                // references cat.jpg with version 3. The client compares the hash list
                // in the local index to the file contents, and sees that there are
                // uncommitted local changes (the hashes differ). The client compares
                // the local index to the remote index, and sees that both indexes are
                // at the same version (in this case, version 3).
                if (remotev == localv) { // file both exists in remote and local
                    // This means that we need to sync our local changes to the cloud.
                    list<string>& modfile_hashlist = modfile_hashmap[remote_filename];
                    upload_data(remote_filename, modfile_hashlist);
                    // The client can now update the mapping on the server
                    int newv = localv + 1;
                    FileInfo new_finfo = make_tuple(newv, modfile_hashlist);
                    bool success = c->call("update_file", remote_filename, new_finfo).as<bool>(); // update the server with the new FileInfo.
                    // if that RPC call completes successfully, the client can
                    // update the entry in the local index and is done (there is
                    // no need to modify the file’s contents in the base directory
                    // in this case).
                    
                    //check if upload success, 

                    if (success) { 
                        //upload success
                        set_local_fileinfo(remote_filename, new_finfo);
                    }
                    else { 
                        // same version but different content. *conflict*
                        remote2local(remote_filename, remote_hashlist, remotev);
                        continue;
                    }
                } // end if (remotev == localv)

                // Finally, we must consider the case where there are local modifications
                // to a file (so, for example, our local index shows the file at version 3,
                // but the file’s contents do not match the local index). Further, we see
                // that the version in the remote index is larger than our local index.
                if (remotev > localv ) {
                    // In this case, we follow the rule that whoever
                    // syncs to the cloud first wins. Thus, we must go with the version of the
                    // file alrady syncd to the server. So we download any required blocks
                    // and bring our local version of the file up to date with the cloud version.
                    
                    //download and ovewrite
                    remote2local(remote_filename, remote_hashlist, remotev);
                }
            } // end if (modfile_hashmap.find(remote_filename) == modfile_hashmap.end())
        } // end if (localv == -1)
    } // end for(const auto& key_val : remote_index)

    // Next, it is possible that there are new files in the local base
    // directory that aren’t in the local index or in the remote index.
    log->info("====== uploading new files ======");
    for (auto const& kv : newfile_hashmap) {
        string new_filename = kv.first;
        list<string> new_hashlist = kv.second;

        log->info("upload filename {}",new_filename);
        for (const string& hash:new_hashlist ) {
            log->info("hash value is {}", hash);
        }

        // The client should upload the blocks corresponding to this file to the server,
        // then update the server with the new FileInfo.
        upload_data(new_filename, new_hashlist);

        // To create a file that has never existed, use the update\_file() API call with a version number set to 1.
        FileInfo new_finfo = make_tuple(1, new_hashlist);
        bool success = c->call("update_file", new_filename, new_finfo).as<bool>(); // update the server with the new FileInfo.

        // If that update is successful, then the client should update its local index.
        if (success) { set_local_fileinfo(new_filename, new_finfo); }
        else {
            // Note it is possible that while this operation is in progress,
            // some other client makes it to the server first, and creates the file
            // first. In that case, the update\_file() operation will fail
            // with a version error, and the client should handle this conflict
            // as described in the next section.
            remote_index = c->call("get_fileinfo_map").as<FileInfoMap>();
            FileInfo remote_overwrite_finfo = remote_index[new_filename];
            remote2local(new_filename, get<1>(remote_overwrite_finfo), get<0>(remote_overwrite_finfo)); // TODO: check this out!
        } // end if (success)
    } // end for (auto const& kv : newfile_hashmap)
}

FileInfo SurfStoreClient::get_local_fileinfo(string filename)
{
    auto log = logger();
    log->info("get_local_fileinfo {}", filename);
    ifstream f(base_dir + "/index.txt");
    if (f.fail())
    {
        int v = -1;
        list<string> blocklist;
        FileInfo ret = make_tuple(v, list<string>());
        return ret;
    }
    do
    {
        vector<string> parts;
        string x;
        getline(f, x);
        stringstream ss(x);
        string tok;
        while (getline(ss, tok, ' '))
        {
            parts.push_back(tok);
        }
        if (parts.size() > 0 && parts[0] == filename)
        {
            list<string> hl(parts.begin() + 2, parts.end());
            int v = stoi(parts[1]);
            return make_tuple(v, hl);
        }
    } while (!f.eof());
    int v = -1;
    list<string> blocklist;
    FileInfo ret = make_tuple(v, list<string>());
    return ret;
}

void SurfStoreClient::set_local_fileinfo(string filename, FileInfo finfo)
{
    auto log = logger();
    log->info("set local file info");
    std::ifstream f(base_dir + "/index.txt");
    std::ofstream out(base_dir + "/index.txt.new");
    int v = get<0>(finfo);
    list<string> hl = get<1>(finfo);
    bool set = false;
    do
    {
        string x;
        vector<string> parts;
        getline(f, x);
        stringstream ss(x);
        string tok;
        while (getline(ss, tok, ' '))
        {
            parts.push_back(tok);
        }
        if (parts.size() > 0)
        {
            if (parts[0] == filename)
            {
                set = true;
                out << filename << " " << v << " ";
                for (auto it : hl)
                    out << it << " ";
                out.seekp(-1, ios_base::cur);
                out << "\n";
            }
            else
            {
                out << x << "\n";
            }
        }
        else
            break;
    } while (!f.eof());
    if (!set)
    {
        out << filename << " " << v << " ";
        for (auto it : hl)
            out << it << " ";
        out.seekp(-1, ios_base::cur);
        out << "\n";
    }
    out.close();
    f.close();
    string real = string(base_dir + "/index.txt");
    string bkp = string(base_dir + "/index.txt.new");

    remove(real.c_str());
    rename(bkp.c_str(), real.c_str());
}

//Get the data blocks from the file given by filename
list<string> SurfStoreClient::get_blocks_from_file(string filename) {
    auto log = logger();
    log->info("getting data blocks from file '{}'", filename);

    list<string> blocks;
    ifstream is(base_dir+ "/" + filename, ifstream::binary); // read in file content

    if (!is) {
        // handle file permission error?
        log->error("error reading file '{}'", filename);
        return blocks;
    } // Sanity check: no permission or corrupt file

    char * blockbuffer = new char[blocksize]; // get each chunk of file content, chunksize = blocksize

    while (!is.eof()) {
        is.read(blockbuffer, blocksize); // only read in next blocksize bytes for each block
        string block(blockbuffer, is.gcount()); // support '\0' element in it
        blocks.push_back(block);
    }

    delete blockbuffer;
    return blocks;
}

//form the file in local directory from the a list of blocks
void SurfStoreClient::create_file_from_blocklist(string filename, list<string>& blocks){
  auto log = logger();
  log->info("Reconstituting file '{}'", filename);
  std::ofstream out(base_dir + "/" + filename);

  for (const string& block : blocks) { out << block; }

  out.close();
  log->info("File '{}' reconstitution successful", filename);
}

void SurfStoreClient::create_file_from_hashlist(string filename, list<string>& hashlist){
    auto log = logger();
    log->info("Getting '{}' file blocks from server", filename);

    // delete the file if needed
    if (hashlist == DELETED_HASHLIST) {
        log->info("Deleted file '{}' detected", filename);
        //delete the file if exists
        const char * filepath = (base_dir +"/" +filename).c_str();

        if (fileExists(filepath)) {
            if(remove(filepath) == -1){
                log->error("remove file '{}' failed", filename);
            }
            else{
                log->info("remove file '{}' successfully", filename);
            }
        }
        return;
    }

    // download file blocks
    list<string> blocks;
    for (const string& hash : hashlist) {
        blocks.push_back(c->call("get_block", hash).as<string>());
    }
    create_file_from_blocklist(filename, blocks); // reconstitute the file
}

void SurfStoreClient::remote2local(string remote_filename, list<string>& remote_hashlist, int remotev){
    create_file_from_hashlist(remote_filename, remote_hashlist);
    FileInfo new_finfo = make_tuple(remotev, remote_hashlist);
    set_local_fileinfo(remote_filename, new_finfo); // update local index
}

void SurfStoreClient::upload_data(string filename, list<string>& hashlist){
    auto log = logger();
    log->info("Uploading '{}' file blocks to server", filename);

    list<string> new_blocks = get_blocks_from_file(filename); // same # of entries as new_hashlist

    // iterate through both new_hashlist and new_blocks simultaneously
    auto hashlist_it = hashlist.begin(); // same length as new_blocks
    auto blocks_it = new_blocks.begin(); // same length as hashlist

    // store all blocks via rpc call. See https://stackoverflow.com/a/36260558
    while(hashlist_it != hashlist.end() && blocks_it != new_blocks.end()){
        c->call("store_block", *hashlist_it, *blocks_it);

        //testing delay
        // this_thread::sleep_for(std::chrono::milliseconds(10));
        ++hashlist_it; ++blocks_it;
    }

    log->info("Upload '{}' file complete", filename);
}
