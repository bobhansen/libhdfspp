/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//Expose a pure C interface that doesn't need any C++/C++11 header files
//todo: Right now this assumes all "void *stream" parameters are hdfs::InputStreams. 
//      Because of this we can get the InputStream via static_cast<InputStream*>().
//      In order to have a stable ABI all streams should have a base class that
//      can be static_casted to and then typechecked/downcast.
//todo: This is assuming that the program calling into this only needs to connect
//      to one hadoop file system.  This is done to keep things simple and speed up
//      implementation for testing.  hdfs_fs_init should really be returning a pointer
//      to all state related to that file system.

#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

#include "libhdfs++/chdfs.h"
#include "libhdfs++/hdfs.h"
#include <sstream>  //for logs
#include <fstream>  //for logs


#include <iostream>
#include <string>
#include <thread>




void log(const std::string& str) {
  #ifdef CHDFS_LOGGING
  std::fstream fstr;
  fstr.open("/home/jclampffer/Desktop/chdfs.log", std::fstream::out | std::fstream::app);
  fstr << str << std::endl;
  fstr.close();
  #endif
}




//---------------------------------------------------------------------------------------
//  Wrap C++ calls in C
//---------------------------------------------------------------------------------------

using namespace hdfs;


void *call_run(void *servicePtr) {
  IoService *wrappedService = reinterpret_cast<IoService*>(servicePtr);
  log("background thread about to call run");
  wrappedService->Run();
  return NULL;
}

class Executor {
public:
  Executor() {
    //Create a new IoService object. This wraps the boost io_service object.
    io_service_ = std::unique_ptr<IoService>(IoService::New());
    //Call run on IoService object in a background thread, the run call should never return.
    int ret = pthread_create(&processing_thread, NULL, call_run, reinterpret_cast<void*>(io_service_.get()));
    
    //log for debug
    std::stringstream ss;
    ss << "background thread should be started, pthread_create returned " << ret << " and thread id is " << (unsigned long long)processing_thread;
    log(ss.str());

    if(ret != 0) {
      throw std::runtime_error("unable to start pthread?");
    }
  }

  ~Executor() {
    //stop IoService event loop and background thread.  Don't need this yet
    log("~Executor called, this should not be happening in vertica yet.");
  }
 
  IoService *io_service() {
    return io_service_.get();
  }

  std::unique_ptr<IoService> io_service_;
  pthread_t processing_thread;  
};



struct hdfsFile_struct {
  hdfsFile_struct() : position(0), inputStream(NULL) {};
  hdfsFile_struct(InputStream *is) : position(0), inputStream(is) {};
  virtual ~hdfsFile_struct() {
    if(NULL != inputStream)
      delete inputStream;
  }

  size_t position; //unused for now
  InputStream *inputStream;
};


struct hdfsFS_struct {
  hdfsFS_struct() : fileSystem(NULL), backgroundIoService(NULL) {};
  hdfsFS_struct(FileSystem *fs, Executor *ex) : fileSystem(fs), backgroundIoService(ex) {};
  virtual ~hdfsFS_struct() {
    delete fileSystem;
    delete backgroundIoService;
  };
 
  FileSystem *fileSystem;
  Executor *backgroundIoService;
};


/*  FS initialization routine
 *    -need to start background thread(s) to run asio::io_service
 *    -connect to specified namenode host:port
 *    -this will need to be extended to allow application to control memory management routines
 *     by passing an allocator/deleter pair as well as specify io_service thread count 
 */
hdfsFS hdfsConnect(const char *nnhost, unsigned short nnport) {
  //start io_service
  std::unique_ptr<Executor> background_io_service = std::unique_ptr<Executor>(new Executor());

  if(NULL == background_io_service) {
    return NULL;
  }

  //connect to NN, fileSystem will be set on success
  FileSystem *fileSystem = NULL;
  Status stat = FileSystem::New(background_io_service->io_service(), nnhost, nnport, &fileSystem);
  if(!stat.ok()){
    return NULL;
  }

  std::stringstream ss;
  ss << "hdfsConnect called. host=" << nnhost << " port=" << nnport << "addressof filesystem = " << reinterpret_cast<unsigned long long>(fileSystem);
  log(ss.str());

  //make a hdfsFS handle
  return new hdfsFS_struct(fileSystem, background_io_service.release());
}


int hdfsDisconnect(hdfsFS fs) {
  //likely other stuff to free up, this is mostly just a stub for basic tests
  if(NULL != fs)
    delete fs;  
  else
    return -1;
  return 0;
}


hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, int blockSize) {
  //the following four params are just placeholders until write path is finished
  (void)flags;
  (void)bufferSize;
  (void)replication;
  (void)blockSize;

  //assuming we want to do a read with default settings, sufficient for now
  InputStream *isPtr = NULL;
  Status stat = fs->fileSystem->Open(path, &isPtr);
  if(!stat.ok()) {
    return NULL;
  }

  return new hdfsFile_struct(isPtr);
}


int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  (void)fs;
  if(NULL != file)
    delete file;
  else
    return -1;
  return 0;
}


size_t hdfsPread(hdfsFS fs, hdfsFile file, off_t position, void *buf, size_t length) {
  if(NULL == fs || NULL == file)
    return -1;

  size_t readBytes = 0;
  Status stat = file->inputStream->PositionRead(buf, length, position, &readBytes);
  if(!stat.ok()) {
    return -1;
  }

  return readBytes;
}


