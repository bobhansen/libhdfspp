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
#ifndef BLOCK_READER_H_
#define BLOCK_READER_H_

#include "libhdfs++/options.h"
#include "libhdfs++/status.h"
#include "datatransfer.pb.h"

#include <memory>

namespace hdfs {


/*
    LocalBlockReader
    Goals:
        Thread safe.  Should be able to do multiple reads at different offsets on the same
        block reader, or at least make some choices early on to make it easy to do that later.
*/

template <class Stream>
class LocalBlockReader : public std::enable_shared_from_this<LocalBlockReader> {
 public:
  explicit LocalBlockReader(const BlockReaderOptions &options,
                            Stream *stream) 
      : stream_(stream)     //needed to request short circuit fds
      , options_(options)   //verify_checksum, cache_stratagy, encryption_scheme
      , data_in_(-1)        //todo: make sure -1 is the right default value for uninitialized files
      , meta_in_(-1)
  {}

  Status connect(const std::string &client_name,
                const hadoop::common::TokenProto *token,
                const hadoop::hdfs::ExtendedBlockProto *block);
                //Don't really need, or want, length and offset here.  This should be much easier
                //to make thread safe than the remote reader because we can just do preads on
                //the same set of descriptors instead of opening more.

  void async_connect(const std::string &client_name,
                     const hadoop::common::TokenProto *token,
                     const hadoop::hdfs::Ext)

  template<class MutableBufferSequence>
  size_t read_some(const MutableBufferSequence &buffers, Status *status);

  template<class MutableBufferSequence, class ReadHandler>
  size_t async_read_some(const MutableBufferSequence &buffers, const ReadHandler &handler);

  enum State {
    kOpen,                     
    kRequestFileDescriptors,  
    kReadFromDataFile,
    kReadFromMetaFile,
    kChecksumData,
    kReleaseFileDescriptors          
  }

  State state_; //Keeping this for now, but I intend to rip it out so that multiple threads can 
                //all use the same pair of file descriptors concurrently.  Or maybe just provide
                //a clone method on LocalBlockReader that shares FDs.  Would need to refcount to 
                //do the releaseFDs call.

  struct RequestFileDescriptors;
  template <class MutableBufferSequence>
  struct ReadFromDataFile;
  struct ReadFromMetaFile;          //also do checksum in here to be able to toss the buffer
  struct ReleaseFileDescriptors;


 private:
  Stream *stream_;
  BlockReaderOptions options_;
  int data_in_; //fd of block data
  int meta_in_; //fd of checksums for block data
  bool do_checksum_; //skip reading metafile and doing checksum if false
};

template<class Stream>
class RemoteBlockReader : public std::enable_shared_from_this<RemoteBlockReader<Stream> > {
 public:
  explicit RemoteBlockReader(const BlockReaderOptions &options,
                             Stream *stream)
      : stream_(stream)
      , state_(kOpen)
      , options_(options)
      , chunk_padding_bytes_(0)
  {}

  template<class MutableBufferSequence, class ReadHandler>
  void async_read_some(const MutableBufferSequence& buffers,
                       const ReadHandler &handler);

  template<class MutableBufferSequence>
  size_t read_some(const MutableBufferSequence &buffers, Status *status);

  Status connect(const std::string &client_name,
                 const hadoop::common::TokenProto *token,
                 const hadoop::hdfs::ExtendedBlockProto *block,
                 uint64_t length, uint64_t offset);

  template<class ConnectHandler>
  void async_connect(const std::string &client_name,
                 const hadoop::common::TokenProto *token,
                 const hadoop::hdfs::ExtendedBlockProto *block,
                 uint64_t length, uint64_t offset,
                 const ConnectHandler &handler);

 private:
  struct ReadPacketHeader;
  struct ReadChecksum;
  struct ReadPadding;
  template<class MutableBufferSequence>
  struct ReadData;
  struct AckRead;
  enum State {
    kOpen,
    kReadPacketHeader,
    kReadChecksum,
    kReadPadding,
    kReadData,
    kFinished,
  };

  Stream *stream_;
  hadoop::hdfs::PacketHeaderProto header_;
  State state_;
  BlockReaderOptions options_;
  size_t packet_len_;
  int packet_data_read_bytes_;
  int chunk_padding_bytes_;
  long long bytes_to_read_;
  std::vector<char> checksum_;
};

}

#include "remote_block_reader_impl.h"
#include "local_block_reader_impl.h"

#endif
