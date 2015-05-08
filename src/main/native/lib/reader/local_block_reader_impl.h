#ifndef IMPL_LOCAL_BLOCK_READER_H_
#define IMPL_LOCAL_BLOCK_READER_H_

namespace hdfs {



template <class Stream>
struct LocalBlockReader<Stream>::RequestFileDescriptors : continuation::Continuation {
  //this is the hard part
};

template <class Stream>
struct LocalBlockReader<Stream>::ReleaseFileDescriptors : continuation::Continuation {
  //this is the other hard part
};

template <class Stream>
template <class MutableBufferSequence>
struct LocalBlockReader<Stream>::ReadFromDataFile : continuation::Continuation {


  asio::async_read()

};

template <class Stream>
struct LocalBlockReader<Stream>::ReadFromMetaFile : continuation::Continuation {
  virtual void Run(const Next& next) override {
    if(!do_checksum_) {
      next(Status.ok());
      return;
    }


    
  }
};



template <class Stream>
template <class ConnectHandler>
void LocalBlockReader<Stream>::async_connect(const std::string &client_name,
                                             const hadoop::common::TokenProto *token,
                                             const hadoop::hdfs::ExtendedBlockProto *block)
  {

    struct State {
      std::string header;
    };

    auto m = continuation::Pipeline<State>::Create();
    State *s = &m->state();



    s->header.insert(s->header.begin(), { 0, kDataTransferVersion, Operation::kRequestShortCircuitFDs });
    //???

    auto 


  }



};


#endif
