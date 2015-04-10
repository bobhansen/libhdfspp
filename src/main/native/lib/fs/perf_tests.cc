#include "libhdfs++/chdfs.h"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>
#include <random>

//default to reading 1MB blocks for linear scans
static const size_t KB = 1024;
static const size_t MB = 1024 * 1024;


struct seek_info {
  seek_info() : seek_count(0), fail_count(0), runtime(0) {};
  std::string str() {
    std::stringstream ss;
    ss << "seeks: " << seek_count << " failed reads:" << fail_count << " runtime:" << runtime << " seeks/sec:" << (seek_count+fail_count)/runtime;
    return ss.str();
  }

  int seek_count;
  int fail_count;
  double runtime;
};

seek_info single_threaded_random_seek(hdfsFS fs, hdfsFile file, 
                                      unsigned int count = 1000,   //how many seeks to try
                                      off_t window_min = 0,        //minimum offset into file
                                      off_t window_max = 64 * MB); //max offset into file


struct scan_info {
  scan_info() : read_bytes(0), runtime(0.0) {};
  std::string str() {
    std::stringstream ss;
    ss << "read " << read_bytes << "bytes in " << runtime << " seconds, bandwidth " << read_bytes / runtime / MB << "MB/s";
    return ss.str();
  }

  uint64_t read_bytes;
  double runtime;
};

scan_info single_threaded_linear_scan(hdfsFS fs, hdfsFile file,
                                      size_t read_size = 128 * KB,
                                      off_t start = 0,      //where in file to start reading
                                      off_t end = 64 * MB); //byte offset in file to stop reading



int main(int argc, char **argv) {
  if(argc != 4) {
    std::cout << "usage: ./perf_tests <host> <port> <file> [...]" << std::endl;
    return 1;
  }

  hdfsFS fs = hdfsConnect(argv[1], std::atoi(argv[2]));  
  hdfsFile file = hdfsOpenFile(fs, argv[3], 0, 0, 0, 0);

  scan_info s = single_threaded_linear_scan(fs, file);

  std::cout << std::endl << s.str() << std::endl;
    

  return 0;
}


seek_info single_threaded_random_seek(hdfsFS fs, hdfsFile file, unsigned int count, off_t window_min, off_t window_max){
  seek_info info;

  //rng setup, bound by window size
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(window_min, window_max);

  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();

  for(unsigned int i=0;i<count;i++){
    std::uint64_t idx = dis(gen);
    char buf[1];
    std::int64_t cnt = hdfsPread(fs, file, idx, buf, 1);
    if(cnt != 1) {
      info.fail_count++;
    } else {
      info.seek_count++;
    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  info.runtime = elapsed.count();

  return info;
}



scan_info single_threaded_linear_scan(hdfsFS fs, hdfsFile file, size_t buffsize, off_t start_offset, off_t end_offset) {
  scan_info info;

  std::vector<char> buffer;
  buffer.reserve(buffsize);

  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();  

  std::int64_t count = start_offset;
  while(count <= end_offset) {
    std::int64_t read_bytes = hdfsPread(fs, file, count, &buffer[0], buffsize);
    if(read_bytes <= 0) {
      //todo: add some retry logic to step over block boundary
      break;
    } else {
      count += read_bytes;
    }
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> dt = end - start;

  info.read_bytes = count - start_offset;
  info.runtime = dt.count();
 
  return info;  
}


