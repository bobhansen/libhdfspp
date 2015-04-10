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
static const size_t MB = 1024 * 1024;

struct scan_info;

struct seek_info{
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


/*
struct scan_info {
  scan_info(std::int64_t read_count) : read_bytes(read_count) {};
  std::int64_t read_bytes;
};

scan_info single_threaded_scan(hdfsFS fs, hdfsFile file, std::vector<char>& buffer){
  std::int64_t read_count = 0;
  while(1) {
    std::int64_t read_bytes = hdfsPread(fs, file, read_count, reinterpret_cast<void*>(&buffer[0]), 10);
    if(read_bytes < 0) {
      std::cout << "error after reading " << read_count << " bytes" << std::endl;
      return scan_info(read_count); 
    } else if (read_bytes == 0) {
      std::cout << "finished reading, read " << read_count << " bytes" << std::endl;
      return scan_info(read_count);
    } else {
      read_count += read_bytes;
    }
  }
  return scan_info(-1);
}
*/

int main(int argc, char **argv) {
  if(argc != 4) {
    std::cout << "usage: ./perf_tests <host> <port> <file> [...]" << std::endl;
    return 1;
  }

  hdfsFS fs = hdfsConnect(argv[1], std::atoi(argv[2]));  
  hdfsFile file = hdfsOpenFile(fs, argv[3], 0, 0, 0, 0);
  std::vector<char> buffer;
  buffer.reserve(MB);

  /* single threaded sequential reads
  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  
  unsigned long long total = 0;
  while(total <= MB * 64) {
    long long count = hdfsPread(fs, file, total, &buffer[0], MB);
    //std::cout << "read " << total << std::endl;
    if(count <= 0)
      break;
    total += count;
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed = end - start;

  double bandwidth = double(total) / elapsed.count() / (1024.0 * 1024.0);

  std::cout << "read " << total << " bytes in " << elapsed.count() << " seconds" << std::endl;
  std::cout << bandwidth << "MB/s" << std::endl;
  */


  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();  

  std::int64_t count = 0 * MB;
  while(count <= 64 * MB) {
    std::int64_t read_bytes = hdfsPread(fs, file, count, &buffer[0], MB);
    //std::cout << "count = " << count << " last read = " << read_bytes << std::endl;

    if(read_bytes <= 0)
      break;
    else
      count += read_bytes;
  }

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> dt = end - start;

  double bandwidth = double(count) / dt.count() / double(MB);
  std::cout << "read " << count << " bytes at " << bandwidth << "MB/s"; 
   

  //seek_info s = single_threaded_random_seek(fs, file, 3000);
  //std::cout << s.str() << std::endl;

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




