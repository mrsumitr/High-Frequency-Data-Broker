#pragma once

#include<atomic>
#include<cstdio>
#include<thread>
#include<vector>

// A fixed set of worker threads created once at startup and kept alive
// for the whole program. they sleep until told to stop; actual "wake up
//and process a slot" signaling comes in the ring-buffer step next.
class WorkerPool {
public:
 WorkerPool(std::size_t num_threads) : running_(true){
  threads_.reserve(num_threads);
  for(std::size_t i=0; i<num_threads; ++i){
    threads_.emplace_back([this, i] {this->worker_loop(i); });
  }
 }
 ~WorkerPool(){
  running_.store(false, std::memory_order_release);
  for(auto &t : threads_){
    if(t.joinable()) t.join();
  }
 }
 WorkerPool(const WorkerPool&) =delete;
 WorkerPool& operator=(const WorkerPool&) = delete;

private:
 void worker_loop(std::size_t worker_id){
  std::printf("Worker %zu started, waiting for work...\n", worker_id);
  while (running_.load(std::memory_order_acquire)){
    //placeholder: next step replaces this sleep with a real 
    // wait on the atomic ring-buffer.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  std::printf("Worker %zu shutting down.\n", worker_id);
 }

 std::atomic<bool> running_;
 std::vector<std::thread> threads_;
};

