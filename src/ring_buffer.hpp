#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

//fixed-capacity circular buffer of pool slot indices. The listener
//thread publishes a slot index by writing it into the ring and 
// bumping write_index_. Worker threads race to claim entries by 
// atomically bumping read_index_ -- whichever thread's fetch_add
// lands on a giver index owns that entry, no locking involved.
//
//capacity must be a power of two so index wraparound can use a 
//bitmask (& (capacity-1 )) instead of modulo
template < std::size_t Capacity>
class RingBuffer{
  static_assert((Capacity & (Capacity - 1)) == 0,"Capacity must be a power of two");

  public:
  // called by the listener thread only.
  void publish(std::size_t pool_slot_index){
    std::size_t pos=write_index_.load(std::memory_order_relaxed);
    entries_[pos & (Capacity - 1)] = pool_slot_index;
    // release: ensures the write to entries_[] above is visible 
    // to any worker thread that observes the new write_index_.
    write_index_.store(pos + 1, std::memory_order_release);

  }
  //called by worker threads. Returns false if there's  currently
  // nothing new to claim
  bool try_claim(std:: size_t& out_pool_slot_index){
    std::size_t pos = read_index_.load(std::memory_order_relaxed);
    //acquire: pairs with publish()'s release, so once we see a 
    // write_index_ that has advanced past pos, entries_[pos] is
    //guaranteed visible.
    if(pos >= write_index_.load(std::memory_order_acquire)){
      return false; // nothing new published yet
    }
    // multiple workers may race here; compare_exchange ensures
    // only one of them wins this particular index.
    if(!read_index_.compare_exchange_weak(pos,pos + 1, std::memory_order_acq_rel, std::memory_order_relaxed)){
      return false; // another worker already claimed it, try again next loop
    }
    out_pool_slot_index=entries_[pos & (Capacity - 1)];
    return true;
  }
private:
  std::size_t entries_[Capacity]{};
  std::atomic<std::size_t> write_index_{0};
  std::atomic<std::size_t> read_index_{0};
};
