#ifndef FIFO_H
#define FIFO_H

// fifo template
// blocks enq() and deq() when queue is FULL or EMPTY

#include <errno.h>
#include <sys/time.h>
#include <time.h>

#include <condition_variable>
#include <list>
#include <mutex>

#include "verify.h"

namespace gxrpc {

template <class T>
class Fifo {
 public:
  Fifo(int m = 0);
  ~Fifo();
  bool enq(T, bool blocking = true);
  void deq(T *);
  bool size();

 private:
  std::list<T> q_;
  std::mutex m_;
  std::condition_variable non_empty_c_;  // q went non-empty
  std::condition_variable has_space_c_;  // q is not longer overfull
  unsigned int max_;  // maximum capacity of the queue, block enq threads if
                      // exceeds this limit
};

template <class T>
Fifo<T>::Fifo(int limit) : max_(limit) {}

template <class T>
bool Fifo<T>::size() {
  std::scoped_lock<std::mutex> ml(m_);
  return q_.size();
}

template <class T>
bool Fifo<T>::enq(T e, bool blocking) {
  std::unique_lock<std::mutex> ml(m_);
  while (1) {
    if (!max_ || q_.size() < max_) {
      q_.push_back(e);
      break;
    }
    if (blocking) {
      has_space_c_.wait(ml);
    } else {
      return false;
    }
  }
  non_empty_c_.notify_one();
  return true;
}

template <class T>
void Fifo<T>::deq(T *e) {
  std::unique_lock<std::mutex> ml(m_);
  while (1) {
    if (q_.empty()) {
      non_empty_c_.wait(ml);
    } else {
      *e = q_.front();
      q_.pop_front();
      if (max_ && q_.size() < max_) {
        has_space_c_.notify_one();
      }
      break;
    }
  }
  return;
}

}  // namespace gxrpc
#endif