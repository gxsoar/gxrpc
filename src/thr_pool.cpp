#include "thr_pool.h"
#include <stdlib.h>

namespace gxrpc {
// if blocking, then addJob() blocks when queue is full
// otherwise, addJob() simply returns false when queue is full
ThreadPool::ThreadPool(int sz, bool blocking)
    : nthreads_(sz), blockadd_(blocking), jobq_(100 * sz) {
  for (int i = 0; i < sz; i++) {
    threads_.emplace_back(std::thread(std::bind(&ThreadPool::runInThread, this)));
  }
}

// IMPORTANT: this function can be called only when no external thread
// will ever use this thread pool again or is currently blocking on it
ThreadPool::~ThreadPool() {
  for (int i = 0; i < nthreads_; ++ i) {
    addJob(nullptr);
  }
  for (auto &th : threads_) {
    th->join();
  }
}

bool ThreadPool::addJob(Job job) {
  return jobq_.enq(job, blockadd_);
}

// bool ThreadPool::addJob(void *(*f)(void *), void *a) {
  // job_t j;
  // j.f = f;
  // j.a = a;
  // return jobq_.enq(j, blockadd_);
// }

bool ThreadPool::takeJob(Job &job) {
  jobq_.deq(&job);
  return job != nullptr;
}

void ThreadPool::runInThread() {
  try {
    while(true) {
      Job job;
      if (takeJob(job)) {
        job();
      } else {
        break;
      }
    }
  } catch(const std::exception &ex) {
    fprintf(stderr, "gxrpc thread ex : %s\n", ex.what());
  }
}
}