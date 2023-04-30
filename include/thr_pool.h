#ifndef GXRPC__THR_POOL__
#define GXRPC__THR_POOL__

#include <pthread.h>
#include <thread>
#include <mutex>
#include <vector>
#include <functional>

#include "fifo.h"
#include "define.h"

namespace gxrpc {

class ThreadPool {
 public:
  ThreadPool(int sz, bool blocking = true);
  ~ThreadPool();
  template <class C, class A>
  bool addObjJob(C *o, void (C::*m)(A), A a);
  void waitDone();

  bool takeJob(Job &job);

 private:
  int nthreads_;
  bool blockadd_;
  std::mutex mutex_;

  Fifo<Job> jobq_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  bool addJob(Job func);
  bool addJob(void *(*f)(void *), void *a);
  void runInThread();
};

template <class C, class A>
bool ThreadPool::addObjJob(C *o, void (C::*m)(A), A a) {
  auto objfunc_wrapper = [&]() { (o->*m)(a); }
  return addJob(std::move(objfunc_wrapper));
}  

} // namespace gxrpc



#endif