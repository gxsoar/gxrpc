#include "pollmgr.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include "jsl_log.h"
#include "verify.h"
#include "method_thread.h"

namespace gxrpc {


PollMgr *PollMgr::instance = nullptr;
std::once_flag pollmgr_is_initialized;

void PollMgrInit() { PollMgr::instance = new PollMgr(); }

PollMgr *PollMgr::Instance() {
  std::call_once(pollmgr_is_initialized, PollMgrInit);
  return instance;
}

PollMgr::PollMgr() : pending_change_(false) {
  std::fill_n(callbacks_, MAX_POLL_FDS, nullptr);
  aio_ = std::make_unique<EPollAIO>();
  th_ = std::make_unique<std::thread>(method_thread(this, &PollMgr::wait_loop));
}

PollMgr::~PollMgr() {
  // never kill me!!!
  VERIFY(0);
}

void PollMgr::add_callback(int fd, poll_flag flag, aio_callback *ch) {
  VERIFY(fd < MAX_POLL_FDS);
  std::scoped_lock<std::mutex> ml(m_);
  aio_->watch_fd(fd, flag);

  VERIFY(!callbacks_[fd] || callbacks_[fd] == ch);
  callbacks_[fd] = ch;
}

// remove all callbacks related to fd
// the return guarantees that callbacks related to fd
// will never be called again
void PollMgr::block_remove_fd(int fd) {
  std::unique_lock<std::mutex> ml(m_);
  aio_->unwatch_fd(fd, poll_flag::CB_RDWR);
  pending_change_ = true;
  changedone_c_.wait(ml);
  callbacks_[fd] = nullptr;
}

void PollMgr::del_callback(int fd, poll_flag flag) {
  std::scoped_lock<std::mutex> ml(m_);
  if (aio_->unwatch_fd(fd, flag)) {
    callbacks_[fd] = nullptr;
  }
}

bool PollMgr::has_callback(int fd, poll_flag flag, aio_callback *c) {
  std::scoped_lock<std::mutex> ml(m_);
  if (!callbacks_[fd] || callbacks_[fd] != c) return false;

  return aio_->is_watched(fd, flag);
}

void PollMgr::wait_loop() {
  std::vector<int> readable;
  std::vector<int> writable;

  while (true) {
    {
      std::scoped_lock<std::mutex> ml(m_);
      if (pending_change_) {
        pending_change_ = false;
        changedone_c_.notify_all();
      }
    }
    readable.clear();
    writable.clear();
    aio_->wait_ready(&readable, &writable);

    if (!readable.size() && !writable.size()) {
      continue;
    }
    // no locking of m_
    // because no add_callback() and del_callback should
    // modify callbacks_[fd] while the fd is not dead
    auto rn = readable.size();
    for (decltype(rn) i = 0; i < rn; i++) {
      int fd = readable[i];
      if (callbacks_[fd]) callbacks_[fd]->read_cb(fd);
    }
    auto wn = writable.size();
    for (decltype(wn) i = 0; i < wn; i++) {
      int fd = writable[i];
      if (callbacks_[fd]) callbacks_[fd]->write_cb(fd);
    }
  }
}

SelectAIO::SelectAIO() : highfds_(0) {
  FD_ZERO(&rfds_);
  FD_ZERO(&wfds_);

  VERIFY(pipe(pipefd_) == 0);
  FD_SET(pipefd_[0], &rfds_);
  highfds_ = pipefd_[0];

  int flags = fcntl(pipefd_[0], F_GETFL, NULL);
  flags |= O_NONBLOCK;
  fcntl(pipefd_[0], F_SETFL, flags);
}

SelectAIO::~SelectAIO() { }

void SelectAIO::watch_fd(int fd, poll_flag flag) {
  std::scoped_lock<std::mutex> ml(m_);
  if (highfds_ <= fd) highfds_ = fd;

  if (flag == poll_flag::CB_RDONLY) {
    FD_SET(fd, &rfds_);
  } else if (flag == poll_flag::CB_WRONLY) {
    FD_SET(fd, &wfds_);
  } else {
    FD_SET(fd, &rfds_);
    FD_SET(fd, &wfds_);
  }

  char tmp = 1;
  VERIFY(write(pipefd_[1], &tmp, sizeof(tmp)) == 1);
}

bool SelectAIO::is_watched(int fd, poll_flag flag) {
  std::scoped_lock<std::mutex> ml(m_);
  if (flag == poll_flag::CB_RDONLY) {
    return FD_ISSET(fd, &rfds_);
  } else if (flag == poll_flag::CB_WRONLY) {
    return FD_ISSET(fd, &wfds_);
  } else {
    return (FD_ISSET(fd, &rfds_) && FD_ISSET(fd, &wfds_));
  }
}

bool SelectAIO::unwatch_fd(int fd, poll_flag flag) {
  std::scoped_lock<std::mutex> ml(m_);
  if (flag == poll_flag::CB_RDONLY) {
    FD_CLR(fd, &rfds_);
  } else if (flag ==poll_flag::CB_WRONLY) {
    FD_CLR(fd, &wfds_);
  } else if (flag == poll_flag::CB_RDWR) {
    FD_CLR(fd, &wfds_);
    FD_CLR(fd, &rfds_);
  } else {
    VERIFY(0);
  }

  if (!FD_ISSET(fd, &rfds_) && !FD_ISSET(fd, &wfds_)) {
    if (fd == highfds_) {
      int newh = pipefd_[0];
      for (int i = 0; i <= highfds_; i++) {
        if (FD_ISSET(i, &rfds_)) {
          newh = i;
        } else if (FD_ISSET(i, &wfds_)) {
          newh = i;
        }
      }
      highfds_ = newh;
    }
  }
  if (flag == poll_flag::CB_RDWR) {
    char tmp = 1;
    VERIFY(write(pipefd_[1], &tmp, sizeof(tmp)) == 1);
  }
  return (!FD_ISSET(fd, &rfds_) && !FD_ISSET(fd, &wfds_));
}

void SelectAIO::wait_ready(std::vector<int> *readable,
                           std::vector<int> *writable) {
  fd_set trfds, twfds;
  int high;

  {
    std::scoped_lock<std::mutex> ml(m_);
    trfds = rfds_;
    twfds = wfds_;
    high = highfds_;
  }

  int ret = select(high + 1, &trfds, &twfds, NULL, NULL);

  if (ret < 0) {
    if (errno == EINTR) {
      return;
    } else {
      perror("select:");
      jsl_log(JSL_DBG_OFF, "PollMgr::select_loop failure errno %d\n", errno);
      VERIFY(0);
    }
  }

  for (int fd = 0; fd <= high; fd++) {
    if (fd == pipefd_[0] && FD_ISSET(fd, &trfds)) {
      char tmp;
      VERIFY(read(pipefd_[0], &tmp, sizeof(tmp)) == 1);
      VERIFY(tmp == 1);
    } else {
      if (FD_ISSET(fd, &twfds)) {
        writable->push_back(fd);
      }
      if (FD_ISSET(fd, &trfds)) {
        readable->push_back(fd);
      }
    }
  }
}

#ifdef __linux__

EPollAIO::EPollAIO() {
  pollfd_ = epoll_create(MAX_POLL_FDS);
  VERIFY(pollfd_ >= 0);
  bzero(fdstatus_, sizeof(int) * MAX_POLL_FDS);
}

EPollAIO::~EPollAIO() { close(pollfd_); }

static inline int poll_flag_to_event(poll_flag flag) {
  int f;
  if (flag == poll_flag::CB_RDONLY) {
    f = EPOLLIN;
  } else if (flag == poll_flag::CB_WRONLY) {
    f = EPOLLOUT;
  } else {  // flag == CB_RDWR
    f = EPOLLIN | EPOLLOUT;
  }
  return f;
}

void EPollAIO::watch_fd(int fd, poll_flag flag) {
  VERIFY(fd < MAX_POLL_FDS);

  struct epoll_event ev;
  int op = fdstatus_[fd] ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
  fdstatus_[fd] |= (int)flag;

  ev.events = EPOLLET;
  ev.data.fd = fd;

  if (fdstatus_[fd] & static_cast<int>(poll_flag::CB_RDONLY)) {
    ev.events |= EPOLLIN;
  }
  if (fdstatus_[fd] & static_cast<int>(poll_flag::CB_WRONLY)) {
    ev.events |= EPOLLOUT;
  }

  if (flag == poll_flag::CB_RDWR) {
    VERIFY(ev.events == (uint32_t)(EPOLLET | EPOLLIN | EPOLLOUT));
  }

  VERIFY(epoll_ctl(pollfd_, op, fd, &ev) == 0);
}

bool EPollAIO::unwatch_fd(int fd, poll_flag flag) {
  VERIFY(fd < MAX_POLL_FDS);
  fdstatus_[fd] &= ~(int)flag;

  struct epoll_event ev;
  int op = fdstatus_[fd] ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;

  ev.events = EPOLLET;
  ev.data.fd = fd;

  if (fdstatus_[fd] & static_cast<int>(poll_flag::CB_RDONLY)) {
    ev.events |= EPOLLIN;
  }
  if (fdstatus_[fd] & static_cast<int>(poll_flag::CB_WRONLY)) {
    ev.events |= EPOLLOUT;
  }

  if (flag == poll_flag::CB_RDWR) {
    VERIFY(op == EPOLL_CTL_DEL);
  }
  VERIFY(epoll_ctl(pollfd_, op, fd, &ev) == 0);
  return (op == EPOLL_CTL_DEL);
}

bool EPollAIO::is_watched(int fd, poll_flag flag) {
  VERIFY(fd < MAX_POLL_FDS);
  return ((fdstatus_[fd] & static_cast<int>(poll_flag::CB_MASK)) == static_cast<int>(flag));
}

void EPollAIO::wait_ready(std::vector<int> *readable,
                          std::vector<int> *writable) {
  int nfds = epoll_wait(pollfd_, ready_, MAX_POLL_FDS, -1);
  for (int i = 0; i < nfds; i++) {
    if (ready_[i].events & EPOLLIN) {
      readable->push_back(ready_[i].data.fd);
    }
    if (ready_[i].events & EPOLLOUT) {
      writable->push_back(ready_[i].data.fd);
    }
  }
}

#endif

}