#pragma once

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <cstddef>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <memory>

#include "verify.h"
#include "define.h"

struct req_header {
  req_header(int x = 0, int p = 0, int c = 0, int s = 0, int xi = 0)
      : xid(x), proc(p), clt_nonce(c), srv_nonce(s), xid_rep(xi) {}
  int xid;
  int proc;
  unsigned int clt_nonce;
  unsigned int srv_nonce;
  int xid_rep;
};

struct reply_header {
  reply_header(int x = 0, int r = 0) : xid(x), ret(r) {}
  int xid;
  int ret;
};

using rpc_checksum_t = uint64_t;
using rpc_sz_t = int;

constexpr int DEFAULT_RPC_SZ = 1024;
// size of initial buffer allocation
#if RPC_CHECKSUMMING
  // size of rpc_header includes a 4-byte int to be filled by tcpchan and
  // uint64_t checksum
  constexpr int RPC_HEADER_SZ = static_max<sizeof(req_header), sizeof(reply_header)>::value +
                  sizeof(rpc_sz_t) + sizeof(rpc_checksum_t);
#else
  constexpr int RPC_HEADER_SZ = static_max<sizeof(req_header), sizeof(reply_header)>::value +
                  sizeof(rpc_sz_t);
#endif

class marshall {
 private:
  std::unique_ptr<char> buf_; // base of the raw bytes buffer
  int capa_;  // capactiry of the buffer
  int ind_;   // Read/write the head position
 public:
  marshall() : buf_(std::make_unique<char>(DEFAULT_RPC_SZ)), capa_(DEFAULT_RPC_SZ), ind_(RPC_HEADER_SZ) {}

  ~marshall() {
  }

  int size() { return ind_; }
  char *cstr() { return buf_.get(); }

  void rawbyte(unsigned char);
  void rawbytes(const char *, int);

  // Return the current content (excluding header) as a string
  std::string get_content() {
    return std::string(buf_.get() + RPC_HEADER_SZ, ind_ - RPC_HEADER_SZ);
  }

  // Return the current content (excluding header) as a string
  std::string str() { return get_content(); }

  void pack(int i);

  void pack_req_header(const req_header &h) {
    int saved_sz = ind_;
    // leave the first 4-byte empty for channel to fill size of pdu
    ind_ = sizeof(rpc_sz_t);
#if RPC_CHECKSUMMING
    _ind += sizeof(rpc_checksum_t);
#endif
    pack(h.xid);
    pack(h.proc);
    pack((int)h.clt_nonce);
    pack((int)h.srv_nonce);
    pack(h.xid_rep);
    ind_ = saved_sz;
  }

  void pack_reply_header(const reply_header &h) {
    int saved_sz = ind_;
    // leave the first 4-byte empty for channel to fill size of pdu
    ind_ = sizeof(rpc_sz_t);
#if RPC_CHECKSUMMING
    ind_ += sizeof(rpc_checksum_t);
#endif
    pack(h.xid);
    pack(h.ret);
    ind_ = saved_sz;
  }

  void take_buf(char **b, int *s) {
    *b = buf_.release();
    *s = ind_;
    ind_ = 0;
    return;
  }
};
marshall &operator<<(marshall &, bool);
marshall &operator<<(marshall &, unsigned int);
marshall &operator<<(marshall &, int);
marshall &operator<<(marshall &, unsigned char);
marshall &operator<<(marshall &, char);
marshall &operator<<(marshall &, unsigned short);
marshall &operator<<(marshall &, short);
marshall &operator<<(marshall &, unsigned long long);
marshall &operator<<(marshall &, const std::string &);

class unmarshall {
 private:
  std::unique_ptr<char> buf_;
  int sz_;
  int ind_;
  bool ok_;

 public:
  unmarshall() : buf_(nullptr), sz_(0), ind_(0), ok_(false) {}
  unmarshall(char *b, int sz) : buf_(std::make_unique<char>(b)), sz_(sz), ind_(), ok_(true) {}
  unmarshall(const std::string &s) : buf_(nullptr), sz_(0), ind_(0), ok_(false) {
    // take the content which does not exclude a RPC header from a string
    take_content(s);
  }
  ~unmarshall() {}

  // take contents from another unmarshall object
  void take_in(unmarshall &another);

  // take the content which does not exclude a RPC header from a string
  void take_content(const std::string &s) {
    sz_ = s.size() + RPC_HEADER_SZ;
    buf_.reset(new char[sz_]);
    VERIFY(buf_);
    ind_ = RPC_HEADER_SZ;
    memcpy(buf_.get() + ind_, s.data(), s.size());
    ok_ = true;
  }

  bool ok() { return ok_; }
  char *cstr() { return buf_.get(); }
  bool okdone();
  unsigned int rawbyte();
  void rawbytes(std::string &s, unsigned int n);

  int ind() { return ind_; }
  int size() { return sz_; }
  void unpack(int *);  // non-const ref
  void take_buf(char **b, int *sz) {
    *b = buf_.release();
    *sz = sz_;
    sz_ = ind_ = 0;
  }

  void unpack_req_header(req_header *h) {
    // the first 4-byte is for channel to fill size of pdu
    ind_ = sizeof(rpc_sz_t);
#if RPC_CHECKSUMMING
    _ind += sizeof(rpc_checksum_t);
#endif
    unpack(&h->xid);
    unpack(&h->proc);
    unpack((int *)&h->clt_nonce);
    unpack((int *)&h->srv_nonce);
    unpack(&h->xid_rep);
    ind_ = RPC_HEADER_SZ;
  }

  void unpack_reply_header(reply_header *h) {
    // the first 4-byte is for channel to fill size of pdu
    ind_ = sizeof(rpc_sz_t);
#if RPC_CHECKSUMMING
    _ind += sizeof(rpc_checksum_t);
#endif
    unpack(&h->xid);
    unpack(&h->ret);
    ind_ = RPC_HEADER_SZ;
  }
};

unmarshall &operator>>(unmarshall &, bool &);
unmarshall &operator>>(unmarshall &, unsigned char &);
unmarshall &operator>>(unmarshall &, char &);
unmarshall &operator>>(unmarshall &, unsigned short &);
unmarshall &operator>>(unmarshall &, short &);
unmarshall &operator>>(unmarshall &, unsigned int &);
unmarshall &operator>>(unmarshall &, int &);
unmarshall &operator>>(unmarshall &, unsigned long long &);
unmarshall &operator>>(unmarshall &, std::string &);

template <class C>
marshall &operator<<(marshall &m, std::vector<C> v) {
  m << (unsigned int)v.size();
  for (const auto &i : v) m << i;
  return m;
}

template <class C>
unmarshall &operator>>(unmarshall &u, std::vector<C> &v) {
  v.clear();
  unsigned n;
  u >> n;
  v.resize(n);
  for (decltype(n) i = 0; i < n; ++ i) {
    C z;
    u >> z;
    v[i] = z;
  }
  return u;
}

template <class A, class B>
marshall &operator<<(marshall &m, const std::map<A, B> &d) {
  m << (unsigned int)d.size();
  for (const auto &[first, second] : d) {
    m << first << second;
  }
  return m;
}

template <class A, class B>
unmarshall &operator>>(unmarshall &u, std::map<A, B> &d) {
  unsigned int n;
  u >> n;
  d.clear();
  for (decltype(n) lcv = 0; lcv < n; lcv++) {
    A a;
    B b;
    u >> a >> b;
    d[a] = b;
  }
  return u;
}
