#pragma once

#include <netinet/in.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

#include <list>
#include <map>
#include <mutex>

#include "connection.h"
#include "marshall.h"
#include "thr_pool.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif

namespace gxrpc {

class rpc_const {
 public:
  static const unsigned int bind = 1;  // handler number reserved for bind
  static const int timeout_failure = -1;
  static const int unmarshal_args_failure = -2;
  static const int unmarshal_reply_failure = -3;
  static const int atmostonce_failure = -4;
  static const int oldsrv_failure = -5;
  static const int bind_failure = -6;
  static const int cancel_failure = -7;
};

// rpc client endpoint.
// manages a xid space per destination socket
// threaded: multiple threads can be sending RPCs,
class rpcc : public chanmgr {
 private:
  // manages per rpc info
  struct caller {
    caller(unsigned int xxid, unmarshall *un);
    ~caller();

    unsigned int xid;
    unmarshall *un;
    int intret;
    bool done;
    std::mutex m;
    std::condition_variable c;
  };

  void get_refconn(connection **ch);
  void update_xid_rep(unsigned int xid);

  sockaddr_in dst_;
  unsigned int clt_nonce_;
  unsigned int srv_nonce_;
  bool bind_done_;
  unsigned int
      xid_;  //	按照顺序来给caller_或者request分配xid,同时方便完成滑动窗口
  int lossytest_;
  bool retrans_;
  bool reachable_;

  connection *chan_;
  std::mutex m_;
  std::mutex chan_m_;

  bool destroy_wait_;
  std::condition_variable destory_wait_c_;

  std::map<int, caller *> calls_;  // 每个rpcc都有一个clt_nonce_,
                                   // 用来建立clt_nonce_和caller之间的映射
  std::list<unsigned int> xid_rep_window_;  // 猜测每个rpcc的发送窗口,
                                            // 和rpcs的什么关系？ rpcc发送的请求
  struct request {
    request() { clear(); }
    void clear() {
      buf.clear();
      xid = -1;
    }
    bool isvalid() { return xid != -1; }
    std::string buf;
    int xid;
  };
  struct request dup_req_;  // 用来表示重复的req请求
  int xid_rep_done_;

 public:
  rpcc(sockaddr_in d, bool retrans = true);
  ~rpcc();

  struct TO {
    int to;
  };
  static const TO to_max;
  static const TO to_min;
  static TO to(int x) {
    TO t;
    t.to = x;
    return t;
  }
  // rpcc有一个单独的clt_nonce_用来标识,有一个xid用来标识每一个request
  unsigned int id() { return clt_nonce_; }

  int bind(TO to = to_max);

  void set_reachable(bool r) { reachable_ = r; }

  void cancel();

  int islossy() { return lossytest_ > 0; }
  // rpcc的核心功能，用来管理rpcc对client的call的调用，用来将req打包发给rep
  int call1(unsigned int proc, marshall &req, unmarshall &rep, TO to);

  bool got_pdu(connection *c, char *b, int sz);

  template <class R>
  int call_m(unsigned int proc, marshall &req, R &r, TO to);

  template <class R, class... Args>
  int call(unsigned int proc, R &r, TO to = to_max, Args&&... args) {
    marshall req;
    req << ... << std::forward<Args>(args);
    return call_m(proc, req, r, to);
  }
};

template <class R>
int rpcc::call_m(unsigned int proc, marshall &req, R &r, TO to) {
  unmarshall u;
  int intret = call1(proc, req, u, to);
  if (intret < 0) return intret;
  u >> r;
  if (u.okdone() != true) {
    fprintf(stderr,
            "rpcc::call_m: failed to unmarshall the reply."
            "You are probably calling RPC 0x%x with wrong return "
            "type.\n",
            proc);
    VERIFY(0);
    return rpc_const::unmarshal_reply_failure;
  }
  return intret;
}

bool operator<(const sockaddr_in &a, const sockaddr_in &b);
// 一个注册的句柄
class handler {
 public:
  handler() {}
  virtual ~handler() {}
  virtual int fn(unmarshall &, marshall &) = 0;
};

// rpc server endpoint.
class rpcs : public chanmgr {
  typedef enum {
    NEW,         // new RPC, not a duplicate
    INPROGRESS,  // duplicate of an RPC we're still processing
    DONE,        // duplicate of an RPC we already replied to (have reply)
    FORGOTTEN,   // duplicate of an old RPC whose reply we've forgotten
  } rpcstate_t;

 private:
  // state about an in-progress or completed RPC, for at-most-once.
  // if cb_present is true, then the RPC is complete and a reply
  // has been sent; in that case buf points to a copy of the reply,
  // and sz holds the size of the reply.
  struct reply_t {
    reply_t(unsigned int _xid) {
      xid = _xid;
      cb_present = false;
      buf = NULL;
      sz = 0;
    }
    bool operator==(const reply_t &rhs) { return xid == rhs.xid; }
    unsigned int xid;
    bool cb_present;  // whether the reply buffer is valid
    char *buf;        // the reply buffer
    int sz;           // the size of reply buffer
  };

  int port_;
  unsigned int nonce_;

  // provide at most once semantics by maintaining a window of replies
  // per client that that client hasn't acknowledged receiving yet.
  // indexed by client nonce.
  // clt_nonce 对应的window
  std::map<unsigned int, std::list<reply_t> > reply_window_;

  void free_reply_window(void);
  void add_reply(unsigned int clt_nonce, unsigned int xid, char *b, int sz);

  rpcstate_t checkduplicate_and_update(unsigned int clt_nonce, unsigned int xid,
                                       unsigned int rep_xid, char **b, int *sz);

  void updatestat(unsigned int proc);

  // latest connection to the client
  std::map<unsigned int, connection *> conns_;

  // counting
  const int counting_;
  int curr_counts_;
  std::map<int, int> counts_;

  int lossytest_;
  bool reachable_;

  // map proc # to function
  std::map<int, handler *> procs_;

  std::mutex procs_m_;         // protect insert/delete to procs[]
  std::mutex count_m_;         // protect modification of counts
  std::mutex reply_window_m_;  // protect reply window et al
  std::mutex conss_m_;         // protect conns_

 protected:
  struct djob_t {
    djob_t(connection *c, char *b, int bsz) : buf(b), sz(bsz), conn(c) {}
    char *buf;
    int sz;
    connection *conn;
  };
  void dispatch(djob_t *);

  // internal handler registration
  void reg1(unsigned int proc, handler *);

  ThreadPool *dispatchpool_;
  tcpsconn *listener_;

 public:
  rpcs(unsigned int port, int counts = 0);
  ~rpcs();
  inline int port() { return listener_->port(); }
  // RPC handler for clients binding
  int rpcbind(int a, int &r);

  void set_reachable(bool r) { reachable_ = r; }

  bool got_pdu(connection *c, char *b, int sz);

  template<class S, class R, class... Args>
  void reg(unsigned int proc, S *sob, int (S::*meth)(Args..., R &r));

};

template<class S, class R, class... Args>
void reg(unsigned int proc, S *sob, int (S::*meth)(Args..., R &r)) {
  class h1 : public handler {
    private:
    S *sob;
    int (S::*meth)(const Args..., R &r);

    public:
    h1(S *xsob, int (S::*xmeth)(const Args..., R &r))
        : sob(xsob), meth(xmeth) {}

    int fn(unmarshall &args, marshall &ret) {
      std::tuple<Args...> args_tuple = unmarshall_args<Args...>(args);
      if (!args.okdone()) return rpc_const::unmarshal_args_failure;

      R r;
      int b = call_method(sob, meth, args_tuple, r);
      ret << r;
      return b;
    }

    template <size_t... I>
    int call_method_impl(S *sob, int (S::*meth)(const Args..., R &r),
                          const std::tuple<Args...> &args_tuple, R &r,
                          std::index_sequence<I...>) {
      return (sob->*meth)(std::get<I>(args_tuple)..., r);
    }

    int call_method(S *sob, int (S::*meth)(const Args..., R &r),
                    const std::tuple<Args...> &args_tuple, R &r) {
      return call_method_impl(sob, meth, args_tuple, r,
                              std::index_sequence_for<Args...>{});
    }

    template <class... A>
    std::tuple<A...> unmarshall_args(unmarshall &args) {
      std::tuple<A...> t;
      args >> ... >> t;
      return t;
    }
  };

  reg1(proc, new h1(sob, meth));
}

void make_sockaddr(const char *hostandport, struct sockaddr_in *dst);
void make_sockaddr(const char *host, const char *port, struct sockaddr_in *dst);

int cmp_timespec(const struct timespec &a, const struct timespec &b);
void add_timespec(const struct timespec &a, int b, struct timespec *result);
int diff_timespec(const struct timespec &a, const struct timespec &b);

} // namespace gxrpc