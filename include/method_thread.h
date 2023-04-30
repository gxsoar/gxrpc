#pragma once

// method_thread(): start a thread that runs an object method.
// returns a pthread_t on success, and zero on error.

#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tuple>
#include <type_traits>

#include "verify.h"

namespace gxrpc {
template <class C, typename M, typename... Args>
auto method_thread_parent(C* object, M method, Args&&... args) {
    using ReturnType = std::result_of_t<M(C*, Args...)>;

    // 使用 std::tuple 存储参数，以便在新线程中调用成员函数
    auto arguments = std::make_tuple(object, std::forward<Args>(args)...);

    // 创建一个 lambda 函数并捕获 arguments tuple
    auto thread_func = [method, arguments = std::move(arguments)]() mutable {
        std::apply([method](C *o, auto&&... args) { (o->*method)(std::forward<decltype(args)>(args)...); }, 
                   std::move(arguments));
    };

    return std::thread(thread_func);
}

template <class C, class R, typename... Args>
std::thread method_thread(C *object, R (C::*method)(Args...), Args&&... args) {
    return method_thread_parent(object, method, std::forward<Args>(args)...);
}
} // namespace gxrpc