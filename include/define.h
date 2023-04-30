#pragma once

#include <functional>

using Task = std::function<void(void*)>;

template <int A, int B>
struct static_max {
  static const int value = A > B ? A : B;
};

template <int A, int B>
struct static_min {
  static const int value = A < B ? A : B;
};