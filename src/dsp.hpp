#pragma once

#include <cstddef>

// Single-pass simple moving average (SMA) over `count` input samples with
// the given window size. O(n) via a running sum -- each sample is added
// once and, once the window fills, the sample sliding out is subtracted
// once. Early samples (i < window) average over a shrinking window
// instead of being padded with zeros, so the filter responds immediately.
inline void moving_average(const float* input, float* output, std::size_t count,
                            std::size_t window) {
    if (window == 0) window = 1;

    float sum = 0.0f;
    for (std::size_t i = 0; i < count; ++i) {
        sum += input[i];
        if (i >= window) {
            sum -= input[i - window];
        }
        std::size_t n = (i + 1 < window) ? (i + 1) : window;
        output[i] = sum / static_cast<float>(n);
    }
}
