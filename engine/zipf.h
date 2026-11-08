// Zipf sampler (inverse CDF over precomputed cumulative weights). theta=0 uniform; ~0.99 very hot.
#pragma once
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

namespace itx {

class ZipfGenerator {
public:
    ZipfGenerator(uint32_t n, double theta) : cdf_(n) {
        double sum = 0;
        for (uint32_t i = 0; i < n; ++i) {
            sum += 1.0 / std::pow(static_cast<double>(i + 1), theta);
            cdf_[i] = sum;
        }
        for (auto& c : cdf_) c /= sum;
    }
    uint32_t next(std::mt19937_64& rng) const {
        double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
        auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
        return static_cast<uint32_t>(std::min<size_t>(it - cdf_.begin(), cdf_.size() - 1));
    }

private:
    std::vector<double> cdf_;
};

}  // namespace itx
