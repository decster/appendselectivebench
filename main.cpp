#include <iostream>
#include <stdint.h>
#include <vector>
#include <random>
#include <memory>
#include <string.h>
#include <benchmark/benchmark.h>

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define PREFETCH(addr) __builtin_prefetch(addr)

using namespace std;

class BinaryColumnBase {
public:
    vector<int32_t> _offsets;
    vector<uint8_t> _bytes;
    BinaryColumnBase() {
        _offsets.emplace_back(0);
    }

    void reset() {
        _offsets.clear();
        _bytes.clear();
        _offsets.emplace_back(0);
    }

    void gen(size_t avglen, size_t n) {
        _offsets.resize(n + 1);
        _bytes.resize(avglen * n);
        for (size_t i = 0; i < n; i++) {
            _offsets[i] = i * avglen;
        }
        _offsets[n] = n * avglen;
    }

    vector<uint32_t> gen_indexes(size_t seed, size_t m) {
        size_t n = _offsets.size() - 1;
        std::vector<int> allIntegers(n);
        for (int i = 0; i < n; ++i) {
            allIntegers[i] = i;
        }

        std::srand(seed);
        vector<uint32_t> ret;
        for (int i = n - 1; i >= 0 && m > 0; --i) {
            int randomIndex = std::rand() % (i + 1);
            std::swap(allIntegers[i], allIntegers[randomIndex]);
            ret.push_back(allIntegers[i]);
            --m;
        }
        sort(ret.begin(), ret.end());
        return ret;
    }

    void append_selective(const BinaryColumnBase& src, const uint32_t* indexes, uint32_t from, uint32_t size);

    void append_selective_prefetch(const BinaryColumnBase& src, const uint32_t* indexes, uint32_t from, uint32_t size);

};

void BinaryColumnBase::append_selective(const BinaryColumnBase& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_column = src;
    const auto& src_offsets = src_column._offsets;
    const auto& src_bytes = src_column._bytes;

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        auto str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (size_t i = 0; i < size; i++) {
        auto row_idx = indexes[from + i];
        auto str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        memcpy(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
                                str_size);
    }
}

void BinaryColumnBase::append_selective_prefetch(const BinaryColumnBase& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_column = src;
    const auto& src_offsets = src_column._offsets;
    const auto& src_bytes = src_column._bytes;

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        auto str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (size_t i = 0; i < size; i++) {
        auto row_idx = indexes[from + i];
        auto str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        uint32_t prefetch_i = i + 1;
        if (LIKELY(prefetch_i < size)) {
            auto prefetch_row_idx = indexes[from + prefetch_i];
            PREFETCH(src_bytes.data() + src_offsets[prefetch_row_idx]);
        }
        memcpy(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
               str_size);
    }
}

unique_ptr<BinaryColumnBase> src_column;
unique_ptr<BinaryColumnBase> dest_column;
vector<uint32_t> indexes;

static void BM_AppendSelective(benchmark::State& state) {
    src_column.reset(new BinaryColumnBase());
    dest_column.reset(new BinaryColumnBase());
    size_t bytes = 1024*1024*256;
    size_t type = state.range(0);
    size_t selectivity = state.range(1);
    size_t avglen = state.range(2);
    size_t total_row = bytes/avglen;
    size_t select_row = total_row / selectivity;
    string label = "row:" + to_string(total_row) + "_select:" + to_string(select_row) + "_keylen:" + to_string(avglen) + string(type == 0 ? " orig" : " prefetch");
    src_column->gen(avglen, total_row);
    indexes = src_column->gen_indexes(0, select_row);
    state.SetLabel(label);
    if (type == 0) {
        for (auto _ : state) {
            dest_column->append_selective(*src_column, indexes.data(), 0, indexes.size());
            dest_column->reset();
        }
    } else {
        for (auto _ : state) {
            dest_column->append_selective_prefetch(*src_column, indexes.data(), 0, indexes.size());
            dest_column->reset();
        }
    }
    src_column.reset();
    dest_column.reset();
    indexes.clear();
}

BENCHMARK(BM_AppendSelective)
    ->ArgsProduct({
        {1, 0},
        {10, 100, 1000},
        {8, 16, 32, 64},
    });

BENCHMARK_MAIN();
