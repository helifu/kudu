// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/common/bloomfilter/bloom-filter.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "kudu/util/test_util.h"
#include "kudu/util/memory/arena.h"

using namespace std;

namespace {

using namespace impala_kudu;

class TestImpalaBloomFilter: public KuduTest {};

// Make a random uint64_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint64_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// BfInsert() and BfFind() are like BloomFilter::{Insert,Find}, except they randomly
// disable AVX2 instructions half of the time. These are used for testing that AVX2
// machines and non-AVX2 machines produce compatible BloomFilters.
void BfInsert(BloomFilter& bf, uint32_t h) {
  if (MakeRand() & 0x1) {
    bf.Insert(h);
  } else {
    CpuInfo::TempDisable t1(CpuInfo::AVX2);
    bf.Insert(h);
  }
}

bool BfFind(BloomFilter& bf, uint32_t h) {
  if (MakeRand() & 0x1) {
    return bf.Find(h);
  } else {
    CpuInfo::TempDisable t1(CpuInfo::AVX2);
    return bf.Find(h);
  }
}

// Computes union of 'x' and 'y'. Computes twice with AVX enabled and disabled and
// verifies both produce the same result. 'success' is set to true if both union
// computations returned the same result and set to false otherwise.
void BfUnion(const BloomFilter* x, BloomFilter* y, bool* success) {
  BloomFilterPB pb_x;
  BloomFilterPB pb_y;
  BloomFilter::ToPB(x, &pb_x);
  BloomFilter::ToPB(y, &pb_y);

  Arena arena(1024, 1024*1024);
  uint8_t* directory = static_cast<uint8_t*>(arena.AllocateBytesAligned(pb_x.directory().size(), 16));
  memcpy(directory, pb_x.directory().data(), pb_x.directory().size());
  BloomFilter* x1 = arena.NewObject<BloomFilter>(pb_x.log_heap_space(), directory);
  /*directory = static_cast<uint8_t*>(arena.AllocateBytesAligned(pb_y.directory().size(), 16));
  memcpy(directory, pb_y.directory().data(), pb_y.directory().size());
  BloomFilter* y1 = arena.NewObject<BloomFilter>(pb_y.log_heap_space(), directory);*/
  BloomFilter::Or(x1, y);
  {
    CpuInfo::TempDisable t1(CpuInfo::AVX);
    CpuInfo::TempDisable t2(CpuInfo::AVX2);
    directory = static_cast<uint8_t*>(arena.AllocateBytesAligned(pb_x.directory().size(), 16));
    memcpy(directory, pb_x.directory().data(), pb_x.directory().size());
    BloomFilter* x2 = arena.NewObject<BloomFilter>(pb_x.log_heap_space(), directory);
    directory = static_cast<uint8_t*>(arena.AllocateBytesAligned(pb_y.directory().size(), 16));
    memcpy(directory, pb_y.directory().data(), pb_y.directory().size());
    BloomFilter* y2 = arena.NewObject<BloomFilter>(pb_y.log_heap_space(), directory);
    BloomFilter::Or(x2, y2);
    *success = ((*y) == (*y2));
  }
  return;
}
}  // namespace

namespace impala {

// We can construct (and destruct) Bloom filters with different spaces.
TEST(TestImpalaBloomFilter, Constructor) {
  for (int i = 0; i < 30; ++i) {
    BloomFilter bf(i);
  }
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST(TestImpalaBloomFilter, Insert) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter bf(i);
    for (int k = 0; k < (1 << 15); ++k) {
      BfInsert(bf, MakeRand());
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again immediately.
TEST(TestImpalaBloomFilter, Find) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter bf(i);
    for (int k = 0; k < (1 << 15); ++k) {
      const uint64_t to_insert = MakeRand();
      BfInsert(bf, to_insert);
      EXPECT_TRUE(BfFind(bf, to_insert));
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again much later.
TEST(TestImpalaBloomFilter, CumulativeFind) {
  srand(0);
  for (int i = 5; i < 11; ++i) {
    std::vector<uint32_t> inserted;
    BloomFilter bf(i);
    for (int k = 0; k < (1 << 10); ++k) {
      const uint32_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      BfInsert(bf, to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(BfFind(bf, inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST(TestImpalaBloomFilter, FindInvalid) {
  srand(0);
  static const int find_limit = 1 << 20;
  unordered_set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(MakeRand());
  }
  static const int max_log_ndv = 19;
  unordered_set<uint32_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    const auto candidate = MakeRand();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 15; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_heap_space = BloomFilter::MinLogSpace(ndv, fpp);
      BloomFilter bf(log_heap_space);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        BfInsert(bf, shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += BfFind(bf, i);
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp = BloomFilter::FalsePositiveProb(ndv, log_heap_space);
      EXPECT_GE(found, find_limit * expected_fpp)
          << "Too few false positives with -log2(fpp) = " << log_fpp;
      EXPECT_LE(found, find_limit * expected_fpp * 8)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
    }
  }
}

// Test that MaxNdv() and MinLogSpace() are dual
TEST(TestImpalaBloomFilter, MinSpaceMaxNdv) {
  for (int log2fpp = -2; log2fpp >= -63; --log2fpp) {
    const double fpp = pow(2, log2fpp);
    for (int given_log_space = 8; given_log_space < 30; ++given_log_space) {
      const size_t derived_ndv = BloomFilter::MaxNdv(given_log_space, fpp);
      int derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp);

      EXPECT_EQ(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;

      // If we lower the fpp, we need more space; if we raise it we need less.
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp / 2);
      EXPECT_GE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp * 2);
      EXPECT_LE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
    }
    for (size_t given_ndv = 1000; given_ndv < 1000 * 1000; given_ndv *= 3) {
      const int derived_log_space = BloomFilter::MinLogSpace(given_ndv, fpp);
      const size_t derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp);

      // The max ndv is close to, but larger than, then ndv we asked for
      EXPECT_LE(given_ndv, derived_ndv) << "fpp: " << fpp
                                        << " derived_log_space: " << derived_log_space;
      EXPECT_GE(given_ndv * 2, derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;

      // Changing the fpp changes the ndv capacity in the expected direction.
      size_t new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp / 2);
      EXPECT_GE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
      new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp * 2);
      EXPECT_LE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
    }
  }
}

TEST(TestImpalaBloomFilter, MinSpaceEdgeCase) {
  int min_space = BloomFilter::MinLogSpace(1, 0.75);
  EXPECT_GE(min_space, 0) << "LogSpace should always be >= 0";
}

// Check that MinLogSpace() and FalsePositiveProb() are dual
TEST(TestImpalaBloomFilter, MinSpaceForFpp) {
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.01) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.99) { // NOLINT: loop on double
      // When contructing a Bloom filter, we can request a particular fpp by calling
      // MinLogSpace().
      const int min_log_space = BloomFilter::MinLogSpace(ndv, fpp);
      // However, at the resulting ndv and space, the expected fpp might be lower than
      // the one that was requested.
      double expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space);
      EXPECT_LE(expected_fpp, fpp);
      // The fpp we get might be much lower than the one we asked for. However, if the
      // space were just one size smaller, the fpp we get would be larger than the one we
      // asked for.
      expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space - 1);
      EXPECT_GE(expected_fpp, fpp);
      // Therefore, the return value of MinLogSpace() is actually the minimum
      // log space at which we can guarantee the requested fpp.
    }
  }
}

TEST(TestImpalaBloomFilter, protobuf) {
  BloomFilter bf(BloomFilter::MinLogSpace(100, 0.01));
  for (int i = 0; i < 10; ++i) BfInsert(bf, i);
  // Check no unexpected new false positives.
  unordered_set<int> missing_ints;
  for (int i = 11; i < 100; ++i) {
    if (!BfFind(bf, i)) missing_ints.insert(i);
  }

  kudu::BloomFilterPB pb;
  BloomFilter::ToPB(&bf, &pb);

  // New a impala_kudu::BloomFilter object.
  Arena arena(1024, 1024*1024);
  uint8_t* buffer = static_cast<uint8_t*>(arena.AllocateBytesAligned(pb.directory().size(), 16/*64*/));
  memcpy(buffer, pb.directory().data(), pb.directory().size());
  BloomFilter* from_thrift = arena.NewObject<impala_kudu::BloomFilter>(pb.log_heap_space(), buffer);

  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*from_thrift, i));
  for (int missing: missing_ints) ASSERT_FALSE(BfFind(*from_thrift, missing));
}

TEST(TestImpalaBloomFilter, BloomFilterOr) {
  BloomFilter bf1(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter bf2(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(bf1, i);

  bool success;
  BfUnion(&bf1, &bf2, &success);
  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(bf2, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(bf2, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(bf2, i);

  // Apply back to BloomFilter and verify if aggregation was correct.
  BfUnion(&bf1, &bf2, &success);
  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(bf2, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(bf2, i)) << i;
  ASSERT_FALSE(BfFind(bf2, 81));
}

}  // namespace impala
