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
#include <stdint.h>
#include <stdlib.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/indexfile.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(cfile_default_block_size);
DECLARE_int32(budgeted_compaction_target_rowset_size);

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cfile {

using tablet::KuduRowSetTest;
using tablet::RollingDiskRowSetWriter;
using tablet::RowSetMetadataVector;

const int kNumRows = 100000;
class TestIndexFile: public KuduRowSetTest {
public:
  TestIndexFile()
    : KuduRowSetTest(Schema( \
      { ColumnSchema("c0", INT32),
        ColumnSchema("c1", STRING, false, nullptr, nullptr, "bitmap_index"),
        ColumnSchema("c2", INT32, false, nullptr, nullptr, "bitmap_index"),
        ColumnSchema("c3", STRING) }, 2)) {
    }
  virtual ~TestIndexFile() {}

  virtual void SetUp() OVERRIDE {
    KuduRowSetTest::SetUp();
    SeedRandom();
    PrepareTestData(kNumRows);
  }

  CMultiIndexFileReader::Iterator* NewIterator() {
    CHECK(reader_);
    CMultiIndexFileReader::Iterator* iter;
    CHECK_OK(reader_->NewIterator(&iter));
    return iter;
  }

protected:
private:
  void PrepareTestData(int nrows) {
    RollingDiskRowSetWriter drsw(tablet()->metadata(), schema_,
                                 BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f),
                                 FLAGS_budgeted_compaction_target_rowset_size);
    ASSERT_OK(drsw.Open());
    WriteTestRowSet(nrows, &drsw);
    //LOG(INFO) << "base written size {" << drsw.drs_written_count()
    //          << "," << drsw.written_size() << "}";
    RowSetMetadataVector metas;
    drsw.GetWrittenRowSetMetadata(&metas);
    ASSERT_EQ(1, metas.size());
    ASSERT_OK(CMultiIndexFileReader::Open(metas[0],
        MemTracker::GetRootTracker(), &reader_));
  }

  template<class WriterClass>
  void WriteTestRowSet(int nrows, WriterClass *writer) {
    RowBuilder rb(schema_);
    for (int i = 0; i < nrows; i++) {
      rb.Reset();
      rb.AddInt32(i);
      rb.AddString(Substitute("$0", i * 2));
      rb.AddInt32(i * 3);
      rb.AddString(Substitute("$0", i * 4));
      ASSERT_OK_FAST(WriteRow(rb.data(), writer));
    }
    ASSERT_OK(writer->Finish());
  }

  gscoped_ptr<CMultiIndexFileReader> reader_;
};

TEST_F(TestIndexFile, TestNoPredicate) {
  ScanSpec spec;
  DeltaStats stats;
  rowid_t lower = 0;
  rowid_t upper = 0;
  int32_t value = 0;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  // "c0" & "c3"
  spec.AddPredicate(ColumnPredicate::Equality(schema_.column(0), &value));
  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(3)));
  iter.reset(NewIterator());
  ASSERT_OK(iter->Init(&spec, &schema_, stats));
  ASSERT_EQ(2, spec.predicates().size());
  ASSERT_FALSE(iter->HasValidBitmap());
  ASSERT_OK(iter->GetBounds(&lower, &upper));
  ASSERT_EQ(0, lower);
  ASSERT_EQ(0, upper);
}

TEST_F(TestIndexFile, TestEqualityPredicate) {
  ScanSpec spec;
  DeltaStats stats;
  rowid_t lower = 0;
  rowid_t upper = 0;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  const int idx = rand() % kNumRows;
  string tmp = Substitute("$0", idx * 2);
  const Slice c1(tmp);
  const int c2 = idx * 3;

  // "c1"
  {
    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::Equality(schema_.column(1), &c1));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    ASSERT_EQ(idx, lower);
    ASSERT_EQ(idx+1, upper);
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    ASSERT_EQ(1, sel_vec.CountSelected());
  }

  // "c2"
  {
    lower = 0;
    upper = 0;
    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::Equality(schema_.column(2), &c2));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    ASSERT_EQ(idx, lower);
    ASSERT_EQ(idx+1, upper);
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    ASSERT_EQ(1, sel_vec.CountSelected());
  }
}

TEST_F(TestIndexFile, TestInListPredicate) {
  ScanSpec spec;
  DeltaStats stats;
  rowid_t lower = 0;
  rowid_t upper = 0;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  Arena arena(32*1024, 1*1024*1024);
  vector<int> idxs;
  int kNum = rand() % kNumRows;
  idxs.reserve(kNum);
  for (int i = 0; i < kNum; ++i) {
    idxs.push_back(rand() % kNumRows);
  }
  std::sort(idxs.begin(), idxs.end());
  idxs.erase(std::unique(idxs.begin(), idxs.end()), idxs.end());
  kNum = idxs.size();
  const int min = *min_element(idxs.begin(), idxs.end());
  const int max = *max_element(idxs.begin(), idxs.end());

  vector<string> vecStrC1;
  vector<int> vecIntC2;
  vector<const void*> c1;
  vector<const void*> c2;
  vecStrC1.reserve(kNum);
  vecIntC2.reserve(kNum);
  c1.reserve(kNum);
  c2.reserve(kNum);
  for (int i = 0; i < kNum; ++i) {
    // "c1"
    vecStrC1.push_back(Substitute("$0", idxs[i] * 2));
    c1.push_back(arena.NewObject<Slice>(vecStrC1[i]));
    // "c2"
    vecIntC2.push_back(idxs[i] * 3);
    c2.push_back(arena.NewObject<int32_t>(vecIntC2[i]));
  }

  // "c1"
  {
    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::InList(schema_.column(1), &c1));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    ASSERT_EQ(min, lower);
    ASSERT_EQ(max+1, upper);
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    ASSERT_EQ(kNum, sel_vec.CountSelected());
    for (int i = 0; i < kNum; ++i) {
      ASSERT_TRUE(sel_vec.IsRowSelected(idxs[i]));
    }
  }

  // "c2"
  {
    lower = 0;
    upper = 0;
    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::InList(schema_.column(2), &c2));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    ASSERT_EQ(min, lower);
    ASSERT_EQ(max+1, upper);
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    ASSERT_EQ(kNum, sel_vec.CountSelected());
    for (int i = 0; i < kNum; ++i) {
      ASSERT_TRUE(sel_vec.IsRowSelected(idxs[i]));
    }
  }
}

TEST_F(TestIndexFile, TestRangePredicate) {
  ScanSpec spec;
  DeltaStats stats;
  rowid_t lower = 0;
  rowid_t upper = 0;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  int min = rand() % kNumRows;
  int max = rand() % kNumRows;
  while (max <= min) {
    max = rand() % kNumRows;
  }
  /*min = 0;
  max = kNumRows;*/
  //LOG(INFO) << "R : " << min << "," << max;

  // "c1"
  {
    // The sequence of Alphabet is not the same with Number.
    string strMin = Substitute("$0", min * 2);
    string strMax = Substitute("$0", max * 2);
    if (strMin > strMax) {
      string tmp = strMin;
      strMin = strMax;
      strMax = tmp;
    }
    Slice c1_lower(strMin);
    Slice c1_upper(strMax);
    //LOG(INFO) << "C1: " << c1_lower.ToString() << "," << c1_upper.ToString();

    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::Range(schema_.column(1), &c1_lower, &c1_upper));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    SelectionVectorView view(&sel_vec);
    for (int i = lower; i < upper; ++i) {
      if (view.TestBit(i)) {
        string str = Substitute("$0", i * 2);
        ASSERT_TRUE(str >= strMin);
        ASSERT_TRUE(str < strMax);
      }
    }
  }

  // "c2"
  {
    int c2_lower = min * 3;
    int c2_upper = max * 3;
    //LOG(INFO) << "C2: " << c2_lower << "," << c2_upper;

    lower = 0;
    upper = 0;
    spec.RemovePredicates();
    spec.AddPredicate(ColumnPredicate::Range(schema_.column(2), &c2_lower, &c2_upper));
    iter.reset(NewIterator());
    ASSERT_OK(iter->Init(&spec, &schema_, stats));
    ASSERT_EQ(0, spec.predicates().size());
    ASSERT_TRUE(iter->HasValidBitmap());
    ASSERT_OK(iter->GetBounds(&lower, &upper));
    ASSERT_EQ(min, lower);
    ASSERT_EQ(max, upper);
    SelectionVector sel_vec(upper);
    sel_vec.SetAllFalse();
    iter->InitializeSelectionVector(0, &sel_vec);
    ASSERT_EQ(max - min, sel_vec.CountSelected());
    for (int i = lower; i < upper; ++i) {
      ASSERT_TRUE(sel_vec.IsRowSelected(i));
    }
  }
  LOG(INFO) << "finish!";
}

TEST_F(TestIndexFile, TestNullAndNotNullPredicate) {
  ScanSpec spec;
  DeltaStats stats;
  rowid_t lower = 0;
  rowid_t upper = 0;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(1)));
  spec.AddPredicate(ColumnPredicate::IsNull(schema_.column(2)));
  iter.reset(NewIterator());
  ASSERT_OK(iter->Init(&spec, &schema_, stats));
  ASSERT_EQ(2, spec.predicates().size());
  ASSERT_FALSE(iter->HasValidBitmap());
  ASSERT_OK(iter->GetBounds(&lower, &upper));
  ASSERT_EQ(0, lower);
  ASSERT_EQ(0, upper);
}

TEST_F(TestIndexFile, TestDeltaStats) {
  ScanSpec spec;
  DeltaStats stats;
  unique_ptr<CMultiIndexFileReader::Iterator> iter;

  const int idx = rand() % kNumRows;
  string tmp = Substitute("$0", idx * 2);
  const Slice c1(tmp);
  const int c2 = idx * 3;

  stats.IncrUpdateCount(schema_.column_id(1), 1);
  stats.IncrUpdateCount(schema_.column_id(2), 1);

  // "c1" & "c2"
  spec.AddPredicate(ColumnPredicate::Equality(schema_.column(1), &c1));
  spec.AddPredicate(ColumnPredicate::Equality(schema_.column(2), &c2));
  iter.reset(NewIterator());
  ASSERT_OK(iter->Init(&spec, &schema_, stats));
  ASSERT_EQ(2, spec.predicates().size());
  ASSERT_FALSE(iter->HasValidBitmap());
}

} // namespace tablet
} // namespace kudu
