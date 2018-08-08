

#ifndef KUDU_CFILE_INDEXFILE_H
#define KUDU_CFILE_INDEXFILE_H

#include <memory>
#include <string>
#include <map>

#include <boost/container/flat_map.hpp>

#include <roaring/roaring.hh>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/schema.h"
#include "kudu/common/scan_spec.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/once.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

using kudu::fs::WritableBlock;
using kudu::fs::ReadableBlock;
using kudu::fs::ScopedWritableBlockCloser;
using kudu::tablet::RowSetMetadata;
using kudu::tablet::DeltaStats;

class CIndexFileWriter {
public:
  CIndexFileWriter(FsManager* fs, const ColumnSchema* col);
  ~CIndexFileWriter();

  Status Open();
  Status Append(rowid_t id, const void* entries, size_t count);
  Status FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer);
  size_t written_size() const;

  Status GetFlushedBlocks(std::pair<BlockId, BlockId>& ret) const;

private:
  DISALLOW_COPY_AND_ASSIGN(CIndexFileWriter);

  Status Append(const std::string& entry, rowid_t id);

  FsManager* fs_;
  const ColumnSchema* col_schema_;
  bool has_finished_;

  BlockId key_block_id_;
  BlockId bitmap_block_id_;
  gscoped_ptr<cfile::CFileWriter> key_writer_;
  gscoped_ptr<cfile::CFileWriter> bitmap_writer_;

  typedef std::map<std::string, std::unique_ptr<Roaring>> KeyToRoaringMap;
  KeyToRoaringMap map_;
};

class CMultiIndexFileWriter {
public:
  CMultiIndexFileWriter(FsManager* fs, const Schema* schema);
  ~CMultiIndexFileWriter();

  Status Open();
  Status AppendBlock(const DeltaStats* redo_delta_stats,
                     const RowBlock& block);
  Status Finish(const DeltaStats* redo_delta_stats);
  Status FinishAndReleaseBlocks(const DeltaStats* redo_delta_stats,
                                ScopedWritableBlockCloser* closer);
  size_t written_size() const;

  void GetFlushedBlocksByColumnId(std::map<ColumnId, std::pair<BlockId, BlockId> >* ret) const;

private:
  FsManager* const fs_;
  const Schema* const schema_;

  typedef boost::container::flat_map<ColumnId, std::unique_ptr<CIndexFileWriter>> ColumnIdToWriterMap;
  ColumnIdToWriterMap writers_;
  rowid_t written_count_;
};

class CIndexFileReader {
public:
  class Iterator;

  static Status Open(FsManager* fs,
                     std::shared_ptr<MemTracker> parent_mem_tracker,
                     const RowSetMetadata::BlockIdPair& block_ids,
                     std::unique_ptr<CIndexFileReader>* reader);

  Status NewIterator(Iterator** iter);

  ~CIndexFileReader();

private:
  friend class Iterator;

  DISALLOW_COPY_AND_ASSIGN(CIndexFileReader);

  CIndexFileReader(std::unique_ptr<CFileReader> key_reader,
                   std::unique_ptr<CFileReader> bitmap_reader);

  Status NewIterator(CFileIterator** key_iter, CFileIterator** bitmap_iter);

  std::unique_ptr<CFileReader> key_reader_;
  std::unique_ptr<CFileReader> bitmap_reader_;
};

class CIndexFileReader::Iterator {
public:
  ~Iterator();

  Status Init();
  Status Pushdown(const ColumnPredicate& predicate, Roaring& r);

private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
  friend class CIndexFileReader;

  Iterator(CIndexFileReader* reader);
  Status PushdownEquaility(const ColumnSchema& col_schema, const vector<const void*>& values, Roaring& r);
  Status PushdownRange(const ColumnSchema& col_schema, const ColumnPredicate& predicate, Roaring& r);

  CIndexFileReader* reader_;
  gscoped_ptr<CFileIterator> key_iter_;
  gscoped_ptr<CFileIterator> bitmap_iter_;
};

class CMultiIndexFileReader {
public:
  class Iterator;

  static Status Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                     std::shared_ptr<MemTracker> parent_mem_tracker,
                     gscoped_ptr<CMultiIndexFileReader>* reader);

  Status NewIterator(Iterator** iter) const;

  ~CMultiIndexFileReader();

private:
  friend class Iterator;

  DISALLOW_COPY_AND_ASSIGN(CMultiIndexFileReader);

  CMultiIndexFileReader(std::shared_ptr<RowSetMetadata> rowset_metadata,
                        std::shared_ptr<MemTracker> parent_mem_tracker);
  Status DoOpen();

  std::shared_ptr<RowSetMetadata> rowset_metadata_;
  std::shared_ptr<MemTracker> parent_mem_tracker_;

  typedef boost::container::flat_map<ColumnId, std::unique_ptr<CIndexFileReader>> ColumnIdToReaderMap;
  ColumnIdToReaderMap readers_;
};

class CMultiIndexFileReader::Iterator {
public:

  Status Init(ScanSpec* spec, const Schema* projection, const DeltaStats& stats);
  Status GetBounds(rowid_t* lower_bound_idx, rowid_t* upper_bound_idx);

  inline bool HasValidBitmap() const {
    if (bHasResult_ && !c_.isEmpty())
      return true;
    return false;
  }
  inline void InitializeSelectionVector(rowid_t cur_idx, SelectionVector *sel_vec) {
    uint32_t* arr = arr_.get();
    for (; arr_i_ < arr_n_; ++arr_i_) {
      if (PREDICT_FALSE(arr[arr_i_] >= (cur_idx+sel_vec->nrows()))) break;
      sel_vec->SetRowSelected(arr[arr_i_] - cur_idx);
      //LOG(INFO) << "set row selected:" << arr[arr_i_] - cur_idx;
    }
    return;
  }

  ~Iterator();

private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
  friend class CMultiIndexFileReader;

  Iterator(const CMultiIndexFileReader* reader);

  const CMultiIndexFileReader* reader_;

  typedef boost::container::flat_map<ColumnId, std::unique_ptr<CIndexFileReader::Iterator>> ColumnIdToReaderIterMap;
  ColumnIdToReaderIterMap reader_iters_;

  // bHasResult_ equals to false: the result is empty definitively.
  // bHasResult_ equals to true : maybe the c_ is still empty when
  //    1) the only one predicate is IsNotNull;
  //    2) the only one predicate has no index yet;
  bool bHasResult_;
  Roaring c_;
  uint64_t arr_n_;
  uint64_t arr_i_;
  std::unique_ptr<uint32_t> arr_;
};

} // namespace cfile
} // namespace kudu

#endif
