

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
#include "kudu/util/mem_tracker.h"
#include "kudu/util/once.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

using kudu::fs::WritableBlock;
using kudu::fs::ReadableBlock;
using kudu::fs::ScopedWritableBlockCloser;
using kudu::tablet::RowSetMetadata;

class CIndexFileWriter {
public:
  CIndexFileWriter(FsManager* fs, const ColumnSchema* col);
  ~CIndexFileWriter();

  Status Open();
  Status Append(rowid_t id, const void *entries, size_t count);
  Status FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer);
  size_t written_size() const;

  void GetFlushedBlocks(std::pair<BlockId, BlockId>& ret) const;

private:
  DISALLOW_COPY_AND_ASSIGN(CIndexFileWriter);

  Status Append(rowid_t id, const Slice* value);

  FsManager* fs_;
  const ColumnSchema* col_schema_;

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
  Status AppendBlock(const RowBlock& block);
  Status FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer);
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

  static Status Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                     std::shared_ptr<MemTracker> parent_mem_tracker,
                     const RowSetMetadata::BlockIdPair& block_ids,
                     std::unique_ptr<CIndexFileReader>* reader);

  Status NewIterator(Iterator** iter);

  Status GetColumnBounds(std::string* min_encoded_key,
                         std::string* max_encoded_key) const;

  ~CIndexFileReader();

private:
  friend class Iterator;

  DISALLOW_COPY_AND_ASSIGN(CIndexFileReader);

  CIndexFileReader(std::unique_ptr<CFileReader> key_reader,
                   std::unique_ptr<CFileReader> bitmap_reader,
                   const std::string& min_encoded_key,
                   const std::string& max_encoded_key);

  Status NewIterator(CFileIterator** key_iter, CFileIterator** bitmap_iter);

  std::unique_ptr<CFileReader> key_reader_;
  std::unique_ptr<CFileReader> bitmap_reader_;

  std::string min_encoded_key_;
  std::string max_encoded_key_;
};

class CIndexFileReader::Iterator {
public:
  ~Iterator();

  Status Init(const ColumnPredicate& pred, const TypeInfo* type_info);
  const Roaring& GetRoaring() const;

private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
  friend class CIndexFileReader;

  Iterator(CIndexFileReader* reader);

  CIndexFileReader* reader_;
  gscoped_ptr<CFileIterator> key_iter_;
  gscoped_ptr<CFileIterator> bitmap_iter_;
  Roaring c_;
};

class CMultiIndexFileReader {
public:
  class Iterator;

  static Status Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                     std::shared_ptr<MemTracker> parent_mem_tracker,
                     gscoped_ptr<CMultiIndexFileReader>* reader);

  Status NewIterator(Iterator** iter) const;

  Status GetColumnBounds(const ColumnId& col_id,
                         std::string* min_encoded_key,
                         std::string* max_encoded_key) const;

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

  Status Init(const Schema* projection, ScanSpec *spec);
  Status GetBounds(rowid_t* lower_bound_idx, rowid_t* upper_bound_idx);
  bool Exist(rowid_t row_idx) const;

  ~Iterator();

private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
  friend class CMultiIndexFileReader;

  Iterator(const CMultiIndexFileReader* reader);

  const CMultiIndexFileReader* reader_;

  typedef boost::container::flat_map<ColumnId, std::unique_ptr<CIndexFileReader::Iterator>> ColumnIdToReaderIterMap;
  ColumnIdToReaderIterMap reader_iters_;

  Roaring c_;
};

} // namespace cfile
} // namespace kudu

#endif
