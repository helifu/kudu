
#include <memory>

#include "kudu/cfile/indexfile.h"
#include "kudu/util/flag_tags.h"
#include "kudu/tablet/diskrowset.h"


DEFINE_int32(default_index_block_size_bytes, 4096,
             "Block size used for index indexes.");
TAG_FLAG(default_index_block_size_bytes, experimental);

namespace kudu {
namespace cfile {

using strings::Substitute;

CIndexFileWriterBase::CIndexFileWriterBase() {
}

CIndexFileWriterBase::~CIndexFileWriterBase() {
}

//////////////////////////////////////////////////////////////////////////
template <typename T>
CIndexFileWriterT<T>::CIndexFileWriterT(FsManager* fs, const ColumnSchema* col_schema)
: fs_(fs)
, col_schema_(col_schema)
, has_finished_(false) {
}

template <typename T>
CIndexFileWriterT<T>::~CIndexFileWriterT() {
}

template <typename T>
Status CIndexFileWriterT<T>::Open() {
  CHECK(col_schema_->is_indexed());
  LOG(INFO) << "Opened index writer for " << col_schema_->name();

  // Key writer
  std::unique_ptr<WritableBlock> key_block;
  RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&key_block),
        "unable to create key block for column " + col_schema_->ToString());
  key_block_id_.SetId(key_block->id().id());
  cfile::WriterOptions key_opts;
  key_opts.write_posidx = false;
  key_opts.write_validx = true;
  key_opts.storage_attributes = col_schema_->attributes();
  key_opts.storage_attributes.compression = NO_COMPRESSION;
  key_opts.storage_attributes.cfile_block_size = FLAGS_default_index_block_size_bytes;
  key_writer_.reset(new CFileWriter(key_opts, col_schema_->type_info(), false, std::move(key_block)));
  RETURN_NOT_OK_PREPEND(key_writer_->Start(), 
        "unable to start key writer for column " + col_schema_->ToString());

  // Bitmap writer
  std::unique_ptr<WritableBlock> bitmap_block;
  RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&bitmap_block),
        "unable to create bitmap block for column " + col_schema_->ToString());
  bitmap_block_id_.SetId(bitmap_block->id().id());
  cfile::WriterOptions bitmap_opts;
  bitmap_opts.write_posidx = true;
  bitmap_opts.write_validx = false;
  bitmap_opts.storage_attributes.encoding = PLAIN_ENCODING;
  bitmap_opts.storage_attributes.compression = NO_COMPRESSION;
  bitmap_writer_.reset(new CFileWriter(bitmap_opts, GetTypeInfo(BINARY), false, std::move(bitmap_block)));
  RETURN_NOT_OK_PREPEND(bitmap_writer_->Start(), 
        "unable to start bitmap writer for column " + col_schema_->ToString());

  LOG(INFO) << "Create index blocks {" << key_block_id_ << "," << bitmap_block_id_ << "}";
  return Status::OK();
}

template <typename T>
Status CIndexFileWriterT<T>::Append(rowid_t id, const void* entries, size_t count) {
  const T* vals = reinterpret_cast<const T*>(entries);
  while (count--) {
    typename KeyToRoaringMap::iterator iter = map_.find(*vals);
    if (iter == map_.end()) {
      std::unique_ptr<Roaring> r(new Roaring());
      iter = map_.insert(typename KeyToRoaringMap::value_type(*vals, std::move(r))).first;
    }
    iter->second->add(id++);
    vals++;
  }

  return Status::OK();
}

template <typename T>
Status CIndexFileWriterT<T>::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  // Flush to Disk.
  Arena arena(32*1024, 1*1024*1024);
  for (const auto& e : map_) {
    T key = e.first;
    Roaring* r = e.second.get();
    r->runOptimize();
    size_t size = r->getSizeInBytes(false);
    char* buff = static_cast<char*>(arena.AllocateBytes(size));
    r->write(buff, false);
    Slice bitmap(buff, size);

    RETURN_NOT_OK(key_writer_->AppendEntries(&key, 1));
    RETURN_NOT_OK(bitmap_writer_->AppendEntries(&bitmap, 1));
  }

  // Finish writer.
  RETURN_NOT_OK_PREPEND(key_writer_->FinishAndReleaseBlock(closer),
        "unable to finish key_writer for column " + col_schema_->ToString());
  RETURN_NOT_OK_PREPEND(bitmap_writer_->FinishAndReleaseBlock(closer),
        "unable to finish bitmap_writer for column " + col_schema_->ToString());

  LOG(INFO) << "Closed index writer for " << col_schema_->name()
            << " with size{"<< key_writer_->written_size()
            << "," << bitmap_writer_->written_size() << "}";

  has_finished_ = true;
  return Status::OK();
}

template <typename T>
size_t CIndexFileWriterT<T>::written_size() const {
  if (!has_finished_) return 0;
  return key_writer_->written_size() + bitmap_writer_->written_size();
}

template <typename T>
Status CIndexFileWriterT<T>::GetFlushedBlocks(std::pair<BlockId, BlockId>& ret) const {
  if (!has_finished_) {
    return Status::Aborted("not finished");
  }

  CHECK(!key_block_id_.IsNull());
  CHECK(!bitmap_block_id_.IsNull());

  ret.first = key_block_id_;
  ret.second = bitmap_block_id_;
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CIndexFileWriterBinary::CIndexFileWriterBinary(FsManager* fs, const ColumnSchema* col_schema)
  : fs_(fs)
  , col_schema_(col_schema)
  , has_finished_(false)
  , arena_(new Arena(32*1024, 1*1024*1024)) {
}

CIndexFileWriterBinary::~CIndexFileWriterBinary() {
}

Status CIndexFileWriterBinary::Open() {
  CHECK(col_schema_->is_indexed());
  LOG(INFO) << "Opened index writer for " << col_schema_->name();

  // Key writer
  std::unique_ptr<WritableBlock> key_block;
  RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&key_block),
    "unable to create key block for column " + col_schema_->ToString());
  key_block_id_.SetId(key_block->id().id());
  cfile::WriterOptions key_opts;
  key_opts.write_posidx = false;
  key_opts.write_validx = true;
  key_opts.storage_attributes = col_schema_->attributes();
  key_opts.storage_attributes.compression = NO_COMPRESSION;
  key_opts.storage_attributes.cfile_block_size = FLAGS_default_index_block_size_bytes;
  key_writer_.reset(new CFileWriter(key_opts, col_schema_->type_info(), false, std::move(key_block)));
  RETURN_NOT_OK_PREPEND(key_writer_->Start(), 
    "unable to start key writer for column " + col_schema_->ToString());

  // Bitmap writer
  std::unique_ptr<WritableBlock> bitmap_block;
  RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&bitmap_block),
    "unable to create bitmap block for column " + col_schema_->ToString());
  bitmap_block_id_.SetId(bitmap_block->id().id());
  cfile::WriterOptions bitmap_opts;
  bitmap_opts.write_posidx = true;
  bitmap_opts.write_validx = false;
  bitmap_opts.storage_attributes.encoding = PLAIN_ENCODING;
  bitmap_opts.storage_attributes.compression = NO_COMPRESSION;
  bitmap_writer_.reset(new CFileWriter(bitmap_opts, GetTypeInfo(BINARY), false, std::move(bitmap_block)));
  RETURN_NOT_OK_PREPEND(bitmap_writer_->Start(), 
    "unable to start bitmap writer for column " + col_schema_->ToString());

  LOG(INFO) << "Create index blocks {" << key_block_id_ << "," << bitmap_block_id_ << "}";
  return Status::OK();
}

Status CIndexFileWriterBinary::Append(rowid_t id, const void* entries, size_t count) {
  const Slice* vals = reinterpret_cast<const Slice*>(entries);
  while (count--) {
    StringPiece val(reinterpret_cast<const char*>(vals->data()), vals->size());
    KeyToRoaringMap::iterator iter = map_.find(val);
    if (iter == map_.end()) {
      const char* data = CHECK_NOTNULL(reinterpret_cast<const char*>(arena_->AddSlice(*vals)));
      StringPiece key(data, vals->size());
      std::unique_ptr<Roaring> r(new Roaring());
      iter = map_.insert(KeyToRoaringMap::value_type(key, std::move(r))).first;
    }
    iter->second->add(id++);
    vals++;
  }
  return Status::OK();
}

Status CIndexFileWriterBinary::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  // Flush to Disk.
  for (const auto& e : map_) {
    Slice key(e.first.data(), e.first.size());
    Roaring* r = e.second.get();
    r->runOptimize();
    size_t size = r->getSizeInBytes(false);
    char* buff = static_cast<char*>(arena_->AllocateBytes(size));
    r->write(buff, false);
    Slice bitmap(buff, size);

    RETURN_NOT_OK(key_writer_->AppendEntries(&key, 1));
    RETURN_NOT_OK(bitmap_writer_->AppendEntries(&bitmap, 1));
  }

  // Finish writer.
  RETURN_NOT_OK_PREPEND(key_writer_->FinishAndReleaseBlock(closer),
    "unable to finish key_writer for column " + col_schema_->ToString());
  RETURN_NOT_OK_PREPEND(bitmap_writer_->FinishAndReleaseBlock(closer),
    "unable to finish bitmap_writer for column " + col_schema_->ToString());

  LOG(INFO) << "Closed index writer for " << col_schema_->name()
            << " with size{"<< key_writer_->written_size()
            << "," << bitmap_writer_->written_size() << "}";

  has_finished_ = true;
  return Status::OK();
}

size_t CIndexFileWriterBinary::written_size() const {
  if (!has_finished_) return 0;
  return key_writer_->written_size() + bitmap_writer_->written_size();
}

Status CIndexFileWriterBinary::GetFlushedBlocks(std::pair<BlockId, BlockId>& ret) const {
  if (!has_finished_) {
    return Status::Aborted("not finished");
  }

  CHECK(!key_block_id_.IsNull());
  CHECK(!bitmap_block_id_.IsNull());

  ret.first = key_block_id_;
  ret.second = bitmap_block_id_;
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileWriter::CMultiIndexFileWriter(FsManager* fs, const Schema* schema)
: fs_(fs)
, schema_(schema)
, written_row_count_(0)
, written_size_(0) {
}

CMultiIndexFileWriter::~CMultiIndexFileWriter() {
}

Status CMultiIndexFileWriter::Open() {
  CHECK(writers_.empty());
  for (int i = 0; i < schema_->num_columns(); ++i) {
    const ColumnSchema& col_schema = schema_->column(i);
    if (col_schema.is_indexed()) {
      std::unique_ptr<CIndexFileWriterBase> writer;
      switch (col_schema.type_info()->type()) {
        case UINT8:
          writer.reset(new CIndexFileWriterT<uint8_t>(fs_, &col_schema));
          break;
        case INT8:
          writer.reset(new CIndexFileWriterT<int8_t>(fs_, &col_schema));
          break;
        case UINT16:
          writer.reset(new CIndexFileWriterT<uint16_t>(fs_, &col_schema));
          break;
        case INT16:
          writer.reset(new CIndexFileWriterT<int16_t>(fs_, &col_schema));
          break;
        case UINT32:
          writer.reset(new CIndexFileWriterT<uint32_t>(fs_, &col_schema));
          break;
        case INT32:
        case UINT64:
          writer.reset(new CIndexFileWriterT<uint64_t>(fs_, &col_schema));
          break;
        case INT64:
        case UNIXTIME_MICROS:
          writer.reset(new CIndexFileWriterT<int64_t>(fs_, &col_schema));
          break;
        case BINARY:
        case STRING:
          writer.reset(new CIndexFileWriterBinary(fs_, &col_schema));
          break;
        default:
          LOG(ERROR) << "Invalid type:" << col_schema.type_info()->type();
          return Status::InvalidArgument("Invalid type.");
      }

      RETURN_NOT_OK(writer->Open());
      writers_[schema_->column_id(i)] = std::move(writer);
    }
  }
  writers_.shrink_to_fit();

  return Status::OK();
}

Status CMultiIndexFileWriter::AppendBlock(const DeltaStats* redo_delta_stats, const RowBlock& block) {
  if (redo_delta_stats->delete_count() != 0) {
    LOG(INFO) << "Skip appending block because of delete flag";
    return Status::OK();
  }

  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      if (redo_delta_stats->update_count_for_col_id(schema_->column_id(i)) != 0) {
        LOG(INFO) << "Skip appending block on " << schema_->column(i).name() << " because of update flag";
        continue;
      }
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriterBase* writer = FindOrDie(writers_, col_id).get();
      const ColumnBlock& column_block = block.column_block(i);
      RETURN_NOT_OK(writer->Append(written_row_count_, column_block.data(), column_block.nrows()));
    }
  }

  written_row_count_ += block.nrows();
  return Status::OK();
}

Status CMultiIndexFileWriter::Finish(const DeltaStats* redo_delta_stats) {
  ScopedWritableBlockCloser closer;
  RETURN_NOT_OK(FinishAndReleaseBlocks(redo_delta_stats, &closer));
  return closer.CloseBlocks();
}

Status CMultiIndexFileWriter::FinishAndReleaseBlocks(const DeltaStats* redo_delta_stats, ScopedWritableBlockCloser* closer) {
  if (redo_delta_stats->delete_count() != 0) {
    LOG(INFO) << "Discard the block because of delete flag";
    return Status::OK();
  }

  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      if (redo_delta_stats->update_count_for_col_id(schema_->column_id(i)) != 0) {
        LOG(INFO) << "Discard the block on " << schema_->column(i).name() << " because of update flag";
        continue;
      }
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriterBase* writer = FindOrDie(writers_, col_id).get();
      RETURN_NOT_OK(writer->FinishAndReleaseBlocks(closer));
      written_size_ += writer->written_size();
    }
  }
  return Status::OK();
}

size_t CMultiIndexFileWriter::written_size() const {
  return written_size_;
}

void CMultiIndexFileWriter::GetFlushedBlocksByColumnId(
    std::map<ColumnId, std::pair<BlockId, BlockId> >* ret) const {
  ret->clear();
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriterBase* writer = FindOrDie(writers_, col_id).get();
      std::pair<BlockId, BlockId> one;
      Status s = writer->GetFlushedBlocks(one);
      if (s.IsAborted()) continue;
      CHECK(s.ok());
      ret->insert(std::map<ColumnId, std::pair<BlockId, BlockId> >
        ::value_type(col_id, std::move(one)));
    }
  }
}

//////////////////////////////////////////////////////////////////////////
CIndexFileReader::CIndexFileReader(std::unique_ptr<CFileReader> key_reader,
                                   std::unique_ptr<CFileReader> bitmap_reader)
  : key_reader_(std::move(key_reader))
  , bitmap_reader_(std::move(bitmap_reader)) {
}

CIndexFileReader::~CIndexFileReader() {
}

Status CIndexFileReader::Open(FsManager* fs,
                              std::shared_ptr<MemTracker> parent_mem_tracker,
                              const RowSetMetadata::BlockIdPair& block_ids,
                              std::unique_ptr<CIndexFileReader>* reader) {
  LOG(INFO) << "Open index blocks {" << block_ids.first.ToString() << "," << block_ids.second.ToString() << "}";

  ReaderOptions opts;
  opts.parent_mem_tracker = parent_mem_tracker;

  // Open key file.
  std::unique_ptr<ReadableBlock> key_block;
  RETURN_NOT_OK(fs->OpenBlock(block_ids.first, &key_block));
  std::unique_ptr<CFileReader> key_reader;
  RETURN_NOT_OK(CFileReader::OpenNoInit(std::move(key_block), opts, &key_reader));
  RETURN_NOT_OK(key_reader->Init());

  // Open bitmap file.
  std::unique_ptr<ReadableBlock> bitmap_block;
  RETURN_NOT_OK(fs->OpenBlock(block_ids.second, &bitmap_block));
  std::unique_ptr<CFileReader> bitmap_reader;
  RETURN_NOT_OK(CFileReader::OpenNoInit(std::move(bitmap_block), opts, &bitmap_reader));
  // Lazy open: the bitmap_reader will be initialized while calling SeekToOrdinal later.

  reader->reset(new CIndexFileReader(std::move(key_reader), std::move(bitmap_reader)));
  return Status::OK();
}

Status CIndexFileReader::NewIterator(Iterator** iter) {
  *iter = new Iterator(this);
  return Status::OK();
}

Status CIndexFileReader::NewIterator(CFileIterator** key_iter, CFileIterator** bitmap_iter) {
  RETURN_NOT_OK(key_reader_->NewIterator(key_iter, CFileReader::CACHE_BLOCK));
  RETURN_NOT_OK(bitmap_reader_->NewIterator(bitmap_iter, CFileReader::CACHE_BLOCK));
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CIndexFileReader::Iterator::Iterator(CIndexFileReader* reader)
  : reader_(reader) {
}

CIndexFileReader::Iterator::~Iterator() {
}

Status CIndexFileReader::Iterator::Init() {
  CFileIterator* key_iter = nullptr;
  CFileIterator* bitmap_iter = nullptr;
  RETURN_NOT_OK(reader_->NewIterator(&key_iter, &bitmap_iter));
  key_iter_.reset(key_iter);
  bitmap_iter_.reset(bitmap_iter);
  return Status::OK();
}

Status CIndexFileReader::Iterator::Pushdown(const ColumnPredicate& predicate, Roaring& r) {
  const ColumnSchema& col_schema = predicate.column();
  switch (predicate.predicate_type())
  {
  case PredicateType::Equality:
    {
      std::vector<const void*> raw_values;
      raw_values.push_back(predicate.raw_lower());
      return PushdownEquaility(col_schema, raw_values, r);
    }
  case PredicateType::InList:
    return PushdownEquaility(col_schema, predicate.raw_values(), r);
  case PredicateType::Range:
    return PushdownRange(col_schema, predicate, r);
  case PredicateType::IsNotNull:
  case PredicateType::IsNull:
  case PredicateType::None:
    return Status::NotSupported("not supported");
  default:
    break;
  }

  return Status::RuntimeError("should not be here");
}

Status CIndexFileReader::Iterator::PushdownEquaility(const ColumnSchema& col_schema,
                                                     const vector<const void*>& values,
                                                     Roaring& r) {
  //LOG(INFO) << "Equality for column " << col_schema.name();
  size_t n = 1;
  Arena arena(1024, 1*1024*1024);
  const KeyEncoder<faststring>* key_encoder = &GetKeyEncoder<faststring>(col_schema.type_info());

  for (const void* value : values) {
    /*string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(value);
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(value), col_schema.type_info()->size());
    }
    DebugEntry(&col_schema, entry.c_str());*/

    // Prepare the 'EncodeKey'.
    faststring enc_value;
    key_encoder->ResetAndEncode(value, &enc_value);
    vector<const void*> raw_values;
    raw_values.push_back(value);
    EncodedKey key(&enc_value, &raw_values, 1);

    // Seek the key.
    bool exact = false;
    Status s = key_iter_->SeekAtOrAfter(key, &exact);
    if (s.IsNotFound() || !exact) {
      //LOG(INFO) << "can not seek the key";
      continue;
    }

    // Get the Bitmap.
    Slice bitmap;
    SelectionVector sel_vec(n);
    sel_vec.SetAllTrue();
    ColumnBlock col_block(GetTypeInfo(BINARY), nullptr, &bitmap, n, &arena);
    ColumnMaterializationContext ctx(0, nullptr, &col_block, &sel_vec);
    RETURN_NOT_OK(bitmap_iter_->SeekToOrdinal(key_iter_->GetCurrentOrdinal()));
    RETURN_NOT_OK(bitmap_iter_->PrepareBatch(&n));
    RETURN_NOT_OK(bitmap_iter_->Scan(&ctx));
    RETURN_NOT_OK(bitmap_iter_->FinishBatch());
    const Roaring& c = Roaring::read(reinterpret_cast<const char*>(bitmap.data()), false);
    if (r.isEmpty()) {
      r = std::move(c);
    } else {
      r |= c; /* Logic OR */
    }
  }

  return Status::OK();
}

Status CIndexFileReader::Iterator::PushdownRange(const ColumnSchema& col_schema,
                                                 const ColumnPredicate& predicate,
                                                 Roaring& r) {
  //LOG(INFO) << "Range for column " << col_schema.name();
  rowid_t lower_bound_id = 0;
  rowid_t upper_bound_id = UINT_MAX;
  reader_->key_reader_->CountRows(&upper_bound_id);
  const KeyEncoder<faststring>* key_encoder = &GetKeyEncoder<faststring>(col_schema.type_info());

  if (predicate.raw_lower() != nullptr) {
    /*string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(predicate.raw_lower());
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(predicate.raw_lower()), col_schema.type_info()->size());
    }
    DebugEntry(&col_schema, entry.c_str());*/

    faststring enc_value;
    key_encoder->ResetAndEncode(predicate.raw_lower(), &enc_value);
    vector<const void*> raw_values;
    raw_values.push_back(predicate.raw_lower());
    EncodedKey key(&enc_value, &raw_values, 1);

    bool exact = false;
    Status s = key_iter_->SeekAtOrAfter(key, &exact);
    if (s.IsNotFound()) {
      //LOG(INFO) << "can not seek the lower key";
      lower_bound_id = upper_bound_id;
    } else {
      RETURN_NOT_OK(s);
      lower_bound_id = std::max(lower_bound_id, key_iter_->GetCurrentOrdinal());
    }
  }

  if (predicate.raw_upper() != nullptr) {
    /*string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(predicate.raw_upper());
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(predicate.raw_upper()), col_schema.type_info()->size());
    }
    DebugEntry(&col_schema, entry.c_str());*/

    faststring enc_value;
    key_encoder->ResetAndEncode(predicate.raw_upper(), &enc_value);
    vector<const void*> raw_values;
    raw_values.push_back(predicate.raw_upper());
    EncodedKey key(&enc_value, &raw_values, 1);

    bool exact = false;
    Status s = key_iter_->SeekAtOrAfter(key, &exact);
    if (s.IsNotFound()) {
      //LOG(INFO) << "can not seek the upper key";
    } else {
      RETURN_NOT_OK(s);
      upper_bound_id = std::min(upper_bound_id, key_iter_->GetCurrentOrdinal());      
    }
  }

  size_t n = 100;
  const size_t MAX_N = n;
  Slice bitmaps[MAX_N];
  Arena arena(1024, 1*1024*1024);
  while (lower_bound_id < upper_bound_id) {
    n = std::min(n, static_cast<size_t>(upper_bound_id-lower_bound_id));

    SelectionVector sel_vec(n);
    sel_vec.SetAllTrue();
    ColumnBlock col_block(GetTypeInfo(BINARY), nullptr, &bitmaps, n, &arena);
    ColumnMaterializationContext ctx(0, nullptr, &col_block, &sel_vec);
    RETURN_NOT_OK(bitmap_iter_->SeekToOrdinal(lower_bound_id));
    RETURN_NOT_OK(bitmap_iter_->PrepareBatch(&n));
    RETURN_NOT_OK(bitmap_iter_->Scan(&ctx));
    RETURN_NOT_OK(bitmap_iter_->FinishBatch());
    for (int i = 0; i < n; ++i) {
      const Roaring& c = Roaring::read(reinterpret_cast<const char*>(bitmaps[i].data()), false);
      if (PREDICT_FALSE(r.isEmpty())) {
        r = std::move(c);
      } else {
        r |= c; /* Logic OR */
      }
    }

    lower_bound_id += n;
  }

  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileReader::CMultiIndexFileReader(std::shared_ptr<RowSetMetadata> rowset_metadata,
                                             std::shared_ptr<MemTracker> parent_mem_tracker)
  : rowset_metadata_(std::move(rowset_metadata))
  , parent_mem_tracker_(std::move(parent_mem_tracker)) {
}

CMultiIndexFileReader::~CMultiIndexFileReader() {
}

Status CMultiIndexFileReader::Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                                   std::shared_ptr<MemTracker> parent_mem_tracker,
                                   gscoped_ptr<CMultiIndexFileReader>* reader) {
  gscoped_ptr<CMultiIndexFileReader> tmp(
    new CMultiIndexFileReader(std::move(rowset_metadata), std::move(parent_mem_tracker)));
  RETURN_NOT_OK(tmp->DoOpen());
  *reader = std::move(tmp);
  return Status::OK();
}

Status CMultiIndexFileReader::DoOpen() {
  RowSetMetadata::ColumnIdToBlockIdPairMap block_map = std::move(rowset_metadata_->GetIndexBlocksById());
  for (const RowSetMetadata::ColumnIdToBlockIdPairMap::value_type& e : block_map) {
    const ColumnId& col_id = e.first;
    const RowSetMetadata::BlockIdPair& block_ids = e.second;
    DCHECK(!ContainsKey(readers_, col_id)) << "already open";

    std::unique_ptr<CIndexFileReader> reader;
    RETURN_NOT_OK(CIndexFileReader::Open(rowset_metadata_->fs_manager(),
                                         parent_mem_tracker_, block_ids, &reader));
    readers_[col_id] = std::move(reader);
  }
  readers_.shrink_to_fit();
  return Status::OK();
}

Status CMultiIndexFileReader::NewIterator(Iterator** iter) const {
  *iter = new Iterator(this);
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileReader::Iterator::Iterator(const CMultiIndexFileReader* reader)
  : reader_(reader)
  , bHasResult_(true)
  , arr_n_(0)
  , arr_i_(0) {
}

CMultiIndexFileReader::Iterator::~Iterator() {
}

Status CMultiIndexFileReader::Iterator::Init(ScanSpec* spec, const Schema* projection, const DeltaStats& stats) {
  std::vector<std::string> col_names;
  for (const auto& one : spec->predicates()) {
    if (!one.second.column().is_indexed()) continue;
    int col_idx = projection->find_column(one.first);
    const ColumnId& col_id = projection->column_id(col_idx);
    if (stats.update_count_for_col_id(col_id) != 0) continue;

    // Find the reader according to column id.
    ColumnIdToReaderMap::const_iterator iter = reader_->readers_.find(col_id);
    if (iter == reader_->readers_.end()) {
      // There is no index for the column:
      //  1) the index creation is later than cfile;
      //  2) the index is invalid while there are mutations;
      //LOG(INFO) << "the column " << one.first << " has no index yet";
      continue;
    }

    // Create and init the iterator for the reader.
    CIndexFileReader::Iterator* it;
    RETURN_NOT_OK(iter->second->NewIterator(&it));
    std::unique_ptr<CIndexFileReader::Iterator> reader_iter(it);
    RETURN_NOT_OK(reader_iter->Init());

    // Push down predicate and get index-bitmap back.
    Roaring c;
    Status s = reader_iter->Pushdown(one.second, c);
    if (s.IsNotSupported()) { // for IsNull & IsNotNull
      PredicateType type = one.second.predicate_type();
      if (type == PredicateType::IsNull || 
          type == PredicateType::IsNotNull ||
          type == PredicateType::None) {
        //LOG(INFO) << "predicate type " << (int)type << " is not supported";
        continue;
      }
    }
    RETURN_NOT_OK(s);
    if (c.isEmpty()) {
      //LOG(INFO) << "predicate for "<< one.first << " is empty";
      bHasResult_ = false;
      break;
    }

    // Merge bitmaps.
    if (c_.isEmpty()) { // first time.
      c_ = std::move(c);
    } else {
      c_ &= c; // Logic AND.
      if (c_.isEmpty()) {
        //LOG(INFO) << "the intersection of the bitmaps is empty";
        bHasResult_ = false;
        break;
      }
    }

    col_names.push_back(one.first);
    reader_iters_[col_id] = std::move(reader_iter);
  }
  reader_iters_.shrink_to_fit();
  //LOG(INFO) << "capture a bitmap: " << c_.toString();

  // Remove the predicates that using index.
  for (auto& name : col_names) {
    spec->RemovePredicate(name);
  }

  // Prepare for traversal.
  if (bHasResult_ && !c_.isEmpty()) {
    arr_n_ = c_.cardinality();
    uint32_t* arr = new uint32_t[arr_n_];
    CHECK(arr);
    c_.toUint32Array(arr);
    arr_.reset(arr);
  }

  return Status::OK();
}

Status CMultiIndexFileReader::Iterator::GetBounds(rowid_t* lower_bound_idx,
                                                  rowid_t* upper_bound_idx) {
  if (!bHasResult_) return Status::NotFound("the result is empty");
  if (!c_.isEmpty()) {
    uint32_t* arr = arr_.get();
    *lower_bound_idx = arr[0];
    *upper_bound_idx = arr[arr_n_-1] + 1; // Exclusive.
  }
  return Status::OK();
}

}
}
