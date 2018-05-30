
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

CIndexFileWriter::CIndexFileWriter(FsManager* fs, const ColumnSchema* col_schema)
: fs_(fs)
, col_schema_(col_schema) {
}

CIndexFileWriter::~CIndexFileWriter() {
}

Status CIndexFileWriter::Open() {
  CHECK(!col_schema_->is_indexed());

  // Key writer
  std::unique_ptr<WritableBlock> key_block;
  RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&key_block),
        "unable to create key block for column " + col_schema_->ToString());
  key_block_id_.SetId(key_block->id().id());
  cfile::WriterOptions key_opts;
  key_opts.write_posidx = false;
  key_opts.write_validx = true;
  key_opts.storage_attributes.encoding = PREFIX_ENCODING;
  key_opts.storage_attributes.compression = LZ4;
  key_opts.storage_attributes.cfile_block_size = FLAGS_default_index_block_size_bytes;
  key_writer_.reset(new CFileWriter(key_opts, GetTypeInfo(BINARY), false, std::move(key_block)));
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
  bitmap_opts.storage_attributes.compression = LZ4;
  bitmap_writer_.reset(new CFileWriter(bitmap_opts, GetTypeInfo(BINARY), false, std::move(bitmap_block)));
  RETURN_NOT_OK_PREPEND(bitmap_writer_->Start(), 
        "unable to start bitmap writer for column " + col_schema_->ToString());

  return Status::OK();
}

Status CIndexFileWriter::Append(rowid_t id, const void *entries, size_t count) {
  const uint8_t *vals = reinterpret_cast<const uint8_t *>(entries);
  for (int i = 1; i <= count; ++i) {
    const Slice* val = reinterpret_cast<const Slice*>(vals);
    RETURN_NOT_OK_PREPEND(Append(id+i, val),
          "unable to Append for column " + col_schema_->ToString());
    vals += col_schema_->type_info()->size();
  }

  return Status::OK();
}

// 积累数据的时候可以使用raw key，但是写入磁盘的时候最好是Encode一下，类似primary key。
Status CIndexFileWriter::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  // Flush Memory to File.
  KeyToRoaringMap::const_iterator iter = map_.begin();
  for (; iter != map_.end(); ++iter) {
    Slice key(iter->first);
    RETURN_NOT_OK_PREPEND(key_writer_->AppendEntries(&key, 1),
          "unable to AppendEntries (key) to CFile for column " + col_schema_->ToString());

    Roaring* c = iter->second.get();
    c->runOptimize();
    size_t size = c->getSizeInBytes();
    char* buff = new char[size];
    c->write(buff);
    Slice bitmap(buff, size);
    RETURN_NOT_OK_PREPEND(bitmap_writer_->AppendEntries(&bitmap, 1),
          "unable to AppendEntries (bitmap) to CFile for column " + col_schema_->ToString());
  }

  // Save the min/max value.
  Slice min_key(map_.begin()->first);
  Slice max_key(map_.end()->first);
  key_writer_->AddMetadataPair(tablet::DiskRowSet::kMinKeyMetaEntryName, min_key);
  key_writer_->AddMetadataPair(tablet::DiskRowSet::kMaxKeyMetaEntryName, max_key);

  // Finish writer.
  RETURN_NOT_OK_PREPEND(key_writer_->FinishAndReleaseBlock(closer),
        "unable to finish key_writer for column " + col_schema_->ToString());
  RETURN_NOT_OK_PREPEND(bitmap_writer_->FinishAndReleaseBlock(closer),
        "unable to finish bitmap_writer for column " + col_schema_->ToString());

  return Status::OK();
}

size_t CIndexFileWriter::written_size() const {
  return key_writer_->written_size() +
         bitmap_writer_->written_size();
}

void CIndexFileWriter::GetFlushedBlocks(std::pair<BlockId, BlockId>& ret) const {
  CHECK(!key_block_id_.IsNull());
  CHECK(!bitmap_block_id_.IsNull());

  ret.first = key_block_id_;
  ret.second = bitmap_block_id_;
}

Status CIndexFileWriter::Append(rowid_t id, const Slice* value) {
  std::string key(reinterpret_cast<const char*>(value->data()), value->size());
  KeyToRoaringMap::iterator iter = map_.find(key);
  if (iter != map_.end()) {
    iter->second->add(id);
    return Status::OK();
  }

  std::unique_ptr<Roaring> c(new Roaring());
  c->add(id);
  map_.insert(KeyToRoaringMap::value_type(key, std::move(c)));

  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileWriter::CMultiIndexFileWriter(FsManager* fs, const Schema* schema)
: fs_(fs)
, schema_(schema)
, written_count_(0) {
}

CMultiIndexFileWriter::~CMultiIndexFileWriter() {
}

Status CMultiIndexFileWriter::Open() {
  CHECK(writers_.empty());

  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (!schema_->column(i).is_indexed()) continue;

    const ColumnSchema& col_schema = schema_->column(i);
    std::unique_ptr<CIndexFileWriter> writer(new CIndexFileWriter(fs_, &col_schema));
    RETURN_NOT_OK(writer->Open());

    const ColumnId& col_id = schema_->column_id(i);
    writers_[col_id] = std::move(writer);
  }
  writers_.shrink_to_fit();

  return Status::OK();
}

Status CMultiIndexFileWriter::AppendBlock(const RowBlock& block) {
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (!schema_->column(i).is_indexed()) continue;

    const ColumnId& col_id = schema_->column_id(i);
    CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();

    const ColumnBlock& column_block = block.column_block(i);
    RETURN_NOT_OK(writer->Append(written_count_, 
          column_block.data(), column_block.nrows()));
  }

  written_count_ += block.nrows();
  return Status::OK();
}

Status CMultiIndexFileWriter::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (!schema_->column(i).is_indexed()) continue;

    const ColumnId& col_id = schema_->column_id(i);
    CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
    RETURN_NOT_OK(writer->FinishAndReleaseBlocks(closer));
  }

  return Status::OK();
}

size_t CMultiIndexFileWriter::written_size() const {
  size_t size = 0;
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (!schema_->column(i).is_indexed()) continue;

    const ColumnId& col_id = schema_->column_id(i);
    CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
    size += writer->written_size();
  }

  return size;
}

void CMultiIndexFileWriter::GetFlushedBlocksByColumnId(
    std::map<ColumnId, std::pair<BlockId, BlockId> >* ret) const {
  ret->clear();
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (!schema_->column(i).is_indexed()) continue;

    const ColumnId& col_id = schema_->column_id(i);
    CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();

    std::pair<BlockId, BlockId> one;
    writer->GetFlushedBlocks(one);
    ret->insert(std::map<ColumnId, std::pair<BlockId, BlockId> >
      ::value_type(col_id, std::move(one)));
  }
}

//////////////////////////////////////////////////////////////////////////
CIndexFileReader::CIndexFileReader(std::unique_ptr<CFileReader> key_reader,
                                   std::unique_ptr<CFileReader> bitmap_reader,
                                   const std::string& min_encoded_key,
                                   const std::string& max_encoded_key)
  : key_reader_(std::move(key_reader))
  , bitmap_reader_(std::move(bitmap_reader))
  , min_encoded_key_(min_encoded_key)
  , max_encoded_key_(max_encoded_key) {
}

CIndexFileReader::~CIndexFileReader() {
}

Status CIndexFileReader::Open(std::shared_ptr<RowSetMetadata> rowset_metadata,
                              std::shared_ptr<MemTracker> parent_mem_tracker,
                              const RowSetMetadata::BlockIdPair& block_ids,
                              std::unique_ptr<CIndexFileReader>* reader) {
  FsManager* fs = rowset_metadata->fs_manager();

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
  // The bitmap_reader will be inited while calling SeekToOrdinal later.

  // Get min&max of the current index.
  std::string min_encoded_key = "";
  std::string max_encoded_key = "";
  if (!key_reader->GetMetadataEntry(tablet::DiskRowSet::kMinKeyMetaEntryName, &min_encoded_key)) {
    return Status::Corruption("No min key found");
  }
  if (!key_reader->GetMetadataEntry(tablet::DiskRowSet::kMaxKeyMetaEntryName, &max_encoded_key)) {
    return Status::Corruption("No max key found");
  }
  if (Slice(min_encoded_key).compare(Slice(max_encoded_key)) > 0) {
    return Status::Corruption(Substitute("error: min $0 > max $1",
      KUDU_REDACT(Slice(min_encoded_key).ToDebugString()),
      KUDU_REDACT(Slice(max_encoded_key).ToDebugString())));
  }

  reader->reset(new CIndexFileReader(std::move(key_reader),
                                     std::move(bitmap_reader),
                                     min_encoded_key,
                                     max_encoded_key));
  return Status::OK();
}

Status CIndexFileReader::NewIterator(Iterator** iter) {
  *iter = new Iterator(this);
  return Status::OK();
}

Status CIndexFileReader::GetColumnBounds(std::string* min_encoded_key,
                                         std::string* max_encoded_key) const {
  // 是否在这里给添加不存在的bound? ("", nullptr)
  if (min_encoded_key_.empty() || max_encoded_key_.empty()) {
    return Status::Corruption("min or max is empty!");
  }

  *min_encoded_key = min_encoded_key_;
  *max_encoded_key = max_encoded_key_;
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

Status CIndexFileReader::Iterator::Init(const ColumnPredicate& pred,
                                        const TypeInfo* type_info) {
  // Setup Iterators.
  CFileIterator* key_iter = NULL;
  CFileIterator* bitmap_iter = NULL;
  RETURN_NOT_OK(reader_->NewIterator(&key_iter, &bitmap_iter));
  key_iter_.reset(key_iter);
  bitmap_iter_.reset(bitmap_iter);

  // Collect probes.
  std::vector<Slice> probes;
  switch (pred.predicate_type())
  {
  case PredicateType::Equality:
    probes.push_back(Slice(reinterpret_cast<const char*>(pred.raw_lower()), type_info->size()));
    break;
  case PredicateType::InList:
    for (const auto& value : pred.raw_values()) {
      probes.push_back(Slice(reinterpret_cast<const char*>(value), type_info->size()));
    }
    break;
  case PredicateType::Range:
  case PredicateType::IsNotNull:
  case PredicateType::IsNull:
  default:
    return Status::RuntimeError("should not be here.");
  }

  // Find the probes.
  size_t n = 1;
  Arena arena(1024, 1*1024*1024);
  for (const auto& probe : probes) {
    bool exact = false;
    Status s;// = key_iter_->SeekAtOrAfter(probe, &exact);
    if (s.IsNotFound() || !exact) {
      return Status::Aborted("can not seek the probe.");
    }

    rowid_t cur_id = key_iter_->GetCurrentOrdinal();
    RETURN_NOT_OK(bitmap_iter_->SeekToOrdinal(cur_id));
    RETURN_NOT_OK(bitmap_iter_->PrepareBatch(&n));
    if (n != 1) {
      return Status::Corruption("can not prepare one row.");
    }

    Slice data;
    SelectionVector sel_vec(n);
    ColumnBlock col_block(GetTypeInfo(BINARY), nullptr, &data, n, &arena);
    ColumnMaterializationContext ctx(0, nullptr, &col_block, &sel_vec);
    RETURN_NOT_OK(bitmap_iter_->Scan(&ctx));
    RETURN_NOT_OK(bitmap_iter_->FinishBatch());

    const Roaring& c = Roaring::readSafe(reinterpret_cast<const char*>(data.data()), data.size());
    if (c_.isEmpty()) {
      c_ = c;
    } else {
      c_ = c | c_;
    }
  }

  return Status::OK();
}

const Roaring& CIndexFileReader::Iterator::GetRoaring() const {
  return c_;
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
  gscoped_ptr<CMultiIndexFileReader> tmp(new CMultiIndexFileReader(std::move(rowset_metadata),
                                                                   std::move(parent_mem_tracker)));
  RETURN_NOT_OK(tmp->DoOpen());
  *reader = std::move(tmp);
  return Status::OK();
}

Status CMultiIndexFileReader::NewIterator(Iterator** iter) const {
  *iter = new Iterator(this);
  return Status::OK();
}

Status CMultiIndexFileReader::GetColumnBounds(const ColumnId& col_id,
                                             std::string* min_encoded_key,
                                             std::string* max_encoded_key) const {
  ColumnIdToReaderMap::const_iterator iter = readers_.find(col_id);
  if (iter == readers_.end()) {
    return Status::NotFound("can not find CIndexFileReader by ColumnId for min&max.");
  }

  return iter->second->GetColumnBounds(min_encoded_key, max_encoded_key);
}

Status CMultiIndexFileReader::DoOpen() {
  RowSetMetadata::ColumnIdToBlockIdPairMap block_map = rowset_metadata_->GetIndexBlocksById();
  for (const RowSetMetadata::ColumnIdToBlockIdPairMap::value_type& e : block_map) {
    const ColumnId& col_id = e.first;
    const RowSetMetadata::BlockIdPair& block_ids = e.second;
    DCHECK(!ContainsKey(readers_, col_id)) << "already open";

    std::unique_ptr<CIndexFileReader> tmp;
    RETURN_NOT_OK(CIndexFileReader::Open(rowset_metadata_, parent_mem_tracker_, block_ids, &tmp));
    readers_[col_id] = std::move(tmp);
  }
  readers_.shrink_to_fit();
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileReader::Iterator::Iterator(const CMultiIndexFileReader* reader)
  : reader_(reader) {
}

CMultiIndexFileReader::Iterator::~Iterator() {
}

Status CMultiIndexFileReader::Iterator::Init(const Schema *projection, ScanSpec *spec) {
  std::vector<std::string> col_names;
  for (const auto& col_pred : spec->predicates()) {
    const std::string& col_name = col_pred.first;
    int col_idx = projection->find_column(col_name);
    const ColumnSchema& col_schema = projection->column(col_idx);

    // Skip the non-index column.
    if (!col_schema.is_indexed()) continue;

    // Only support Equality&InList.
    const ColumnPredicate& pred = col_pred.second;
    switch (pred.predicate_type())
    {
    case PredicateType::Equality:
    case PredicateType::InList:
    	break;
    case PredicateType::Range: // Ignore.
    case PredicateType::IsNotNull:
    case PredicateType::IsNull:
    default:
      continue;
    }

    col_names.push_back(col_name);
    const ColumnId& col_id = projection->column_id(col_idx);
    ColumnIdToReaderMap::const_iterator iter = reader_->readers_.find(col_id);
    if (iter == reader_->readers_.end()) {
      return Status::RuntimeError("can not find CIndexFileReader by ColumnId for NewIterator.");
    }

    CIndexFileReader::Iterator* tmp;
    std::unique_ptr<CIndexFileReader::Iterator> reader_iter;
    RETURN_NOT_OK(iter->second->NewIterator(&tmp));
    reader_iter.reset(tmp);
    RETURN_NOT_OK(reader_iter->Init(pred, projection->column(col_idx).type_info()));
    reader_iters_[col_id] = std::move(reader_iter);
  }
  reader_iters_.shrink_to_fit();

  // Remove the predicates that have found the bitmap. 
  for (auto& name : col_names) {
    spec->RemovePredicate(name);
  }

  return Status::OK();
}

Status CMultiIndexFileReader::Iterator::GetBounds(rowid_t* lower_bound_idx,
                                                  rowid_t* upper_bound_idx) {
  if (reader_iters_.empty()) return Status::OK();
  for (const auto& reader_iter : reader_iters_) {
    const Roaring& c = reader_iter.second->GetRoaring();
    c_ &= c;
  }

  if (c_.isEmpty()) return Status::Aborted("The result is EMPTY!");
  *lower_bound_idx = c_.minimum();
  *upper_bound_idx = c_.maximum();
  return Status::OK();
}

bool CMultiIndexFileReader::Iterator::Exist(rowid_t row_idx) const {
  return c_.contains(row_idx);
}

}
}
