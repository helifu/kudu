
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


void DebugEntry(const ColumnSchema* col_schema, const char* val) {
  switch (col_schema->type_info()->type()) {
  case BOOL:
    {
      bool v = (val[0]!=0?true:false);
      LOG(INFO) << "    (" << (v?"true":"false") << ")";
      break;
    }
  case UINT8:
    {
      uint8_t v = val[0];
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case INT8:
    {
      int8_t v = val[0];
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case UINT16:
    {
      uint16_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case INT16:
    {
      int16_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case UINT32:
    {
      uint32_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case INT32:
    {
      int32_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case UINT64:
  case UNIXTIME_MICROS:
    {
      uint64_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case INT64:
    {
      int64_t v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case FLOAT:
    {
      float v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case DOUBLE:
    {
      double v;
      memcpy(&v, val, sizeof(v));
      LOG(INFO) << "    (" << v << ")";
      break;
    }
  case STRING:
  case BINARY:
    {
      LOG(INFO) << "    (" << val << ")";
      break;
    }
  default:
    {
      LOG(WARNING) << "Unknown entry type:" << col_schema->type_info()->type();
      break;
    }
  }
}

CIndexFileWriter::CIndexFileWriter(FsManager* fs, const ColumnSchema* col_schema)
: fs_(fs)
, col_schema_(col_schema) {
}

CIndexFileWriter::~CIndexFileWriter() {
}

Status CIndexFileWriter::Open() {
  CHECK(col_schema_->is_indexed());
  LOG(INFO) << "Create index file for " << col_schema_->ToString();

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

  LOG(INFO) << "Generate block ids [" << key_block_id_
            << "," << bitmap_block_id_ << "]";
  return Status::OK();
}

Status CIndexFileWriter::Append(rowid_t id, const void* entries, size_t count) {
  LOG(INFO) << "Append " << count << " entries from " << id;
  const uint8_t* vals = reinterpret_cast<const uint8_t*>(entries);
  while (count--) {
    std::string entry;
    if (col_schema_->type_info()->physical_type() == BINARY) {
      const Slice* val = reinterpret_cast<const Slice*>(vals);
      entry.assign(reinterpret_cast<const char*>(val->data()), val->size());
      vals += sizeof(Slice);
    } else {
      size_t size = col_schema_->type_info()->size();
      const char* val = reinterpret_cast<const char*>(vals);
      entry.assign(val, size);
      vals += size;
    }
    //DebugEntry(col_schema_, entry.c_str());

    RETURN_NOT_OK_PREPEND(Append(entry, id++),
      "unable to Append for column " + col_schema_->ToString());
  }

  return Status::OK();
}

Status CIndexFileWriter::Append(const std::string& entry, rowid_t id) {
  KeyToRoaringMap::iterator iter = map_.find(entry);
  if (iter == map_.end()) {
    std::unique_ptr<Roaring> c(new Roaring());
    iter = map_.insert(KeyToRoaringMap::value_type(std::move(entry), std::move(c))).first;
  }

  iter->second->add(id);
  return Status::OK();
}

Status CIndexFileWriter::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  KeyToRoaringMap::const_iterator iter;
  for (iter = map_.begin(); iter != map_.end(); ++iter) {
    // Flush Key: the CFileWriter will encode the key for the validx_tree.
    if (col_schema_->type_info()->physical_type() == BINARY) {
      Slice key(iter->first);
      RETURN_NOT_OK_PREPEND(key_writer_->AppendEntries(&key, 1), 
        "unable to AppendEntries (&key) to CFile for column " + col_schema_->ToString());
    } else {
      const void* key = reinterpret_cast<const void*>(iter->first.c_str());
      RETURN_NOT_OK_PREPEND(key_writer_->AppendEntries(key, 1),
        "unable to AppendEntries (key) to CFile for column " + col_schema_->ToString());
    }
    //DebugEntry(col_schema_, iter->first.c_str());

    // Flush Bitmap.
    Roaring* c = iter->second.get();
    //LOG(INFO) << c->toString();
    c->runOptimize();
    size_t size = c->getSizeInBytes(false);
    std::unique_ptr<char> buff(new char[size]);
    c->write(buff.get(), false);
    Slice bitmap(buff.get(), size);
    RETURN_NOT_OK_PREPEND(bitmap_writer_->AppendEntries(&bitmap, 1),
          "unable to AppendEntries (bitmap) to CFile for column " + col_schema_->ToString());
  }

  // Encode and save the min&max value.
  faststring enc_min_key, enc_max_key;
  const KeyEncoder<faststring>* key_encoder = &GetKeyEncoder<faststring>(col_schema_->type_info());
  if (col_schema_->type_info()->physical_type() == BINARY) {
    Slice min_key(map_.begin()->first);
    Slice max_key(map_.rbegin()->first);
    key_encoder->ResetAndEncode(&min_key, &enc_min_key);
    key_encoder->ResetAndEncode(&max_key, &enc_max_key);
  } else {
    key_encoder->ResetAndEncode(map_.begin()->first.c_str(), &enc_min_key);
    key_encoder->ResetAndEncode(map_.rbegin()->first.c_str(), &enc_max_key);
  }

  key_writer_->AddMetadataPair(tablet::DiskRowSet::kMinKeyMetaEntryName, enc_min_key);
  key_writer_->AddMetadataPair(tablet::DiskRowSet::kMaxKeyMetaEntryName, enc_max_key);

  // Finish writer.
  RETURN_NOT_OK_PREPEND(key_writer_->FinishAndReleaseBlock(closer),
        "unable to finish key_writer for column " + col_schema_->ToString());
  RETURN_NOT_OK_PREPEND(bitmap_writer_->FinishAndReleaseBlock(closer),
        "unable to finish bitmap_writer for column " + col_schema_->ToString());

  LOG(INFO) << "Finish block for column ''" << col_schema_->name()
            << "' with size("<< key_writer_->written_size()
            << "," << bitmap_writer_->written_size() << ")";

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
    if (schema_->column(i).is_indexed()) {
      const ColumnSchema& col_schema = schema_->column(i);
      std::unique_ptr<CIndexFileWriter> writer(new CIndexFileWriter(fs_, &col_schema));
      RETURN_NOT_OK(writer->Open());
      const ColumnId& col_id = schema_->column_id(i);
      writers_[col_id] = std::move(writer);
    }
  }
  writers_.shrink_to_fit();

  return Status::OK();
}

Status CMultiIndexFileWriter::AppendBlock(const RowBlock& block) {
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
      const ColumnBlock& column_block = block.column_block(i);
      RETURN_NOT_OK(writer->Append(written_count_, 
            column_block.data(), column_block.nrows()));
    }
  }

  written_count_ += block.nrows();
  return Status::OK();
}

Status CMultiIndexFileWriter::FinishAndReleaseBlocks(ScopedWritableBlockCloser* closer) {
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
      RETURN_NOT_OK(writer->FinishAndReleaseBlocks(closer));
    }
  }
  return Status::OK();
}

size_t CMultiIndexFileWriter::written_size() const {
  size_t size = 0;
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
      size += writer->written_size();
    }
  }
  return size;
}

void CMultiIndexFileWriter::GetFlushedBlocksByColumnId(
    std::map<ColumnId, std::pair<BlockId, BlockId> >* ret) const {
  ret->clear();
  for (int i = 0; i < schema_->num_columns(); ++i) {
    if (schema_->column(i).is_indexed()) {
      const ColumnId& col_id = schema_->column_id(i);
      CIndexFileWriter* writer = FindOrDie(writers_, col_id).get();
      std::pair<BlockId, BlockId> one;
      writer->GetFlushedBlocks(one);
      ret->insert(std::map<ColumnId, std::pair<BlockId, BlockId> >
        ::value_type(col_id, std::move(one)));
    }
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

Status CIndexFileReader::Open(FsManager* fs,
                              std::shared_ptr<MemTracker> parent_mem_tracker,
                              const RowSetMetadata::BlockIdPair& block_ids,
                              std::unique_ptr<CIndexFileReader>* reader) {
  LOG(INFO) << "Open index file with [" << block_ids.first.ToString()
            << "," << block_ids.second.ToString() << "]";

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

  // Get min&max of the current index.
  std::string min_encoded_key, max_encoded_key;
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
    return Status::OK();
  case PredicateType::IsNull:
    return Status::NotSupported("IsNull is not support");
  default:
    break;
  }

  return Status::RuntimeError("should not be here.");
}

Status CIndexFileReader::Iterator::PushdownEquaility(const ColumnSchema& col_schema,
                                                     const vector<const void*>& values,
                                                     Roaring& r) {
  LOG(INFO) << "Equality for column '" << col_schema.name() 
            << "' with " << values.size() << " values.";
  size_t n = 1;
  Arena arena(1024, 1*1024*1024);
  const KeyEncoder<faststring>* key_encoder = &GetKeyEncoder<faststring>(col_schema.type_info());

  for (const void* value : values) {
    string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(value);
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(value), col_schema.type_info()->size());
    }
    //DebugEntry(&col_schema, entry.c_str());

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
      LOG(INFO) << "can not seek the key, next ...";
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
  LOG(INFO) << "Range for column '" << col_schema.type_info()->name() << "'";
  rowid_t lower_bound_id = 0;
  rowid_t upper_bound_id = UINT_MAX;
  reader_->key_reader_->CountRows(&upper_bound_id);
  const KeyEncoder<faststring>* key_encoder = &GetKeyEncoder<faststring>(col_schema.type_info());

  if (predicate.raw_lower() != nullptr) {
    string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(predicate.raw_lower());
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(predicate.raw_lower()), col_schema.type_info()->size());
    }
    //DebugEntry(&col_schema, entry.c_str());

    faststring enc_value;
    key_encoder->ResetAndEncode(predicate.raw_lower(), &enc_value);
    vector<const void*> raw_values;
    raw_values.push_back(predicate.raw_lower());
    EncodedKey key(&enc_value, &raw_values, 1);

    bool exact = false;
    Status s = key_iter_->SeekAtOrAfter(key, &exact);
    if (s.IsNotFound()) {
      LOG(INFO) << "can not seek the lower key, so set lower bound to upper bound.";
      lower_bound_id = upper_bound_id;
    } else {
      RETURN_NOT_OK(s);
      lower_bound_id = std::max(lower_bound_id, key_iter_->GetCurrentOrdinal());
    }
  }

  if (predicate.raw_upper() != nullptr) {
    string entry;
    if (col_schema.type_info()->physical_type() == BINARY) {
      const Slice* d = reinterpret_cast<const Slice*>(predicate.raw_upper());
      entry.assign(reinterpret_cast<const char*>(d->data()), d->size());
    } else {
      entry.assign(reinterpret_cast<const char*>(predicate.raw_upper()), col_schema.type_info()->size());
    }
    DebugEntry(&col_schema, entry.c_str());

    faststring enc_value;
    key_encoder->ResetAndEncode(predicate.raw_upper(), &enc_value);
    vector<const void*> raw_values;
    raw_values.push_back(predicate.raw_upper());
    EncodedKey key(&enc_value, &raw_values, 1);

    bool exact = false;
    Status s = key_iter_->SeekAtOrAfter(key, &exact);
    if (s.IsNotFound()) {
      LOG(INFO) << "can not seek the upper key, so still use the number of rows.";
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
  RowSetMetadata::ColumnIdToBlockIdPairMap block_map = rowset_metadata_->GetIndexBlocksById();
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

Status CMultiIndexFileReader::GetColumnBounds(const ColumnId& col_id,
                                             std::string* min_encoded_key,
                                             std::string* max_encoded_key) const {
  ColumnIdToReaderMap::const_iterator iter = readers_.find(col_id);
  if (iter == readers_.end()) {
    LOG(WARNING) << "can not find the reader for " << col_id;
    return Status::NotSupported("can not find reader");
  }

  return iter->second->GetColumnBounds(min_encoded_key, max_encoded_key);
}

//////////////////////////////////////////////////////////////////////////
CMultiIndexFileReader::Iterator::Iterator(const CMultiIndexFileReader* reader)
  : reader_(reader)
  , bHasResult_(true)
  , c_card_(0)
  , arr_i_(0) {
}

CMultiIndexFileReader::Iterator::~Iterator() {
}

Status CMultiIndexFileReader::Iterator::Init(const Schema* projection, ScanSpec* spec) {
  std::vector<std::string> col_names;
  for (const auto& one : spec->predicates()) {
    // Skip the non-index column.
    if (!one.second.column().is_indexed()) continue;

    // Find the reader according to column id.
    int col_idx = projection->find_column(one.first);
    const ColumnId& col_id = projection->column_id(col_idx);
    ColumnIdToReaderMap::const_iterator iter = reader_->readers_.find(col_id);
    if (iter == reader_->readers_.end()) {
      LOG(ERROR) << "can not find the reader for column_id" << col_id;
      return Status::RuntimeError("can not find reader");
    }

    // Create and init the iterator for the reader.
    CIndexFileReader::Iterator* it;
    RETURN_NOT_OK(iter->second->NewIterator(&it));
    std::unique_ptr<CIndexFileReader::Iterator> reader_iter(it);
    RETURN_NOT_OK(reader_iter->Init());

    // Push down predicate and get index-bitmap back.
    Roaring c;
    Status s = reader_iter->Pushdown(one.second, c);
    if (s.IsNotSupported() && 
        one.second.predicate_type() == PredicateType::IsNull) {
      continue; // for IsNotNull.
    }
    RETURN_NOT_OK(s);
    if (c.isEmpty()) {
      LOG(INFO) << "predicate of "<< one.first << " can not filter any rows, so the result is zero.";
      bHasResult_ = false;
      break;
    }

    // Merge bitmaps.
    if (c_.isEmpty()) { // first time.
      c_ = std::move(c);
    } else {
      c_ &= c; // Logic AND.
      if (c_.isEmpty()) {
        LOG(INFO) << "the intersection of the two bitmaps is empty.";
        bHasResult_ = false;
        break;
      }
    }

    col_names.push_back(one.first);
    reader_iters_[col_id] = std::move(reader_iter);
  }
  reader_iters_.shrink_to_fit();
  //LOG(INFO) << "capture a bitmap: " << c_.toString();

  // Remove the predicates that have index.
  for (auto& name : col_names) {
    spec->RemovePredicate(name);
  }

  // Prepare for traversal.
  if (bHasResult_ && !c_.isEmpty()) {
    c_card_ = c_.cardinality();
    uint32_t* arr = new uint32_t[c_card_];
    CHECK(arr);
    c_.toUint32Array(arr);
    arr_.reset(arr);
  }

  return Status::OK();
}

Status CMultiIndexFileReader::Iterator::GetBounds(rowid_t* lower_bound_idx,
                                                  rowid_t* upper_bound_idx) {
  if (!bHasResult_) return Status::NotFound("");
  if (!c_.isEmpty()) {
    uint32_t* arr = arr_.get();
    *lower_bound_idx = arr[0];
    *upper_bound_idx = arr[c_card_-1] + 1; // Exclusive.
  }
  return Status::OK();
}

}
}
