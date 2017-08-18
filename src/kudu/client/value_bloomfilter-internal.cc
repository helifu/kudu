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

#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/client/value-internal.h"
#include "kudu/client/value-bloomfilter-internal.h"
#include "kudu/common/bloomfilter/raw-value.inline.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace client {

KuduValueBloomFilter::Data::Data(const std::string& col_name,
                                 const DataType type, 
                                 const int log_heap_space)
  : col_name_(col_name)
  , type_(type)
  , bf_(new impala::BloomFilter(log_heap_space)) {
}

KuduValueBloomFilter::Data::Data(const std::string& col_name,
                                 const DataType type)
  : col_name_(col_name)
  , type_(type)
  , bf_(nullptr) {
}

KuduValueBloomFilter::Data::~Data() {
  if (bf_) {
    delete bf_;
    bf_ = nullptr;
  }
}

KuduValueBloomFilter::Data* KuduValueBloomFilter::Data::Clone() const {
  KuduValueBloomFilter::Data* one = new KuduValueBloomFilter::Data(this->col_name_, this->type_);
  one->SetBloomFilter(this->bf_->Clone());
  return one;
}

void KuduValueBloomFilter::Data::Insert(const KuduValue* value) {
  if (type_ == kudu::UNKNOWN_DATA) return;
  void* val_void = nullptr;
  value->data_->CheckTypeAndGetPointer(col_name_, type_, &val_void);
  switch (type_) {
  case kudu::INT8:
    {
      int8_t v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<INT8>(&v));
      break;
    }
  case kudu::INT16:
    {
      int16_t v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<INT16>(&v));
      break;
    }
  case kudu::INT32:
    {
      int32_t v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<INT32>(&v));
      break;
    }
  case kudu::INT64:
    {
      int64_t v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<INT64>(&v));
      break;
    }
  case kudu::BOOL:
    {
      bool v = *reinterpret_cast<const int64_t*>(val_void) ? true : false;
      bf_->Insert(impala::GetHashValue<BOOL>(&v));
      break;
    }
  case kudu::FLOAT:
    {
      float v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<FLOAT>(&v));
      break;
    }
  case kudu::DOUBLE:
    {
      double v = *reinterpret_cast<const int64_t*>(val_void);
      bf_->Insert(impala::GetHashValue<DOUBLE>(&v));
      break;
    }
  case kudu::BINARY:
    {
      bf_->Insert(impala::GetHashValue<STRING>(val_void));
      break;
    }
  default:
    {
      LOG(FATAL) << strings::Substitute("Unexpected physical type: %0", type_);
      break;
    }
  }
  return;
}

bool KuduValueBloomFilter::Data::Find(const KuduValue* value) const {
  if (type_ == kudu::UNKNOWN_DATA) return false;
  void* val_void = nullptr;
  value->data_->CheckTypeAndGetPointer(col_name_, type_, &val_void);
  switch (type_)
  {
  case kudu::INT8:
    {
      int8_t v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<INT8>(&v));
    }
  case kudu::INT16:
    {
      int16_t v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<INT16>(&v));
    }
  case kudu::INT32:
    {
      int32_t v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<INT32>(&v));
    }
  case kudu::INT64:
    {
      int64_t v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<INT64>(&v));
    }
  case kudu::BOOL:
    {
      bool v = *reinterpret_cast<const int64_t*>(val_void) ? true : false;
      return bf_->Find(impala::GetHashValue<BOOL>(&v));
    }
  case kudu::FLOAT:
    {
      float v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<FLOAT>(&v));
    }
  case kudu::DOUBLE:
    {
      double v = *reinterpret_cast<const int64_t*>(val_void);
      return bf_->Find(impala::GetHashValue<DOUBLE>(&v));
    }
  case kudu::BINARY:
    {
      return bf_->Find(impala::GetHashValue<STRING>(val_void));
    }
  default:
    {
      LOG(FATAL) << strings::Substitute("Unexpected physical type: %0", type_);
      break;
    }
  }
  return false;
}

impala::BloomFilter* KuduValueBloomFilter::Data::GetBloomFilter() const {
  return bf_;
}

void KuduValueBloomFilter::Data::SetBloomFilter(impala::BloomFilter* bf) {
  bf_ = bf;
  return;
}

KuduValueBloomFilterBuilder::Data::Data()
  : schema_(nullptr)
  , col_name_("")
  , log_heap_space_(0) {
}

KuduValueBloomFilterBuilder::Data::~Data() {
}

void KuduValueBloomFilterBuilder::Data::SetKuduSchema(const KuduSchema* schema) {
  schema_ = schema;
  return;
}

void KuduValueBloomFilterBuilder::Data::SetColumnName(const std::string& col_name) {
  col_name_ = col_name;
  return;
}

void KuduValueBloomFilterBuilder::Data::SetLogSpace(const size_t ndv, const double fpp) {
  log_heap_space_ = impala::BloomFilter::MinLogSpace(ndv, fpp);
  return;
}

KuduValueBloomFilter* KuduValueBloomFilterBuilder::Data::Build() const {
  if (schema_ == nullptr ||
      col_name_.empty() ||
      log_heap_space_ == 0) {
    return nullptr;
  }

  int col_idx = schema_->schema_->find_column(col_name_);
  if (col_idx == Schema::kColumnNotFound) {
    return nullptr;
  }

  const ColumnSchema& col_schema = schema_->schema_->column(col_idx);
  DataType type = col_schema.type_info()->physical_type();

  KuduValueBloomFilter* one = new KuduValueBloomFilter();
  one->data_ = new KuduValueBloomFilter::Data(col_name_, type, log_heap_space_);
  return one;
}

KuduValueBloomFilter* KuduValueBloomFilterBuilder::Data::Build(void* bf) const {
  if (bf == nullptr) return nullptr;

  DataType type = kudu::UNKNOWN_DATA;
  if (schema_ != nullptr && !col_name_.empty()) {
    int col_idx = schema_->schema_->find_column(col_name_);
    if (col_idx == Schema::kColumnNotFound) {
      return nullptr;
    }
    const ColumnSchema& col_schema = schema_->schema_->column(col_idx);
    type = col_schema.type_info()->physical_type();
  }

  KuduValueBloomFilter* one = new KuduValueBloomFilter();
  one->data_ = new KuduValueBloomFilter::Data(col_name_, type);
  one->data_->SetBloomFilter(reinterpret_cast<impala::BloomFilter*>(bf));
  return one;
}

} // namespace client
} // namespace kudu
