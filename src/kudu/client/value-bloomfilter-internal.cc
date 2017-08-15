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

KuduValueBloomFilter::Data::Data(KuduSchema* schema,
                                 const std::string& col_name,
                                 int log_heap_space)
  : schema_(schema)
  , col_name_(col_name)
  , bf_(new impala::BloomFilter(log_heap_space)) {
}

KuduValueBloomFilter::Data::Data()
  : col_name_("")
  , bf_(nullptr) {
}

KuduValueBloomFilter::Data::~Data() {
  delete bf_;
}

KuduValueBloomFilter::Data* KuduValueBloomFilter::Data::Clone() const {
  KuduValueBloomFilter::Data* one = new KuduValueBloomFilter::Data();
  one->schema_.reset(new KuduSchema(*this->schema_));
  one->col_name_ = this->col_name_;
  one->bf_ = this->bf_->Clone();
  return one;
}

void KuduValueBloomFilter::Data::Insert(const KuduValue* value) {
  int col_idx = schema_->schema_->find_column(col_name_);
  const ColumnSchema& col_schema = schema_->schema_->column(col_idx);
  DataType type = col_schema.type_info()->physical_type();

  void* val_void;
  value->data_->CheckTypeAndGetPointer(col_schema.name(), type, &val_void);
  switch (type) {
  case INT8:
    {
      int8_t v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<INT8>(&v));
      break;
    }
  case INT16:
    {
      int16_t v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<INT16>(&v));
      break;
    }
  case INT32:
    {
      int32_t v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<INT32>(&v));
      break;
    }
  case kudu::INT64:
    {
      int64_t v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<INT64>(&v));
      break;
    }
  case kudu::BOOL:
    {
      bool v = *reinterpret_cast<const int64_t*>(value) ? true : false;
      bf_->Insert(impala::GetHashValue<BOOL>(&v));
      break;
    }
  case kudu::FLOAT:
    {
      float v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<FLOAT>(&v));
      break;
    }
  case kudu::DOUBLE:
    {
      double v = *reinterpret_cast<const int64_t*>(value);
      bf_->Insert(impala::GetHashValue<DOUBLE>(&v));
      break;
    }
  case kudu::BINARY:
    {
      bf_->Insert(impala::GetHashValue<STRING>(value));
      break;
    }
  default:
    {
      LOG(FATAL) << strings::Substitute("Unexpected physical type: %0", type);
      break;
    }
  }
  return;
}

bool KuduValueBloomFilter::Data::Find(const KuduValue* value) const {
  int col_idx = schema_->schema_->find_column(col_name_);
  const ColumnSchema& col_schema = schema_->schema_->column(col_idx);
  DataType type = col_schema.type_info()->physical_type();

  void* val_void;
  value->data_->CheckTypeAndGetPointer(col_schema.name(), type, &val_void);
  switch (type)
  {
  case kudu::INT8:
    {
      int8_t v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<INT8>(&v));
    }
  case kudu::INT16:
    {
      int16_t v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<INT16>(&v));
    }
  case kudu::INT32:
    {
      int32_t v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<INT32>(&v));
    }
  case kudu::INT64:
    {
      int64_t v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<INT64>(&v));
    }
  case kudu::BOOL:
    {
      bool v = *reinterpret_cast<const int64_t*>(value) ? true : false;
      return bf_->Find(impala::GetHashValue<BOOL>(&v));
    }
  case kudu::FLOAT:
    {
      float v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<FLOAT>(&v));
    }
  case kudu::DOUBLE:
    {
      double v = *reinterpret_cast<const int64_t*>(value);
      return bf_->Find(impala::GetHashValue<DOUBLE>(&v));
    }
  case kudu::BINARY:
    {
      return bf_->Find(impala::GetHashValue<STRING>(value));
    }
  default:
    {
      LOG(FATAL) << strings::Substitute("Unexpected physical type: %0", type);
      break;
    }
  }
  return false;
}

impala::BloomFilter* KuduValueBloomFilter::Data::GetBloomFilter() const {
  return bf_;
}

KuduValueBloomFilterBuilder::Data::Data()
  : schema_(nullptr)
  , col_name_("")
  , log_heap_space_(0) {
}

KuduValueBloomFilterBuilder::Data::~Data() {
}

void KuduValueBloomFilterBuilder::Data::SetKuduSchema(KuduSchema* schema) {
  schema_.reset(schema);
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

Status KuduValueBloomFilterBuilder::Data::Build(sp::shared_ptr<KuduValueBloomFilter>* bloomfilter) {
  if (schema_ == nullptr ||
      col_name_.empty() ||
      log_heap_space_ == 0) {
    return Status::Uninitialized("the parameters have not been set yet.");
  }

  int col_idx = schema_->schema_->find_column(col_name_);
  if (col_idx == Schema::kColumnNotFound) {
    return Status::NotFound("the column %0 is not found.", col_name_);
  }

  sp::shared_ptr<KuduValueBloomFilter> one(new KuduValueBloomFilter());
  one->data_ = new KuduValueBloomFilter::Data(schema_.release(), col_name_, log_heap_space_);
  bloomfilter->swap(one);
  return Status::OK();
}

} // namespace client
} // namespace kudu
