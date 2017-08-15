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

#include "kudu/client/value-bloomfilter.h"
#include "kudu/client/value-bloomfilter-internal.h"

namespace kudu {
namespace client {

KuduValueBloomFilter::KuduValueBloomFilter()
  : data_(nullptr) {
}

KuduValueBloomFilter::~KuduValueBloomFilter() {
  delete data_;
}

KuduValueBloomFilter* KuduValueBloomFilter::Clone() const {
  KuduValueBloomFilter* one = new KuduValueBloomFilter();
  one->data_ = this->data_->Clone();
  return one;
}

void KuduValueBloomFilter::Insert(KuduValue* value) {
  /*gscoped_ptr<KuduValue> one(value);*/
  data_->Insert(value);
  return;
}

bool KuduValueBloomFilter::Find(KuduValue* value) const {
  /*gscoped_ptr<KuduValue> one(value);*/
  return data_->Find(value);
}

void* KuduValueBloomFilter::GetBloomFilter() const{
  return data_->GetBloomFilter();
}

KuduValueBloomFilterBuilder::KuduValueBloomFilterBuilder()
  : data_(new KuduValueBloomFilterBuilder::Data()) {
}

KuduValueBloomFilterBuilder::~KuduValueBloomFilterBuilder() {
  delete data_;
}

KuduValueBloomFilterBuilder& KuduValueBloomFilterBuilder::SetKuduSchema(KuduSchema* schema) {
  data_->SetKuduSchema(schema);
  return *this;
}

KuduValueBloomFilterBuilder& KuduValueBloomFilterBuilder::SetColumnName(std::string& col_name) {
  data_->SetColumnName(col_name);
  return *this;
}

KuduValueBloomFilterBuilder& KuduValueBloomFilterBuilder::SetLogSpace(const size_t ndv, const double fpp) {
  data_->SetLogSpace(ndv, fpp);
  return *this;
}

Status KuduValueBloomFilterBuilder::Build(sp::shared_ptr<KuduValueBloomFilter>* bloomfilter) {
  return data_->Build(bloomfilter);
}

} // namespace client
} // namespace kudu
