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
#ifndef KUDU_CLIENT_VALUE_BLOOMFILTER_INTERNAL_H
#define KUDU_CLIENT_VALUE_BLOOMFILTER_INTERNAL_H

#include <string>

#include "kudu/client/value-bloomfilter.h"
#include "kudu/common/bloomfilter/bloom-filter.h"
#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {
namespace client {

class KuduValueBloomFilter::Data {
public:
  explicit Data(const std::string& col_name,
                const DataType type,
                const int log_heap_space);
  explicit Data(const std::string& col_name,
                const DataType type);
  ~Data();

  KuduValueBloomFilter::Data* Clone() const;

  void Insert(const KuduValue* value);

  bool Find(const KuduValue* value) const;

  impala::BloomFilter* GetBloomFilter() const;

  void SetBloomFilter(impala::BloomFilter* bf);

private:
  std::string col_name_;
  DataType type_;
  impala::BloomFilter* bf_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

class KuduValueBloomFilterBuilder::Data {
public:
  explicit Data();
  ~Data();

  void SetKuduSchema(const KuduSchema* schema);

  void SetColumnName(const std::string& col_name);

  void SetLogSpace(const size_t ndv, const double fpp);

  KuduValueBloomFilter* Build() const;

  KuduValueBloomFilter* Build(void* bf) const;

private:
  const KuduSchema* schema_;
  std::string col_name_;
  int log_heap_space_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif /* KUDU_CLIENT_VALUE_BLOOMFILTER_INTERNAL_H */
