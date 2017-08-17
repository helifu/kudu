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
#ifndef KUDU_CLIENT_VALUE_BLOOMFILTER_H
#define KUDU_CLIENT_VALUE_BLOOMFILTER_H

#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/status.h"


namespace kudu {
namespace client {

/// @brief A constant cell value with a specific type.
class KUDU_EXPORT KuduValueBloomFilter {
public:
  /// @return A new identical KuduValueBloomFilter object.
  KuduValueBloomFilter* Clone() const;

  /// @Add A KuduValue to the BloomFilter.
  ///
  /// @param [in] value
  ///   The KuduValueBloomFilter takes the ownership over the value.
  void Insert(const KuduValue* value);

  /// @Check whether A KuduValue is in the BloomFilter.
  ///
  /// @param [in] value
  ///   The KuduValueBloomFilter takes the ownership over the value.
  /// @return true if the value is in the BloomFilter.
  bool Find(const KuduValue* value) const;

  /// @return the BloomFilter pointer.
  void* GetBloomFilter() const;

  ~KuduValueBloomFilter();

private:
  friend class BloomFilterPredicateData;
  friend class KuduValueBloomFilterBuilder;

  class KUDU_NO_EXPORT Data;
  explicit KuduValueBloomFilter();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduValueBloomFilter);
};

/// @brief A "factory" for KuduValueBloomFilter objects.
///
/// This class is used to create instances of the KuduValueBloomFilter class
/// with pre-set options/parameters.
class KUDU_EXPORT KuduValueBloomFilterBuilder {
public:
  KuduValueBloomFilterBuilder();
  ~KuduValueBloomFilterBuilder();

  /// @Set Schema for the BloomFilter.
  KuduValueBloomFilterBuilder& SetKuduSchema(const KuduSchema* schema);

  /// @Set Column  for the BloomFilter.
  KuduValueBloomFilterBuilder& SetColumnName(const std::string& col_name);

  /// @Set the log heap space for the BloomFilter
  KuduValueBloomFilterBuilder& SetLogSpace(const size_t ndv, const double fpp);

  /// @Return a BloomFilter after setting.
  KuduValueBloomFilter* Build() const;

private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduValueBloomFilterBuilder);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_VALUE_BLOOMFILTER_H */
