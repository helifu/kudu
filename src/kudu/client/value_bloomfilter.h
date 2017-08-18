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
  ///   The caller takes the ownership of the object.
  KuduValueBloomFilter* Clone() const;

  /// @Add A KuduValue to the BloomFilter.
  ///
  /// @param [in] value
  ///   Value to insert.
  void Insert(const KuduValue* value);

  /// @Check whether A KuduValue is in the BloomFilter.
  ///
  /// @param [in] value
  ///   Value to find.
  /// @return true if the value is in the BloomFilter.
  bool Find(const KuduValue* value) const;

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

  /// @Set Schema for the KuduValueBloomFilter.
  KuduValueBloomFilterBuilder& SetKuduSchema(const KuduSchema* schema);

  /// @Set Column  for the KuduValueBloomFilter.
  KuduValueBloomFilterBuilder& SetColumnName(const std::string& col_name);

  /// @Set the log heap space for the KuduValueBloomFilter
  KuduValueBloomFilterBuilder& SetLogSpace(const size_t ndv, const double fpp);

  /// @brief
  ///   The caller must call SetKuduSchema & SetColumnName & SetLogSpace before 
  ///   calling Build, otherwise it won't succeed.
  /// @Return a KuduValueBloomFilter object.
  ///   The caller owns the result until it is passed into
  ///   KuduScanner::AddConjunctPredicate().
  KuduValueBloomFilter* Build() const;

  /// @brief
  ///   It's not necessary to call SetKuduSchema & SetColumnName before calling
  ///   Build, but if you want to call Insert or Find continually so you have to.
  /// @param [in] bf
  ///   The API will clone the 'bf' object inside.
  /// @Return a KuduValueBloomFilter object.
  ///   The caller owns the result until it is passed into
  ///   KuduScanner::AddConjunctPredicate().
  KuduValueBloomFilter* Build(const void* bf) const;

private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduValueBloomFilterBuilder);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_VALUE_BLOOMFILTER_H */
