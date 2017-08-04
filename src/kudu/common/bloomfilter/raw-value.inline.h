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


#ifndef IMPALA_RUNTIME_RAW_VALUE_INLINE_H
#define IMPALA_RUNTIME_RAW_VALUE_INLINE_H

//#include "runtime/raw-value.h"

#include <cmath>

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/functional/hash.hpp>

#include "glog/logging.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/bloomfilter/hash-util.h"

namespace impala {

/// Arbitrary constants used to compute hash values for special cases. Constants were
/// obtained by taking lower bytes of generated UUID. NULL and empty strings should
/// hash to different values.
static const uint32_t HASH_VAL_NULL = 0x58081667;
static const uint32_t HASH_VAL_EMPTY = 0x7dca7eee;

/// default hash seed. copy from impala.
static const uint32_t DEFAULT_HASH_SEED = 1234;

/// microsecond per second.
static const int64_t MICROS_PER_SEC = 1000000ll;

template<DataType T>
inline uint32_t GetHashValueNonNull(const void* v, uint32_t seed = 0);

template<>
inline uint32_t GetHashValueNonNull<INT8>(const void* v, uint32_t seed) 
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 1, seed);
}

template<>
inline uint32_t GetHashValueNonNull<INT16>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 2, seed);
}

template<>
inline uint32_t GetHashValueNonNull<INT32>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t GetHashValueNonNull<INT64>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 8, seed);
}

template<>
inline uint32_t GetHashValueNonNull<STRING>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  const Slice* s = reinterpret_cast<const Slice *>(v);
  if (s->size() == 0) {
    return HashUtil::HashCombine32(HASH_VAL_EMPTY, seed);
  }
  return HashUtil::MurmurHash2_64(s->data(), s->size(), seed);
}

template<>
inline uint32_t GetHashValueNonNull<BOOL>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::HashCombine32(*reinterpret_cast<const bool*>(v), seed);
}

template<>
inline uint32_t GetHashValueNonNull<FLOAT>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 4, seed);
}

template<>
inline uint32_t GetHashValueNonNull<DOUBLE>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);
  return HashUtil::MurmurHash2_64(v, 8, seed);
}

template<>
inline uint32_t GetHashValueNonNull<UNIXTIME_MICROS>(const void* v, uint32_t seed) {
  DCHECK(v != NULL);

  // convert unixtime to Impala's TimestampValue.
  int64_t unixtime = *reinterpret_cast<const int64_t*>(v);
  int64_t seconds = unixtime / MICROS_PER_SEC;
  int64_t micros = unixtime - (seconds * MICROS_PER_SEC);

  tm t;
  gmtime_r(&seconds, &t); /* localtime_r(&seconds, &t); */
  boost::posix_time::ptime p = ptime_from_tm(t);
  p += boost::posix_time::microseconds(micros);
  boost::posix_time::time_duration time = p.time_of_day();
  boost::gregorian::date date = p.date();

  char b[12];
  memcpy(b, &time, 8);
  memcpy(b+8, &date, 4);
  return HashUtil::MurmurHash2_64(b, 12, seed);
}

template<typename T>
inline uint32_t GetHashValue(const void* v, uint32_t seed) noexcept {
  // Use HashCombine with arbitrary constant to ensure we don't return seed.
  if (UNLIKELY(v == NULL)) return HashUtil::HashCombine32(HASH_VAL_NULL, seed);
  return GetHashValueNonNull<T>(v, seed);
}

}

#endif
