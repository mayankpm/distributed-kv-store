#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

// Builds a per-SSTable bloom filter. With the default 10 bits per key the
// false positive rate is about 1%, so ~99% of lookups for keys that are not in
// a table skip the disk read entirely.
class BloomFilterBuilder {
 public:
  explicit BloomFilterBuilder(int bits_per_key = 10);

  void AddKey(std::string_view key);
  // Serialized filter: bit array followed by one byte holding the probe count.
  std::string Finish() const;

 private:
  int bits_per_key_;
  int num_probes_;
  std::vector<uint64_t> hashes_;
};

bool BloomMayContain(std::string_view filter, std::string_view key);

}  // namespace kv
