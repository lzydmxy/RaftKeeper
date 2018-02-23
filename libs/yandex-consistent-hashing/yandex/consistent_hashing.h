#pragma once

#include <cstdint>
#include <cstddef>

/*
 * Maps random ui64 x (in fact hash of some string) to n baskets/shards.
 * Output value is id of a basket. 0 <= ConsistentHashing(x, n) < n.
 * Probability of all baskets must be equal. Also, it should be consistent
 * in terms, that with different n_1 < n_2 probability of
 * ConsistentHashing(x, n_1) != ConsistentHashing(x, n_2) must be equal to
 * (n_2 - n_1) / n_2 - the least possible with previous conditions.
 * It requires O(1) memory and cpu to calculate. So, it is faster than classic
 * consistent hashing algos with points on circle.
 */
std::size_t ConsistentHashing(std::uint64_t x, std::size_t n); // Works good for n < 65536
std::size_t ConsistentHashing(std::uint64_t lo, std::uint64_t hi, std::size_t n); // Works good for n < 4294967296
