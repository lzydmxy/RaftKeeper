#pragma once

#include <DB/Core/ErrorCodes.h>
#include <DB/Common/Arena.h>
#include <common/likely.h>
#include <ext/range.hpp>
#include <ext/size.hpp>
#include <ext/bit_cast.hpp>
#include <cstdlib>
#include <memory>


namespace DB
{


class SmallObjectPool
{
private:
	struct Block { Block * next; };

	const std::size_t object_size;
	Arena pool;
	Block * free_list;

public:
	SmallObjectPool(
		const std::size_t object_size, const std::size_t initial_size = 4096, const std::size_t growth_factor = 2,
		const std::size_t linear_growth_threshold = 128 * 1024 * 1024)
		: object_size{object_size}, pool{initial_size, growth_factor, linear_growth_threshold}
	{
		if (object_size < sizeof(Block))
			throw Exception{
				"Can't make allocations smaller than sizeof(Block) = " + std::to_string(sizeof(Block)),
				ErrorCodes::LOGICAL_ERROR
			};

		const auto num_objects = pool.size() / object_size;
		auto head = free_list = ext::bit_cast<Block *>(pool.alloc(num_objects * object_size));

		for (const auto i : ext::range(0, num_objects - 1))
		{
			(void) i;
			head->next = ext::bit_cast<Block *>(ext::bit_cast<char *>(head) + object_size);
			head = head->next;
		}

		head->next = nullptr;
	}

	char * alloc()
	{
		if (free_list)
		{
			const auto res = reinterpret_cast<char *>(free_list);
			free_list = free_list->next;
			return res;
		}

		return pool.alloc(object_size);
	}

	void free(const void * ptr)
	{
		union {
			const void * p_v;
			Block * block;
		};

		p_v = ptr;
		block->next = free_list;

		free_list = block;
	}

	/// Размер выделенного пула в байтах
	size_t size() const
	{
		return pool.size();
	}

};


}
