#pragma once

#include <string.h>
#include <memory>
#include <vector>
#include <Poco/SharedPtr.h>
#include <Yandex/likely.h>


namespace DB
{


/** Пул, в который можно складывать что-нибудь. Например, короткие строки.
  * Сценарий использования:
  * - складываем много строк и запоминаем их адреса;
  * - адреса остаются валидными в течение жизни пула;
  * - при уничтожении пула, вся память освобождается;
  * - память выделяется и освобождается большими кусками;
  * - удаление части данных не предусмотрено;
  */
class Arena
{
private:
	/// Непрерывный кусок памяти и указатель на свободное место в нём. Односвязный список.
	struct Chunk : private std::allocator<char>	/// empty base optimization
	{
		char * begin;
		char * pos;
		char * end;

		Chunk * prev;

		Chunk(size_t size_, Chunk * prev_)
		{
			begin = allocate(size_);
			pos = begin;
			end = begin + size_;
			prev = prev_;
		}

		~Chunk()
		{
			deallocate(begin, size());

			if (prev)
				delete prev;
		}

		size_t size() { return end - begin; }
	};

	size_t growth_factor;
	size_t linear_growth_threshold;
	
	/// Последний непрерывный кусок памяти.
	Chunk * head;
	size_t size_in_bytes;

	static size_t roundUpToPageSize(size_t s)
	{
		return (s + 4096 - 1) / 4096;
	}

	/// Если размер чанка меньше linear_growth_threshold, то рост экспоненциальный, иначе - линейный, для уменьшения потребления памяти.
	size_t nextSize(size_t min_next_size) const
	{
		size_t size_after_grow = 0;
		
		if (head->size() < linear_growth_threshold)
			size_after_grow = head->size() * growth_factor;
		else
			size_after_grow = head->size() + linear_growth_threshold;

		if (size_after_grow < min_next_size)
			size_after_grow = min_next_size;

		return roundUpToPageSize(size_after_grow);
	}

	/// Добавить следующий непрерывный кусок памяти размера не меньше заданного.
	void addChunk(size_t min_size)
	{
		head = new Chunk(nextSize(min_size), head);
		size_in_bytes += head->size();
	}

public:
	Arena(size_t initial_size_ = 4096, size_t growth_factor_ = 2, size_t linear_growth_threshold_ = 128 * 1024 * 1024)
		: growth_factor(growth_factor_), linear_growth_threshold(linear_growth_threshold_),
		head(new Chunk(initial_size_, NULL)), size_in_bytes(head->size())
	{
	}

	~Arena()
	{
		delete head;
	}

	/// Получить кусок памяти, без выравнивания.
	char * alloc(size_t size)
	{
		if (unlikely(head->pos + size > head->end))
			addChunk(size);

		char * res = head->pos;
		head->pos += size;
		return res;
	}

	/// Вставить строку без выравнивания.
	const char * insert(const char * data, size_t size)
	{
		char * res = alloc(size);
		memcpy(res, data, size);
		return res;
	}
	
	/// Размер выделенного пула в байтах
	size_t size() const
	{
		return size_in_bytes;
	}
};

typedef Poco::SharedPtr<Arena> ArenaPtr;
typedef std::vector<ArenaPtr> Arenas;


}
