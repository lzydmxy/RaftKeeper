#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  */
class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: input(input_), description(description_), has_been_read(false), log(&Logger::get("MergeSortingBlockInputStream"))
	{
		children.push_back(input);
	}

	Block readImpl();

	String getName() const { return "MergeSortingBlockInputStream"; }

	BlockInputStreamPtr clone() { return new MergeSortingBlockInputStream(input, description); }

private:
	BlockInputStreamPtr input;
	SortDescription description;

	/// Всё было прочитано.
	bool has_been_read;

	Logger * log;
	
	/** Слить сразу много блоков с помощью priority queue. 
	  */
	Block merge(Blocks & blocks);
};

}
