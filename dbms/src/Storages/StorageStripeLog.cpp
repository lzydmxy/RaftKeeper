#include <map>

#include <Poco/Path.h>
#include <Poco/Util/XMLConfiguration.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedReadBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNested.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>

#include <DB/Columns/ColumnArray.h>

#include <DB/Storages/StorageStripeLog.h>
#include <Poco/DirectoryIterator.h>


namespace DB
{

#define INDEX_BUFFER_SIZE 4096


class StripeLogBlockInputStream : public IProfilingBlockInputStream
{
public:
	StripeLogBlockInputStream(const NameSet & column_names_, StorageStripeLog & storage_, size_t max_read_buffer_size_,
		std::shared_ptr<const IndexForNativeFormat> & index_,
		IndexForNativeFormat::Blocks::const_iterator index_begin_,
		IndexForNativeFormat::Blocks::const_iterator index_end_)
		: column_names(column_names_.begin(), column_names_.end()), storage(storage_),
		index(index_), index_begin(index_begin_), index_end(index_end_),
		data_in(std::make_unique<CompressedReadBufferFromFile>(storage.full_path() + "data.bin", 0, 0, max_read_buffer_size_)),
		block_in(std::make_unique<NativeBlockInputStream>(*data_in, 0, true, index_begin, index_end))
	{
	}

	String getName() const override { return "StripeLog"; }

	String getID() const override
	{
		std::stringstream s;
		s << "StripeLog";
		for (const auto & name : column_names)
			s << ", " << name;	/// NOTE Отсутствует эскейпинг.
		return s.str();
	}

protected:
	Block readImpl() override
	{
		Block res;

		if (block_in)
		{
			res = block_in->read();

			/// Освобождаем память раньше уничтожения объекта.
			if (!res)
			{
				block_in.reset();
				data_in.reset();
				index.reset();
			}
		}

		return res;
	}

private:
	NameSet column_names;
	StorageStripeLog & storage;

	std::shared_ptr<const IndexForNativeFormat> index;
	IndexForNativeFormat::Blocks::const_iterator index_begin;
	IndexForNativeFormat::Blocks::const_iterator index_end;

	/** unique_ptr - чтобы удалять объекты (освобождать буферы) после исчерпания источника
	  * - для экономии оперативки при использовании большого количества источников.
	  */
	std::unique_ptr<CompressedReadBufferFromFile> data_in;
	std::unique_ptr<NativeBlockInputStream> block_in;
};


class StripeLogBlockOutputStream : public IBlockOutputStream
{
public:
	StripeLogBlockOutputStream(StorageStripeLog & storage_)
		: storage(storage_), lock(storage.rwlock),
		data_out_compressed(storage.full_path() + "data.bin"),
		data_out(data_out_compressed, CompressionMethod::LZ4, storage.max_compress_block_size),
		index_out_compressed(storage.full_path() + "index.mrk", INDEX_BUFFER_SIZE),
		index_out(index_out_compressed),
		block_out(data_out, 0, &index_out)
	{
	}

	~StripeLogBlockOutputStream()
	{
		try
		{
			writeSuffix();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	void write(const Block & block) override
	{
		block_out.write(block);
	}

	void writeSuffix() override
	{
		if (done)
			return;

		block_out.writeSuffix();
		data_out.next();
		data_out_compressed.next();
		index_out.next();
		index_out_compressed.next();

		FileChecker::Files files{ data_out_compressed.getFileName(), index_out_compressed.getFileName() };
		storage.file_checker.update(files.begin(), files.end());

		done = true;
	}

private:
	StorageStripeLog & storage;
	Poco::ScopedWriteRWLock lock;

	WriteBufferFromFile data_out_compressed;
	CompressedWriteBuffer data_out;
	WriteBufferFromFile index_out_compressed;
	CompressedWriteBuffer index_out;
	NativeBlockOutputStream block_out;

	bool done = false;
};


StorageStripeLog::StorageStripeLog(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach,
	size_t max_compress_block_size_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	path(path_), name(name_), columns(columns_),
	max_compress_block_size(max_compress_block_size_),
	file_checker(path + escapeForFileName(name) + '/' + "sizes.json"),
	log(&Logger::get("StorageStripeLog"))
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageStripeLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	String full_path = path + escapeForFileName(name) + '/';
	if (!attach)
	{
		/// создаём файлы, если их нет
		if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
			throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
	}
}

StoragePtr StorageStripeLog::create(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach,
	size_t max_compress_block_size_)
{
	return (new StorageStripeLog{
		path_, name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		attach, max_compress_block_size_
	})->thisPtr();
}


void StorageStripeLog::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

	path = new_path_to_db;
	name = new_table_name;
	file_checker.setPath(path + escapeForFileName(name) + "/" + "sizes.json");
}


BlockInputStreams StorageStripeLog::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	unsigned threads)
{
	Poco::ScopedReadRWLock lock(rwlock);

	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	NameSet column_names_set(column_names.begin(), column_names.end());

	if (!Poco::File(full_path() + "index.mrk").exists())
		return { new NullBlockInputStream };

	CompressedReadBufferFromFile index_in(full_path() + "index.mrk", 0, 0, INDEX_BUFFER_SIZE);
	std::shared_ptr<const IndexForNativeFormat> index{std::make_shared<IndexForNativeFormat>(index_in, column_names_set)};

	BlockInputStreams res;

	size_t size = index->blocks.size();
	if (threads > size)
		threads = size;

	for (size_t thread = 0; thread < threads; ++thread)
	{
		IndexForNativeFormat::Blocks::const_iterator begin = index->blocks.begin();
		IndexForNativeFormat::Blocks::const_iterator end = index->blocks.begin();

		std::advance(begin, thread * size / threads);
		std::advance(end, (thread + 1) * size / threads);

		res.emplace_back(new StripeLogBlockInputStream(column_names_set, *this, settings.max_read_buffer_size, index, begin, end));
	}

	/// Непосредственно во время чтения не держим read lock, потому что мы читаем диапазоны данных, которые не меняются.

	return res;
}


BlockOutputStreamPtr StorageStripeLog::write(
	ASTPtr query, const Settings & settings)
{
	return new StripeLogBlockOutputStream(*this);
}


bool StorageStripeLog::checkData() const
{
	Poco::ScopedReadRWLock lock(const_cast<Poco::RWLock &>(rwlock));
	return file_checker.check();
}

}
