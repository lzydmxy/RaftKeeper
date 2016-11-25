#include <DB/Dictionaries/ExecutableDictionarySource.h>

#include <DB/Common/ShellCommand.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

ExecutableDictionarySource::ExecutableDictionarySource(const DictionaryStructure & dict_struct_,
	const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
	Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	path{config.getString(config_prefix + ".path")},
	format{config.getString(config_prefix + ".format")},
	sample_block{sample_block},
	context(context)
{
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other) :
	  dict_struct{other.dict_struct},
	  path{other.path},
	  format{other.format},
	  sample_block{other.sample_block},
	  context(other.context)
{
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
	LOG_TRACE(log, "loadAll " + toString());
	auto process = ShellCommand::execute(path);
	auto stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	LOG_TRACE(log, "loadIds " + toString() + " ids=" + std::to_string(ids.size()));
	auto process = ShellCommand::execute(path);

	{
		ColumnWithTypeAndName column;
		column.type = std::make_shared<DataTypeUInt64>();
		column.column = column.type->createColumn();

		for (auto & id : ids) {
			column.column->insert(id); //CHECKME maybe faster?
		}

		Block block;
		block.insert(std::move(column));

		auto stream_out = context.getOutputFormat(format, process->in, sample_block);
		stream_out->writePrefix();
		stream_out->write(block);
		stream_out->writeSuffix();
		stream_out->flush();
	}

	process->in.close();

	/*
	std::string process_err;
	readStringUntilEOF(process_err, process->err);
	std::cerr << "readed STDERR [" <<  process_err  << "] " << std::endl;
	*/

	auto stream = context.getInputFormat( format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	LOG_TRACE(log, "loadKeys " + toString() + " rows=" + std::to_string(requested_rows.size()));
	auto process = ShellCommand::execute(path);

	{
		Block block;

		const auto keys_size = key_columns.size();
		for (const auto i : ext::range(0, keys_size))
		{
			const auto & key_description = (*dict_struct.key)[i];
			const auto & key = key_columns[i];
			ColumnWithTypeAndName column;
			column.type = key_description.type;
			column.column = key->clone(); // CHECKME !!
			block.insert(std::move(column));
		}

		auto stream_out = context.getOutputFormat(format, process->in, sample_block);
		stream_out->writePrefix();
		stream_out->write(block);
		stream_out->writeSuffix();
		stream_out->flush();

	}

		process->in.close();

	/*
	std::string process_err;
	readStringUntilEOF(process_err, process->err);
	std::cerr << "readed STDERR [" <<  process_err  << "] " << std::endl;
	*/

	auto stream = context.getInputFormat( format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

bool ExecutableDictionarySource::isModified() const
{
	return true;
}

bool ExecutableDictionarySource::supportsSelectiveLoad() const
{
	return true;
}

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
	return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
	return "Executable: " + path;
}

}
