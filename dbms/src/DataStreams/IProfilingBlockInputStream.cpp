#include <iomanip>

/*#include <Poco/Mutex.h>
#include <Poco/Ext/ThreadNumber.h>*/

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


void BlockStreamProfileInfo::read(ReadBuffer & in)
{
	readVarUInt(rows, in);
	readVarUInt(blocks, in);
	readVarUInt(bytes, in);
	readBinary(applied_limit, in);
	readVarUInt(rows_before_limit, in);
	readBinary(calculated_rows_before_limit, in);	
}


void BlockStreamProfileInfo::write(WriteBuffer & out) const
{
	writeVarUInt(rows, out);
	writeVarUInt(blocks, out);
	writeVarUInt(bytes, out);
	writeBinary(hasAppliedLimit(), out);
	writeVarUInt(getRowsBeforeLimit(), out);
	writeBinary(calculated_rows_before_limit, out);
}


size_t BlockStreamProfileInfo::getRowsBeforeLimit() const
{
	if (!calculated_rows_before_limit)
		calculateRowsBeforeLimit();
	return rows_before_limit;
}


bool BlockStreamProfileInfo::hasAppliedLimit() const
{
	if (!calculated_rows_before_limit)
		calculateRowsBeforeLimit();
	return applied_limit;
}	


void BlockStreamProfileInfo::update(Block & block)
{
	++blocks;
	rows += block.rows();
	bytes += block.bytes();

	if (column_names.empty())
		column_names = block.dumpNames();
}


void BlockStreamProfileInfo::calculateRowsBeforeLimit() const
{
	calculated_rows_before_limit = true;
	applied_limit |= stream_name == "Limit";
	rows_before_limit = 0;
	for (BlockStreamProfileInfos::const_iterator it = nested_infos.begin(); it != nested_infos.end(); ++it)
	{
		const BlockStreamProfileInfo & info = **it;
		info.calculateRowsBeforeLimit();
		if (stream_name == "Limit" && info.rows_before_limit == 0) 
			rows_before_limit += info.rows;
		else
			rows_before_limit += info.rows_before_limit;
	}
}


void BlockStreamProfileInfo::print(std::ostream & ostr) const
{
	UInt64 elapsed 			= work_stopwatch.elapsed();
	UInt64 nested_elapsed	= 0;
	double elapsed_seconds	= work_stopwatch.elapsedSeconds();
	double nested_elapsed_seconds = 0;
	
	UInt64 nested_rows 		= 0;
	UInt64 nested_blocks 	= 0;
	UInt64 nested_bytes 	= 0;
	
	if (!nested_infos.empty())
	{
		for (BlockStreamProfileInfos::const_iterator it = nested_infos.begin(); it != nested_infos.end(); ++it)
		{
			if ((*it)->work_stopwatch.elapsed() > nested_elapsed)
			{
				nested_elapsed = (*it)->work_stopwatch.elapsed();
				nested_elapsed_seconds = (*it)->work_stopwatch.elapsedSeconds();
			}
			
			nested_rows 	+= (*it)->rows;
			nested_blocks	+= (*it)->blocks;
			nested_bytes 	+= (*it)->bytes;
		}
	}
	
	ostr 	<< std::fixed << std::setprecision(2)
			<< "Columns: " << column_names << std::endl
			<< "Elapsed:        " << elapsed_seconds << " sec. "
			<< "(" << elapsed * 100.0 / total_stopwatch.elapsed() << "%), " << std::endl;

	if (!nested_infos.empty())
	{
		double self_percents = (elapsed - nested_elapsed) * 100.0 / total_stopwatch.elapsed();
		
		ostr<< "Elapsed (self): " << (elapsed_seconds - nested_elapsed_seconds) << " sec. "
			<< "(" << (self_percents >= 50 ? "\033[1;31m" : (self_percents >= 10 ? "\033[1;33m" : ""))	/// Раскраска больших значений
				<< self_percents << "%"
				<< (self_percents >= 10 ? "\033[0m" : "") << "), " << std::endl
			<< "Rows (in):      " << nested_rows << ", per second: " << nested_rows / elapsed_seconds << ", " << std::endl
			<< "Blocks (in):    " << nested_blocks << ", per second: " << nested_blocks / elapsed_seconds << ", " << std::endl
			<< "                " << nested_bytes / 1000000.0 << " MB (memory), "
				<< nested_bytes * 1000 / elapsed << " MB/s (memory), " << std::endl;

		if (self_percents > 0.1)
			ostr << "Rows per second (in, self): " << (nested_rows / (elapsed_seconds - nested_elapsed_seconds))
				<< ", " << (elapsed - nested_elapsed) / nested_rows << " ns/row, " << std::endl;
	}
		
	ostr 	<< "Rows (out):     " << rows << ", per second: " << rows / elapsed_seconds << ", " << std::endl
			<< "Blocks (out):   " << blocks << ", per second: " << blocks / elapsed_seconds << ", " << std::endl
			<< "                " << bytes / 1000000.0 << " MB (memory), " << bytes * 1000 / elapsed << " MB/s (memory), " << std::endl
			<< "Average block size (out): " << rows / blocks << "." << std::endl;
}


Block IProfilingBlockInputStream::read()
{
	if (!info.started)
	{
		info.total_stopwatch.start();
		info.stream_name = getShortName();

		for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
			if (const IProfilingBlockInputStream * child = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
				info.nested_infos.push_back(&child->info);
		
		info.started = true;
	}

	if (is_cancelled)
		return Block();

	info.work_stopwatch.start();
	Block res = readImpl();
	info.work_stopwatch.stop();

/*	if (res)
	{
		static Poco::FastMutex mutex;
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		std::cerr << std::endl;
		std::cerr << "[ " << Poco::ThreadNumber::get() << " ]\t" << getShortName() << std::endl;
		std::cerr << "[ " << Poco::ThreadNumber::get() << " ]\t";

		for (size_t i = 0; i < res.columns(); ++i)
		{
			if (i != 0)
				std::cerr << ", ";
			std::cerr << res.getByPosition(i).name << " (" << res.getByPosition(i).column->size() << ")";
		}
		
		std::cerr << std::endl;
	}*/

	if (res)
		info.update(res);
	else
	{
		/** Если поток закончился, то ещё попросим всех детей прервать выполнение.
		  * Это имеет смысл при выполнении запроса с LIMIT-ом:
		  * - бывает ситуация, когда все необходимые данные уже прочитали,
		  *   но источники-дети ещё продолжают работать,
		  *   при чём они могут работать в отдельных потоках или даже удалённо.
		  */
		cancel();
	}

	progress(res);

	/// Проверка ограничений.
	if ((limits.max_rows_to_read && info.rows > limits.max_rows_to_read)
		|| (limits.max_bytes_to_read && info.bytes > limits.max_bytes_to_read))
	{
		if (limits.read_overflow_mode == Limits::THROW)
			throw Exception("Limit for rows to read exceeded: read " + toString(info.rows)
				+ " rows, maximum: " + toString(limits.max_rows_to_read),
				ErrorCodes::TOO_MUCH_ROWS);

		if (limits.read_overflow_mode == Limits::BREAK)
			return Block();

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	if (limits.max_execution_time != 0
		&& info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
	{
		if (limits.timeout_overflow_mode == Limits::THROW)
			throw Exception("Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
				+ " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
			ErrorCodes::TIMEOUT_EXCEEDED);

		if (limits.timeout_overflow_mode == Limits::BREAK)
			return Block();

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	if (limits.min_execution_speed
		&& info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.timeout_before_checking_execution_speed.totalMicroseconds()) * 1000
		&& info.rows / info.total_stopwatch.elapsedSeconds() < limits.min_execution_speed)
	{
		throw Exception("Query is executing too slow: " + toString(info.rows / info.total_stopwatch.elapsedSeconds())
			+ " rows/sec., minimum: " + toString(limits.min_execution_speed),
			ErrorCodes::TOO_SLOW);
	}

	/// Проверка квоты.
	if (quota != NULL)
	{
		time_t current_time = time(0);
		double total_elapsed = info.total_stopwatch.elapsedSeconds();

		switch (quota_mode)
		{
			case QUOTA_READ:
				quota->checkAndAddReadRowsBytes(current_time, res.rows(), res.bytes());
				break;

			case QUOTA_RESULT:
				quota->checkAndAddResultRowsBytes(current_time, res.rows(), res.bytes());
				quota->checkAndAddExecutionTime(current_time, Poco::Timespan((total_elapsed - prev_elapsed) * 1000000.0));
				break;

			default:
				throw Exception("Logical error: unknown quota mode.", ErrorCodes::LOGICAL_ERROR);
		}

		prev_elapsed = total_elapsed;
	}
	
	return res;
}


void IProfilingBlockInputStream::progress(Block & block)
{
	if (children.empty() && progress_callback)
		progress_callback(block.rows(), block.bytes());
}
	

const BlockStreamProfileInfo & IProfilingBlockInputStream::getInfo() const
{
	return info;
}


void IProfilingBlockInputStream::cancel()
{
	if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
		return;

	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->cancel();
}


void IProfilingBlockInputStream::setProgressCallback(ProgressCallback callback)
{
	progress_callback = callback;
	
	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->setProgressCallback(callback);
}


const Block & IProfilingBlockInputStream::getTotals() const
{
	if (totals)
		return totals;

	for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
	{
		if (const IProfilingBlockInputStream * child = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
		{
			const Block & res = child->getTotals();
			if (res)
				return res;
		}
	}

	return totals;
}


}
