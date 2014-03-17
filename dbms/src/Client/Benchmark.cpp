#define DBMS_CLIENT 1	/// Используется в Context.h

#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>

#include <iostream>
#include <fstream>
#include <iomanip>

#include <Poco/File.h>
#include <Poco/SharedPtr.h>
#include <Poco/Util/Application.h>

#include <statdaemons/Stopwatch.h>
#include <statdaemons/threadpool.hpp>
#include <stats/ReservoirSampler.h>

#include <boost/program_options.hpp>

#include <DB/Common/ConcurrentBoundedQueue.h>

#include <DB/Core/Exception.h>
#include <DB/Core/Types.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/Context.h>

#include <DB/Client/Connection.h>

#include "InterruptListener.h"


/** Инструмент для измерения производительности ClickHouse
  *  при выполнении запросов с фиксированным количеством одновременных запросов.
  */

namespace DB
{

class Benchmark
{
public:
	Benchmark(unsigned concurrency_,
			const String & host_, UInt16 port_, const String & default_database_,
			const String & user_, const String & password_)
		: concurrency(concurrency_), queue(concurrency), pool(concurrency),
		connections(concurrency, host_, port_, default_database_, user_, password_, data_type_factory)
	{
		std::cerr << std::fixed << std::setprecision(3);

		readQueries();
		run();
		report();
	}

private:
	typedef std::string Query;

	unsigned concurrency;

	typedef std::vector<Query> Queries;
	Queries queries;

	typedef ConcurrentBoundedQueue<Query> Queue;
	Queue queue;

	boost::threadpool::pool pool;

	DataTypeFactory data_type_factory;
	ConnectionPool connections;

	Stopwatch total_watch;
	size_t total_queries = 0;
	size_t total_rows = 0;
	size_t total_bytes = 0;
	ReservoirSampler<double> sampler {1 << 16};
	Poco::FastMutex mutex;


	void readQueries()
	{
		ReadBufferFromFileDescriptor in(STDIN_FILENO);

		while (!in.eof())
		{
			std::string query;
			readText(query, in);
			assertString("\n", in);

			if (!query.empty())
				queries.emplace_back(query);
		}

		if (queries.empty())
			throw Exception("Empty list of queries.");

		std::cerr << "Loaded " << queries.size() << " queries." << std::endl;
	}


	void run()
	{
		for (size_t i = 0; i < concurrency; ++i)
			pool.schedule(std::bind(&Benchmark::thread, this, connections.get()));

		InterruptListener interrupt_listener;

		total_watch.restart();
		Stopwatch watch;

		/// В цикле, кладём все запросы в очередь.
		for (size_t i = 0; !interrupt_listener.check(); ++i)
		{
			if (i >= queries.size())
				i = 0;

			queue.push(queries[i]);

			if (watch.elapsedSeconds() > 1)
			{
				report();
				watch.restart();
			}
		}

		/// Попросим потоки завершиться.
		for (size_t i = 0; i < concurrency; ++i)
			queue.push("");

		pool.wait();
	}


	void thread(ConnectionPool::Entry connection)
	{
		try
		{
			/// В этих потоках не будем принимать сигнал INT.
			sigset_t sig_set;
			if (sigemptyset(&sig_set)
				|| sigaddset(&sig_set, SIGINT)
				|| pthread_sigmask(SIG_BLOCK, &sig_set, NULL))
				throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

			Query query;

			while (true)
			{
				queue.pop(query);

				/// Пустой запрос обозначает конец работы.
				if (query.empty())
					break;

				execute(connection, query);
			}
		}
		catch (const Exception & e)
		{
			std::string text = e.displayText();

			std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

			/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
			if (std::string::npos == text.find("Stack trace"))
				std::cerr << "Stack trace:" << std::endl
					<< e.getStackTrace().toString();

			throw;
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
			throw;
		}
		catch (const std::exception & e)
		{
			std::cerr << "std::exception: " << e.what() << std::endl;
			throw;
		}
		catch (...)
		{
			std::cerr << "Unknown exception" << std::endl;
			throw;
		}
	}


	void execute(ConnectionPool::Entry & connection, Query & query)
	{
		Stopwatch watch;
		RemoteBlockInputStream stream(*connection, query, nullptr);

		size_t rows = 0;
		size_t bytes = 0;
		stream.setProgressCallback([&](size_t rows_inc , size_t bytes_inc) { rows += rows_inc; bytes += bytes_inc; });

		stream.readPrefix();
		while (Block block = stream.read())
			;
		stream.readSuffix();

		addTiming(watch.elapsedSeconds(), rows, bytes);
	}


	void addTiming(double seconds, size_t rows, size_t bytes)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		++total_queries;
		total_rows += rows;
		total_bytes += bytes;
		sampler.insert(seconds);
	}


	void report()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		std::cerr
			<< std::endl
			<< "QPS: " << (total_queries / total_watch.elapsedSeconds()) << ", "
			<< "RPS: " << (total_rows / total_watch.elapsedSeconds()) << ", "
			<< "MiB/s: " << (total_bytes / total_watch.elapsedSeconds() / 1048576) << "."
			<< std::endl;

		for (double level = 0; level < 1; level += 0.1)
			std::cerr << int(level * 100) << "%\t" << sampler.quantileInterpolated(level) << " sec." << std::endl;
	}
};

}


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		boost::program_options::options_description desc("Allowed options");
		desc.add_options()
			("help", "produce help message")
			("concurrency,c", boost::program_options::value<unsigned>()->default_value(1), "number of parallel queries")
			("host,h", boost::program_options::value<std::string>()->default_value("localhost"), "")
			("port", boost::program_options::value<UInt16>()->default_value(9000), "")
			("user", boost::program_options::value<std::string>()->default_value("default"), "")
			("password", boost::program_options::value<std::string>()->default_value(""), "")
			("database", boost::program_options::value<std::string>()->default_value("default"), "")
		;

		boost::program_options::variables_map options;
		boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

		if (options.count("help"))
		{
			std::cout << "Usage: " << argv[0] << " [options] < queries.txt" << std::endl;
			std::cout << desc << std::endl;
			return 1;
		}

		Benchmark benchmark(
			options["concurrency"].as<unsigned>(),
			options["host"].as<std::string>(),
			options["port"].as<UInt16>(),
			options["database"].as<std::string>(),
			options["user"].as<std::string>(),
			options["password"].as<std::string>());
	}
	catch (const Exception & e)
	{
		std::string text = e.displayText();

		std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

		/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
		if (std::string::npos == text.find("Stack trace"))
			std::cerr << "Stack trace:" << std::endl
				<< e.getStackTrace().toString();

		return e.code();
	}
	catch (const Poco::Exception & e)
	{
		std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
		return ErrorCodes::POCO_EXCEPTION;
	}
	catch (const std::exception & e)
	{
		std::cerr << "std::exception: " << e.what() << std::endl;
		return ErrorCodes::STD_EXCEPTION;
	}
	catch (...)
	{
		std::cerr << "Unknown exception" << std::endl;
		return ErrorCodes::UNKNOWN_EXCEPTION;
	}
}
