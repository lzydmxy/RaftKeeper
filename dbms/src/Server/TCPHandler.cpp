#include <iomanip>

#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/copyData.h>

#include <DB/Interpreters/executeQuery.h>

#include "TCPHandler.h"


namespace DB
{


void TCPHandler::runImpl()
{
	ReadBufferFromPocoSocket in(socket());
	WriteBufferFromPocoSocket out(socket());
	WriteBufferFromPocoSocket out_for_chunks(socket());
	
	/// Сразу после соединения, отправляем hello-пакет.
	sendHello(out);

	while (!in.eof())
	{
		/// Пакет с запросом.
		receivePacket(in);

		LOG_DEBUG(log, "Query ID: " << state.query_id);
		LOG_DEBUG(log, "Query: " << state.query);
		LOG_DEBUG(log, "In format: " << state.in_format);
		LOG_DEBUG(log, "Out format: " << state.out_format);

		Stopwatch watch;

		/// Читаем из сети данные для INSERT-а, если надо, и вставляем их.
		if (state.io.out)
		{
			while (receivePacket(in))
				;
		}

		/// Вынимаем результат выполнения запроса, если есть, и пишем его в сеть.
		if (state.io.in)
		{
			while (sendData(out, out_for_chunks))
				;
		}

		watch.stop();

		LOG_INFO(log, std::fixed << std::setprecision(3)
			<< "Processed in " << watch.elapsedSeconds() << " sec.");
	}
}


void TCPHandler::sendHello(WriteBuffer & out)
{
	writeVarUInt(Protocol::Server::Hello, out);
	writeStringBinary(DBMS_NAME, out);
	writeVarUInt(DBMS_VERSION_MAJOR, out);
	writeVarUInt(DBMS_VERSION_MINOR, out);
	writeVarUInt(Revision::get(), out);
	out.next();
}

bool TCPHandler::receivePacket(ReadBuffer & in)
{
	UInt64 packet_type = 0;
	readVarUInt(packet_type, in);

	std::cerr << "Packet: " << packet_type << std::endl;

	switch (packet_type)
	{
		case Protocol::Client::Query:
			if (!state.empty())
				throw Exception("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			receiveQuery(in);
			return true;
			
		case Protocol::Client::Data:
			if (state.empty())
				throw Exception("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			return receiveData(in);
						
		default:
			throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
	}
}

void TCPHandler::receiveQuery(ReadBuffer & in)
{
	UInt64 stage = 0;
	UInt64 compression = 0;
	
	readIntBinary(state.query_id, in);

	readVarUInt(stage, in);
	state.stage = Protocol::QueryProcessingStage::Enum(stage);

	readVarUInt(compression, in);
	state.compression = Protocol::Compression::Enum(compression);

	readStringBinary(state.in_format, in);
	readStringBinary(state.out_format, in);
	
	readStringBinary(state.query, in);

	state.context = server.global_context;
	state.io = executeQuery(state.query, state.context);
}

bool TCPHandler::receiveData(ReadBuffer & in)
{
	if (!state.block_in)
	{
		state.chunked_in = new ChunkedReadBuffer(in, state.query_id);
		state.maybe_compressed_in = state.compression == Protocol::Compression::Enable
			? new CompressedReadBuffer(*state.chunked_in)
			: state.chunked_in;

		state.block_in = state.context.format_factory->getInput(
			state.out_format,
			*state.maybe_compressed_in,
			state.io.out_sample,
			state.context.settings.max_block_size,
			*state.context.data_type_factory);
	}
	
	/// Прочитать из сети один блок и засунуть его в state.io.out (данные для INSERT-а)
	Block block = state.block_in->read();
	if (block)
	{
		state.io.out->write(block);
		return true;
	}
	else
		return false;
}

bool TCPHandler::sendData(WriteBuffer & out, WriteBuffer & out_for_chunks)
{
	writeVarUInt(Protocol::Server::Data, out);
	out.next();

	if (!state.block_out)
	{
		state.chunked_out = new ChunkedWriteBuffer(out_for_chunks, state.query_id);
		state.maybe_compressed_out = state.compression == Protocol::Compression::Enable
			? new CompressedWriteBuffer(*state.chunked_out)
			: state.chunked_out;

		state.block_out = state.context.format_factory->getOutput(
			state.in_format,
			*state.maybe_compressed_out,
			state.io.in_sample);
	}

	/// Получить один блок результата выполнения запроса из state.io.in и записать его в сеть.
	Block block = state.io.in->read();
	if (block)
	{
		state.block_out->write(block);
		return true;
	}
	else
	{
		dynamic_cast<ChunkedWriteBuffer &>(*state.chunked_out).finish();
		return false;
	}
}

void TCPHandler::sendException(WriteBuffer & out)
{
	/// TODO
}

void TCPHandler::sendProgress(WriteBuffer & out)
{
	/// TODO
}


void TCPHandler::run()
{
	try
	{
		runImpl();

		LOG_INFO(log, "Done processing connection.");
	}
	catch (Poco::Exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.message() = " << e.message() << ", e.what() = " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (std::exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (...)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		LOG_ERROR(log, s.str());
	}
}


}
