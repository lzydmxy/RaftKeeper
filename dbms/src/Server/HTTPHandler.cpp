#include <iomanip>

#include <Poco/Net/HTTPBasicCredentials.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Interpreters/executeQuery.h>

#include <DB/Common/ExternalTable.h>

#include "HTTPHandler.h"



namespace DB
{

void HTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	LOG_TRACE(log, "Request URI: " << request.getURI());

	HTMLForm params(request);
	std::istream & istr = request.stream();
	bool readonly = request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET;

	BlockInputStreamPtr query_plan;

	/** Часть запроса может быть передана в параметре query, а часть - POST-ом
	  *  (точнее - в теле запроса, а метод не обязательно должен быть POST).
	  * В таком случае, считается, что запрос - параметр query, затем перевод строки, а затем - данные POST-а.
	  */
	std::string query_param = params.get("query", "");
	if (!query_param.empty())
		query_param += '\n';
	
	/// Если указано compress, то будем сжимать результат.
	SharedPtr<WriteBufferFromHTTPServerResponse> out = new WriteBufferFromHTTPServerResponse(response);
	SharedPtr<WriteBuffer> out_maybe_compressed;

	if (parse<bool>(params.get("compress", "0")))
		out_maybe_compressed = new CompressedWriteBuffer(*out);
	else
		out_maybe_compressed = out;

	/// Имя пользователя и пароль могут быть заданы как в параметрах URL, так и с помощью HTTP Basic authentification (и то, и другое не секъюрно).
	std::string user = params.get("user", "default");
	std::string password = params.get("password", "");

	if (request.hasCredentials())
	{
		Poco::Net::HTTPBasicCredentials credentials(request);

		user = credentials.getUsername();
		password = credentials.getPassword();
	}

	std::string quota_key = params.get("quota_key", "");
	std::string query_id = params.get("query_id", "");
	
	Context context = *server.global_context;
	context.setGlobalContext(*server.global_context);

	context.setUser(user, password, request.clientAddress().host(), quota_key);
	context.setCurrentQueryId(query_id);

	SharedPtr<ReadBuffer> in_param = new ReadBufferFromString(query_param);
	SharedPtr<ReadBuffer> in_post = new ReadBufferFromIStream(istr);
	SharedPtr<ReadBuffer> in_post_maybe_compressed;

	/// Если указано decompress, то будем разжимать то, что передано POST-ом.
	if (parse<bool>(params.get("decompress", "0")))
		in_post_maybe_compressed = new CompressedReadBuffer(*in_post);
	else
		in_post_maybe_compressed = in_post;

	SharedPtr<ReadBuffer> in;

	if (0 == strncmp(request.getContentType().data(), "multipart/form-data", strlen("multipart/form-data")))
	{
		in = in_param;
		ExternalTablesHandler handler(context, params);

		params.load(request, istr, handler);

		/// Удаляем уже нененужные параметры из хранилища, чтобы впоследствии не перепутать их с натройками контекста и параметрами запроса.
		for (const auto & it : handler.names)
		{
			params.erase(it + "_format");
			params.erase(it + "_types");
			params.erase(it + "_structure");
		}
	}
	else
		in = new ConcatReadBuffer(*in_param, *in_post_maybe_compressed);

	/// Настройки могут быть переопределены в запросе.
	for (Poco::Net::NameValueCollection::ConstIterator it = params.begin(); it != params.end(); ++it)
	{
		if (it->first == "database")
		{
			context.setCurrentDatabase(it->second);
		}
		else if (it->first == "default_format")
		{
			context.setDefaultFormat(it->second);
		}
		else if (readonly && it->first == "readonly")
		{
			throw Exception("Setting 'readonly' cannot be overrided in readonly mode", ErrorCodes::READONLY);
		}
		else if (it->first == "query"
			|| it->first == "compress"
			|| it->first == "decompress"
			|| it->first == "user"
			|| it->first == "password"
			|| it->first == "quota_key"
			|| it->first == "query_id")
		{
		}
		else	/// Все неизвестные параметры запроса рассматриваются, как настройки.
			context.setSetting(it->first, it->second);
	}

	if (readonly)
		context.getSettingsRef().limits.readonly = true;

	Stopwatch watch;
	executeQuery(*in, *out_maybe_compressed, context, query_plan);
	watch.stop();

	if (query_plan)
	{
		std::stringstream log_str;
		log_str << "Query pipeline:\n";
		query_plan->dumpTree(log_str);
		LOG_DEBUG(log, log_str.str());

		/// Выведем информацию о том, сколько считано строк и байт.
		size_t rows = 0;
		size_t bytes = 0;

		query_plan->getLeafRowsBytes(rows, bytes);

		if (rows != 0)
		{
			LOG_INFO(log, std::fixed << std::setprecision(3)
				<< "Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
				<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
		}
	}

	QuotaForIntervals & quota = context.getQuota();
	if (!quota.empty())
		LOG_INFO(log, "Quota:\n" << quota.toString());

	/// Если не было эксепшена и данные ещё не отправлены - отправляются HTTP заголовки с кодом 200.
	out->finalize();
}


void HTTPHandler::trySendExceptionToClient(std::stringstream & s, Poco::Net::HTTPServerResponse & response)
{
	try
	{
		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
		if (!response.sent())
			response.send() << s.str() << std::endl;
	}
	catch (...)
	{
		LOG_ERROR(log, "Cannot send exception to client");
	}
}


void HTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	try
	{
		bool is_browser = false;
		if (request.has("Accept"))
		{
			String accept = request.get("Accept");
			if (0 == strncmp(accept.c_str(), "text/html", strlen("text/html")))
				is_browser = true;
		}

		if (is_browser)
			response.setContentType("text/plain; charset=UTF-8");

		/// Для того, чтобы работал keep-alive.
		if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
			response.setChunkedTransferEncoding(true);

		processQuery(request, response);
		LOG_INFO(log, "Done processing query");
	}
	catch (Exception & e)
	{
		std::stringstream s;
		s << "Code: " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
		LOG_ERROR(log, s.str());
		trySendExceptionToClient(s, response);
	}
	catch (Poco::Exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
		trySendExceptionToClient(s, response);
	}
	catch (std::exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		trySendExceptionToClient(s, response);
	}
	catch (...)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		trySendExceptionToClient(s, response);
	}
}


}
