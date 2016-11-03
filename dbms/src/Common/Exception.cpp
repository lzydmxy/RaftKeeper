#include <errno.h>
#include <string.h>
#include <cxxabi.h>

#include <common/logger_useful.h>

#include <DB/IO/WriteHelpers.h>

#include <DB/Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
	extern const int CANNOT_TRUNCATE_FILE;
}


void throwFromErrno(const std::string & s, int code, int e)
{
	const size_t buf_size = 128;
	char buf[buf_size];
#ifndef _GNU_SOURCE
	const char * unknown_message = "Unknown error ";
	int rc = strerror_r(e, buf, buf_size);
#ifdef __APPLE__
	if (rc != 0 && rc != EINVAL)
#else
	if (rc != 0)
#endif
	{
		std::string tmp = std::to_string(code);
		const char* code = tmp.c_str();
		strcpy(buf, unknown_message);
		strcpy(buf + strlen(unknown_message), code);
	}
	throw ErrnoException(s + ", errno: " + toString(e) + ", strerror: " + std::string(buf), code, e);
#else
	throw ErrnoException(s + ", errno: " + toString(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf))), code, e);
#endif
}


inline std::string demangle(const char * const mangled, int & status)
{
	const auto demangled_str = abi::__cxa_demangle(mangled, 0, 0, &status);
	std::string demangled{demangled_str};
	free(demangled_str);

	return demangled;
}

void tryLogCurrentException(const char * log_name, const std::string & start_of_message)
{
	tryLogCurrentException(&Logger::get(log_name), start_of_message);
}

void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message)
{
	try
	{
		LOG_ERROR(logger, start_of_message << (start_of_message.empty() ? "" : ": ") << getCurrentExceptionMessage(true));
	}
	catch (...)
	{
	}
}

std::string getCurrentExceptionMessage(bool with_stacktrace)
{
	std::stringstream stream;

	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		try
		{
			stream << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();

			if (with_stacktrace)
				stream << ", Stack trace:\n\n" << e.getStackTrace().toString();
		}
		catch (...) {}
	}
	catch (const Poco::Exception & e)
	{
		try
		{
			stream << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
				<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
		}
		catch (...) {}
	}
	catch (const std::exception & e)
	{
		try
		{
			int status = 0;
			auto name = demangle(typeid(e).name(), status);

			if (status)
				name += " (demangling status: " + toString(status) + ")";

			stream << "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what();
		}
		catch (...) {}
	}
	catch (...)
	{
		try
		{
			int status = 0;
			auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

			if (status)
				name += " (demangling status: " + toString(status) + ")";

			stream << "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name;
		}
		catch (...) {}
	}

	return stream.str();
}


std::unique_ptr<Poco::Exception> convertCurrentException()
{
	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		return std::unique_ptr<Poco::Exception>{ e.clone() };
	}
	catch (const Poco::Exception & e)
	{
		return std::unique_ptr<Poco::Exception>{ e.clone() };
	}
	catch (const std::exception & e)
	{
		return std::make_unique<Exception>(e.what(), ErrorCodes::STD_EXCEPTION);
	}
	catch (...)
	{
		return std::make_unique<Exception>("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
	}
}


void rethrowFirstException(const Exceptions & exceptions)
{
	for (size_t i = 0, size = exceptions.size(); i < size; ++i)
		if (exceptions[i])
			std::rethrow_exception(exceptions[i]);
}


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message)
{
	try
	{
		std::rethrow_exception(e);
	}
	catch (...)
	{
		tryLogCurrentException(log_name, start_of_message);
	}
}

void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message)
{
	try
	{
		std::rethrow_exception(e);
	}
	catch (...)
	{
		tryLogCurrentException(logger, start_of_message);
	}
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace)
{
	try
	{
		std::rethrow_exception(e);
	}
	catch (...)
	{
		return getCurrentExceptionMessage(with_stacktrace);
	}
}


}
