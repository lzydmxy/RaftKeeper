#include <iomanip>

#include <Yandex/logger_useful.h>

#include <DB/Common/SipHash.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Interpreters/Quota.h>


namespace DB
{

void QuotaValues::initFromConfig(const String & config_elem)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

	queries 		= parse<UInt64>(config.getString(config_elem + ".queries", 		"0"));
	errors 			= parse<UInt64>(config.getString(config_elem + ".errors", 		"0"));
	result_rows 	= parse<UInt64>(config.getString(config_elem + ".result_rows",	"0"));
	result_bytes 	= parse<UInt64>(config.getString(config_elem + ".result_bytes",	"0"));
	read_rows 		= parse<UInt64>(config.getString(config_elem + ".read_rows", 	"0"));
	read_bytes 		= parse<UInt64>(config.getString(config_elem + ".read_bytes", 	"0"));
	execution_time = Poco::Timespan(config.getInt(config_elem + ".execution_time", 0), 0);
}


void QuotaForInterval::initFromConfig(const String & config_elem, time_t duration_)
{
	rounded_time = 0;
	duration = duration_;
	max.initFromConfig(config_elem);
}

void QuotaForInterval::checkExceeded(time_t current_time, const String & quota_name)
{
	updateTime(current_time);
	check(max.queries, used.queries, current_time, quota_name, "Queries");
	check(max.errors, used.errors, current_time, quota_name, "Errors");
	check(max.result_rows, used.result_rows, current_time, quota_name, "Total result rows");
	check(max.result_bytes, used.result_bytes, current_time, quota_name, "Total result bytes");
	check(max.read_rows, used.read_rows, current_time, quota_name, "Total rows read");
	check(max.read_bytes, used.read_bytes, current_time, quota_name, "Total bytes read");
	check(max.execution_time.totalSeconds(), used.execution_time.totalSeconds(), current_time, quota_name, "Total execution time");
}

String QuotaForInterval::toString() const
{
	std::stringstream res;

	res << std::fixed << std::setprecision(3)
		<< "Interval:       " << mysqlxx::DateTime(rounded_time) << " - " << mysqlxx::DateTime(rounded_time + duration) << ".\n"
		<< "Queries:        " << used.queries 		<< ".\n"
		<< "Errors:         " << used.errors 		<< ".\n"
		<< "Result rows:    " << used.result_rows 	<< ".\n"
		<< "Result bytes:   " << used.result_bytes 	<< ".\n"
		<< "Read rows:      " << used.read_rows 	<< ".\n"
		<< "Read bytes:     " << used.read_bytes 	<< ".\n"
		<< "Execution time: " << used.execution_time.totalMilliseconds() / 1000.0 << " sec.\n";

	return res.str();
}

void QuotaForInterval::addQuery(time_t current_time, const String & quota_name)
{
	__sync_fetch_and_add(&used.queries, 1);
}

void QuotaForInterval::addError(time_t current_time, const String & quota_name)
{
	__sync_fetch_and_add(&used.errors, 1);
}

void QuotaForInterval::checkAndAddResultRowsBytes(time_t current_time, const String & quota_name, size_t rows, size_t bytes)
{
	checkExceeded(current_time, quota_name);
	__sync_fetch_and_add(&used.result_rows, rows);
	__sync_fetch_and_add(&used.result_bytes, bytes);
}

void QuotaForInterval::checkAndAddReadRowsBytes(time_t current_time, const String & quota_name, size_t rows, size_t bytes)
{
	checkExceeded(current_time, quota_name);
	__sync_fetch_and_add(&used.read_rows, rows);
	__sync_fetch_and_add(&used.read_bytes, bytes);
}

void QuotaForInterval::checkAndAddExecutionTime(time_t current_time, const String & quota_name, Poco::Timespan amount)
{
	checkExceeded(current_time, quota_name);
	/// Используется информация о внутреннем представлении Poco::Timespan.
	__sync_fetch_and_add(reinterpret_cast<Int64 *>(&used.execution_time), amount.totalMicroseconds());
}

void QuotaForInterval::updateTime(time_t current_time)
{
	if (current_time >= rounded_time + static_cast<int>(duration))
	{
		rounded_time = current_time / duration * duration;
		used.clear();
	}
}

void QuotaForInterval::check(size_t max_amount, size_t used_amount, time_t current_time, const String & quota_name, const char * resource_name)
{
	if (max_amount && used_amount >= max_amount)
	{
		std::stringstream message;
		message << "Quota '" << quota_name << "' for ";

		if (duration == 3600)
			message << "1 hour";
		else if (duration == 60)
			message << "1 minute";
		else if (duration % 3600 == 0)
			message << (duration / 3600) << " hours";
		else if (duration % 60 == 0)
			message << (duration / 60) << " minutes";
		else
			message << duration << " seconds";

		message << " has been expired. "
			<< resource_name << ": " << used_amount << ", max: " << max_amount << ". "
			<< "Interval will end at " << mysqlxx::DateTime(rounded_time + duration) << ".";

		throw Exception(message.str(), ErrorCodes::QUOTA_EXPIRED);
	}
}


void QuotaForIntervals::initFromConfig(const String & config_elem)
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(config_elem, config_keys);

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		if (0 != it->compare(0, strlen("interval"), "interval"))
			continue;

		String interval_config_elem = config_elem + "." + *it;
		time_t duration = config.getInt(interval_config_elem + ".duration");

		cont[duration].initFromConfig(interval_config_elem, duration);
	}
}

void QuotaForIntervals::checkExceeded(time_t current_time)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkExceeded(current_time, parent->name);
}

void QuotaForIntervals::addQuery(time_t current_time)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.addQuery(current_time, parent->name);
}

void QuotaForIntervals::addError(time_t current_time)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.addError(current_time, parent->name);
}

void QuotaForIntervals::checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddResultRowsBytes(current_time, parent->name, rows, bytes);
}

void QuotaForIntervals::checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddReadRowsBytes(current_time, parent->name, rows, bytes);
}

void QuotaForIntervals::checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddExecutionTime(current_time, parent->name, amount);
}

String QuotaForIntervals::toString() const
{
	std::stringstream res;

	for (Container::const_reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		res << std::endl << it->second.toString();

	return res.str();
}


void Quota::initFromConfig(const String & config_elem, const String & name_)
{
	name = name_;

	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

	keyed_by_ip = config.has(config_elem + ".keyed_by_ip");
	is_keyed = keyed_by_ip || config.has(config_elem + ".keyed");

	max.initFromConfig(config_elem);
}

QuotaForIntervals & Quota::get(const String & quota_key, const Poco::Net::IPAddress & ip)
{
	if (!quota_key.empty() && (!is_keyed || keyed_by_ip))
		throw Exception("Quota " + name + " doesn't allow client supplied keys.", ErrorCodes::QUOTA_DOESNT_ALLOW_KEYS);

	String quota_key_or_ip = keyed_by_ip ? ip.toString() : quota_key;
	UInt64 quota_key_hashed = 0;

	if (!quota_key_or_ip.empty())
		quota_key_hashed = sipHash64(quota_key_or_ip);

	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	Container::iterator it = quota_for_keys.find(quota_key_hashed);
	if (quota_for_keys.end() == it)
	{
		it = quota_for_keys.insert(std::make_pair(quota_key_hashed, QuotaForIntervals(this))).first;
		it->second = max;
	}

	return it->second;
}


void Quotas::initFromConfig()
{
	Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys("quotas", config_keys);

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		cont[*it] = new Quota();
		cont[*it]->initFromConfig("quotas." + *it, *it);
	}
}

QuotaForIntervals & Quotas::get(const String & name, const String & quota_key, const Poco::Net::IPAddress & ip)
{
	Container::iterator it = cont.find(name);
	if (cont.end() == it)
		throw Exception("Unknown quota " + name, ErrorCodes::UNKNOWN_QUOTA);

	return it->second->get(quota_key, ip);
}

}
