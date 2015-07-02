#pragma once

#include <statdaemons/PoolBase.h>

#include <DB/Client/Connection.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс для пулов соединений.
  *
  * Использование (на примере обычного ConnectionPool):
  * ConnectionPool pool(...);
  *
  *	void thread()
  *	{
  *	  	sqxxl::Pool::Entry connection = pool.get();
  *		connection->sendQuery("SELECT 'Hello, world!' AS world");
  *	}
  */
class IConnectionPool : private boost::noncopyable
{
public:
	typedef PoolBase<Connection>::Entry Entry;
	virtual Entry get(const Settings * settings = nullptr) = 0;

	/** Выделяет до указанного количества соединений для работы.
	  * Соединения предоставляют доступ к разным репликам одного шарда.
	  * Выкидывает исключение, если не удалось выделить ни одного соединения.
	  */
	virtual std::vector<Entry> getMany(const Settings * settings = nullptr)
	{
		return std::vector<Entry>{ get(settings) };
	}

	virtual ~IConnectionPool() {}
};

typedef SharedPtr<IConnectionPool> ConnectionPoolPtr;
typedef std::vector<ConnectionPoolPtr> ConnectionPools;



/** Обычный пул соединений, без отказоустойчивости.
  */
class ConnectionPool : public PoolBase<Connection>, public IConnectionPool
{
public:
	typedef IConnectionPool::Entry Entry;
	typedef PoolBase<Connection> Base;

	ConnectionPool(unsigned max_connections_,
			const String & host_, UInt16 port_,
			const String & default_database_,
			const String & user_, const String & password_,
			const String & client_name_ = "client",
			Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
			Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
			Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
			Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
	   : Base(max_connections_, &Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
		host(host_), port(port_), default_database(default_database_),
		user(user_), password(password_), resolved_address(host_, port_),
		client_name(client_name_), compression(compression_),
		connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_)
	{
	}

	ConnectionPool(unsigned max_connections_,
			const String & host_, UInt16 port_, const Poco::Net::SocketAddress & resolved_address_,
			const String & default_database_,
			const String & user_, const String & password_,
			const String & client_name_ = "client",
			Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
			Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
			Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
			Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0))
		: Base(max_connections_, &Logger::get("ConnectionPool (" + host_ + ":" + toString(port_) + ")")),
		host(host_), port(port_), default_database(default_database_),
		user(user_), password(password_), resolved_address(resolved_address_),
		client_name(client_name_), compression(compression_),
		connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_)
	{
	}


	/** Выделяет соединение для работы. */
	Entry get(const Settings * settings = nullptr) override
	{
		if (settings)
			return Base::get(settings->queue_max_wait_ms.totalMilliseconds());
		else
			return Base::get(-1);
	}

	const std::string & getHost() const
	{
		return host;
	}

protected:
	/** Создает новый объект для помещения в пул. */
	ConnectionPtr allocObject() override
	{
		return new Connection(
			host, port, resolved_address,
			default_database, user, password,
			client_name, compression,
			connect_timeout, receive_timeout, send_timeout);
	}

private:
	String host;
	UInt16 port;
	String default_database;
	String user;
	String password;

	/** Адрес может быть заранее отрезолвен и передан в конструктор. Тогда поля host и port имеют смысл только для логгирования.
	  * Иначе адрес резолвится в конструкторе. То есть, DNS балансировка не поддерживается.
	  */
	Poco::Net::SocketAddress resolved_address;

	String client_name;
	Protocol::Compression::Enum compression;		/// Сжимать ли данные при взаимодействии с сервером.

	Poco::Timespan connect_timeout;
	Poco::Timespan receive_timeout;
	Poco::Timespan send_timeout;
};

}
