#ifndef MYSQLXX_TRANSACTION_H
#define MYSQLXX_TRANSACTION_H

#include <boost/noncopyable.hpp>

#include <mysqlxx/Connection.h>


namespace mysqlxx
{

class Transaction : private boost::noncopyable
{
public:
	Transaction(Connection & conn_)
		: conn(conn_), finished(false)
	{
		conn.query("START TRANSACTION").execute();
	}

	virtual ~Transaction()
	{
		if (!finished)
			rollback();
	}

	void commit()
	{
		conn.query("COMMIT").execute();
		finished = true;
	}

	void rollback()
	{
		conn.query("ROLLBACK").execute();
		finished = true;
	}

private:
	Connection & conn;
	bool finished;
};


}

#endif
