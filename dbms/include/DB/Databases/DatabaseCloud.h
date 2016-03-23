#pragma once

#include <threadpool.hpp>
#include <DB/Databases/IDatabase.h>


namespace DB
{

/** Позволяет создавать "облачные таблицы".
  * Список таких таблиц хранится в ZooKeeper.
  * Все серверы, ссылающиеся на один путь в ZooKeeper, видят один и тот же список таблиц.
  * CREATE, DROP, RENAME атомарны.
  *
  * Для БД задаётся уровень репликации N.
  * При записи в "облачную" таблицу, некоторым образом выбирается N живых серверов в разных датацентрах,
  *  на каждом из них создаётся локальная таблица, и в них производится запись.
  *
  * Движок имеет параметры: Cloud(zookeeper_path, replication_factor, datacenter_name)
  * Пример: Cloud('/clickhouse/clouds/production/', 3, 'FIN')
  *
  * Структура в ZooKeeper:
  *
  * cloud_path                   - путь к "облаку"; может существовать несколько разных независимых облаков
		/table_definitions       - множество уникальных определений таблиц, чтобы не писать их много раз для большого количества таблиц
			/hash128 -> sql      - отображение: хэш от определения таблицы (идентификатор) -> само определение таблицы в виде CREATE запроса
		/tables                  - список таблиц
			/database_name       - имя базы данных
				/name_hash_mod -> compressed_table_list
			                     - список таблиц сделан двухуровневым, чтобы уменьшить количество узлов в ZooKeeper при наличии большого количества таблиц
			                     - узлы создаются для каждого остатка от деления хэша от имени таблицы, например, на 4096
			                     - и в каждом узле хранится список таблиц (имя таблицы, хэш от структуры, список хостов) в сжатом виде
		/ordered_locality_keys   - ключи локальности
			/key-SEQNO -> key_value - порядковый номер ключа локальности (в порядке того, как они были встречены) -> значение ключа локальности
			                     - ключ локальности - произвольная строка
			                     - движок БД определяет серверы для расположения данных таким образом, чтобы, при одинаковом множестве живых серверов,
								   одному ключу локальности соответствовала одна группа из N серверов для расположения данных.
		/nodes                   - список серверов, на которых зарегистрированы облачные БД с таким путём в ZK
			/hostname            - имя хоста
				/alive           - эфемерная нода для предварительной проверки живости
				/datacenter      - имя датацентра
				/disk_space

  * К одному облаку может относиться несколько БД, названных по-разному. Например, БД hits и visits могут относиться к одному облаку.
  */
class DatabaseCloud : public IDatabase
{
private:
	const String name;
	const String data_path;
	String zookeeper_path;
	const size_t replication_factor;
	const String hostname;
	const String datacenter_name;

	Logger * log;

	Context & context;

public:
	DatabaseCloud(
		bool attach,
		const String & name_,
		const String & zookeeper_path_,
		size_t replication_factor_,
		const String & datacenter_name_,
		Context & context_,
		boost::threadpool::pool * thread_pool);

	String getEngineName() const override { return "Cloud"; }

	bool isTableExist(const String & table_name) const override;
	StoragePtr tryGetTable(const String & table_name) override;

	DatabaseIteratorPtr getIterator() override;

	bool empty() const override;

	void createTable(const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine) override;
	StoragePtr removeTable(const String & table_name) override;

	void attachTable(const String & table_name, const StoragePtr & table) override;
	StoragePtr detachTable(const String & table_name) override;

	void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name) override;

	ASTPtr getCreateQuery(const String & table_name) const override;

	void shutdown() override;

private:
	void createZookeeperNodes();
	String getNameOfNodeWithTables(const String & table_name);
};

}
