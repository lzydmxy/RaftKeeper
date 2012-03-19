#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Интерпретирует запрос INSERT.
  */
class InterpreterInsertQuery
{
public:
	InterpreterInsertQuery(ASTPtr query_ptr_, Context & context_);

	/** Выполнить запрос.
	  * remaining_data_istr, если не NULL, может содержать нераспарсенные данные для вставки.
	  * (заранее может быть считан в оперативку для парсинга лишь небольшой кусок запроса, который содержит не все данные)
	  */
	void execute(ReadBuffer * remaining_data_istr);

	/** Подготовить запрос к выполнению. Вернуть поток блоков, в который можно писать данные для выполнения запроса.
	  * Или вернуть NULL, если запрос INSERT SELECT (самодостаточный запрос - не принимает входные данные).
	  */
	BlockOutputStreamPtr execute();

	Block getSampleBlock();

private:
	StoragePtr getTable();

	ASTPtr query_ptr;
	Context context;
};


}
