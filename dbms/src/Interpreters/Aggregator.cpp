#include <openssl/md5.h>

#include <iomanip>

#include <statdaemons/Stopwatch.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


AggregatedDataVariants::~AggregatedDataVariants()
{
	if (type == HASHED)
	{
		/// Уничтожаем ключи из keys_pool.
		for (AggregatedDataHashed::iterator it = hashed.begin(); it != hashed.end(); ++it)
			if (it->second.first != NULL)	/// Они могли быть перенесены в другой AggregatedDataVariants, с занулением указателя.
				for (size_t i = 0; i < keys_size; ++i)
					it->second.first[i].~Field();
	}

	if (aggregator)
		aggregator->destroyAggregateStates(*this);
}


void Aggregator::initialize(Block & block)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	if (initialized)
		return;

	initialized = true;

	aggregate_functions.resize(aggregates_size);
	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_functions[i] = &*aggregates[i].function;

	/// Инициализируем размеры состояний и смещения для агрегатных функций.
	offsets_of_aggregate_states.resize(aggregates_size);
	total_size_of_aggregate_states = 0;

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
		total_size_of_aggregate_states += aggregates[i].function->sizeOfData();
	}

	/** Всё остальное - только если передан непустой block.
	  * (всё остальное не нужно в методе merge блоков с готовыми состояниями агрегатных функций).
	  */
	if (!block)
		return;
	
	/// Преобразуем имена столбцов в номера, если номера не заданы
	if (keys.empty() && !key_names.empty())
		for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
			keys.push_back(block.getPositionByName(*it));

	for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
		if (it->arguments.empty() && !it->argument_names.empty())
			for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
				it->arguments.push_back(block.getPositionByName(*jt));

	/// Создадим пример блока, описывающего результат
	if (!sample)
	{
		for (size_t i = 0; i < keys_size; ++i)
		{
			sample.insert(block.getByPosition(keys[i]).cloneEmpty());
			if (sample.getByPosition(i).column->isConst())
				sample.getByPosition(i).column = dynamic_cast<IColumnConst &>(*sample.getByPosition(i).column).convertToFullColumn();
		}

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			ColumnWithNameAndType col;
			col.name = aggregates[i].column_name;

			size_t arguments_size = aggregates[i].arguments.size();
			DataTypes argument_types(arguments_size);
			for (size_t j = 0; j < arguments_size; ++j)
				argument_types[j] = block.getByPosition(aggregates[i].arguments[j]).type;
			
			col.type = new DataTypeAggregateFunction(aggregates[i].function, argument_types);
			col.column = new ColumnAggregateFunction(aggregates[i].function);

			sample.insert(col);
		}
	}	
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isNumeric())
		{
			keys_fit_128_bits = false;
			break;
		}
		key_sizes[j] = key_columns[j]->sizeOfField();
		keys_bytes += key_sizes[j];
	}
	if (keys_bytes > 16)
		keys_fit_128_bits = false;

	/// Если ключей нет
	if (keys_size == 0)
		return AggregatedDataVariants::WITHOUT_KEY;

	/// Если есть один ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
		return AggregatedDataVariants::KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<const ColumnString *>(key_columns[0]) || dynamic_cast<const ColumnFixedString *>(key_columns[0])
			|| dynamic_cast<const ColumnConstString *>(key_columns[0])))
		return AggregatedDataVariants::KEY_STRING;

	/// Если много ключей - будем агрегировать по хэшу от них
	return AggregatedDataVariants::HASHED;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	Row key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	typedef std::vector<ConstColumnPlainPtrs> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	LOG_TRACE(log, "Aggregating");

	Stopwatch watch;

	size_t src_rows = 0;
	size_t src_bytes = 0;

	bool keys_fit_128_bits = false;
	Sizes key_sizes;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		initialize(block);

		/// result будет уничтожать состояния агрегатных функций в деструкторе
		result.aggregator = this;
		
		src_rows += block.rows();
		src_bytes += block.bytes();

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_columns[i].resize(aggregates[i].arguments.size());
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(keys[i]).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
				aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		if (result.empty())
		{
			result.type = chooseAggregationMethod(key_columns, keys_fit_128_bits, key_sizes);
			result.keys_size = keys_size;
			LOG_TRACE(log, "Aggregation method: " << result.getMethodName() << ", keys_fit_128_bits: " << (keys_fit_128_bits ? "true" : "false"));
		}

		if (result.type == AggregatedDataVariants::WITHOUT_KEY || with_totals)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (!res)
			{
				res = result.aggregates_pool->alloc(total_size_of_aggregate_states);

				for (size_t i = 0; i < aggregates_size; ++i)
					aggregates[i].function->create(res + offsets_of_aggregate_states[i]);
			}

			/// Оптимизация в случае единственной агрегатной функции count.
			AggregateFunctionCount * agg_count = dynamic_cast<AggregateFunctionCount *>(aggregate_functions[0]);
			if (aggregates_size == 1 && agg_count)
				agg_count->addDelta(res, rows);
			else
			{
				for (size_t i = 0; i < rows; ++i)
				{
					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->add(res + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
				}
			}
		}
		
		if (result.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = result.key64;
			const IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				UInt64 key = get<UInt64>(column[i]);

				AggregatedDataWithUInt64Key::iterator it;
				bool inserted;

				if (!no_more_keys)
					res.emplace(key, it, inserted);
				else
				{
					inserted = false;
					it = res.find(key);
					if (res.end() == it)
						continue;
				}
				
				if (inserted)
				{
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);
					
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = result.key_string;
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
	            const ColumnString::Chars_t & data = column_string->getChars();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;

					if (!no_more_keys)
						res.emplace(ref, it, inserted);
					else
					{
						inserted = false;
						it = res.find(ref);
						if (res.end() == it)
							continue;
					}

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

						for (size_t j = 0; j < aggregates_size; ++j)
							aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
			{
				size_t n = column_string->getN();
				const ColumnFixedString::Chars_t & data = column_string->getChars();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i * n], n);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;

					if (!no_more_keys)
						res.emplace(ref, it, inserted);
					else
					{
						inserted = false;
						it = res.find(ref);
						if (res.end() == it)
							continue;
					}

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

						for (size_t j = 0; j < aggregates_size; ++j)
							aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
				}
			}
			else
				throw Exception("Illegal type of column when aggregating by string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				AggregatedDataHashed::iterator it;
				bool inserted;
				UInt128 key128 = pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes);

				if (!no_more_keys)
					res.emplace(key128, it, inserted);
				else
				{
					inserted = false;
					it = res.find(key128);
					if (res.end() == it)
						continue;
				}

				if (inserted)
				{
					/// Выделяем место для ключей в пуле.
					char * keys_place = result.keys_pool.alloc(keys_size * sizeof(Field));
					it->second.first = reinterpret_cast<Field*>(keys_place);

					/// Деструктивно переносим туда ключи. Делается допущение о том, что Field является relocatable типом.
					memcpy(keys_place, reinterpret_cast<const char *>(&key[0]), keys_size * sizeof(Field));
					/// А также, что field, инициализированный нулями, содержит значение Null.
					memset(reinterpret_cast<char *>(&key[0]), 0, keys_size * sizeof(Field));
					
					it->second.second = result.aggregates_pool->alloc(total_size_of_aggregate_states);
					
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second.second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second.second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (result.type == AggregatedDataVariants::GENERIC)
		{
			/// Общий способ
			AggregatedData & res = result.generic;
			
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key_columns[j]->get(i, key[j]);

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					if (no_more_keys)
						continue;
					
					it = res.insert(AggregatedData::value_type(key, NULL)).first;
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->add(it->second + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
			}
		}
		else if (result.type != AggregatedDataVariants::WITHOUT_KEY)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		/// Проверка ограничений.
		if (!no_more_keys && max_rows_to_group_by && result.size() > max_rows_to_group_by)
		{
			if (group_by_overflow_mode == Limits::THROW)
				throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result.size())
					+ " rows, maximum: " + toString(max_rows_to_group_by),
					ErrorCodes::TOO_MUCH_ROWS);
			else if (group_by_overflow_mode == Limits::BREAK)
				break;
			else if (group_by_overflow_mode == Limits::ANY)
				no_more_keys = true;
			else
				throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
		}
	}

	double elapsed_seconds = watch.elapsedSeconds();
	size_t rows = result.size();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


Block Aggregator::convertToBlock(AggregatedDataVariants & data_variants)
{
	Block res = getSampleBlock();
	size_t rows = data_variants.size();

	LOG_TRACE(log, "Converting aggregated data to block");

	Stopwatch watch;

	/// В какой структуре данных агрегированы данные?
	if (data_variants.empty())
		return res;

	typedef std::vector<ColumnAggregateFunction::Container_t *> AggregateColumns;
	
	ColumnPlainPtrs key_columns(keys_size);
	AggregateColumns aggregate_columns(aggregates_size);

	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = res.getByPosition(i).column;
		key_columns[i]->reserve(rows);
	}

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
		ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*res.getByPosition(i + keys_size).column);

		for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
			column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

		aggregate_columns[i] = &column_aggregate_func.getData();
		aggregate_columns[i]->resize(rows);
	}

	if (data_variants.type == AggregatedDataVariants::WITHOUT_KEY || with_totals)
	{
		AggregatedDataWithoutKey & data = data_variants.without_key;

		for (size_t i = 0; i < aggregates_size; ++i)
			(*aggregate_columns[i])[0] = data + offsets_of_aggregate_states[i];

		/// Для тотальных агрегатов вместо ключей пишутся значения по-умолчанию (нули, пустые строки).
		if (with_totals)
			for (size_t i = 0; i < keys_size; ++i)
				key_columns[i]->insertDefault();
	}
	
	if (data_variants.type == AggregatedDataVariants::KEY_64)
	{
		AggregatedDataWithUInt64Key & data = data_variants.key64;

		IColumn & first_column = *key_columns[0];

		size_t j = with_totals ? 1 : 0;
		for (AggregatedDataWithUInt64Key::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
		{
			first_column.insertData(reinterpret_cast<const char *>(&it->first), sizeof(it->first));

			for (size_t i = 0; i < aggregates_size; ++i)
				(*aggregate_columns[i])[j] = it->second + offsets_of_aggregate_states[i];
		}
	}
	else if (data_variants.type == AggregatedDataVariants::KEY_STRING)
	{
		AggregatedDataWithStringKey & data = data_variants.key_string;
		IColumn & first_column = *key_columns[0];

		size_t j = with_totals ? 1 : 0;
		for (AggregatedDataWithStringKey::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
		{
			first_column.insertData(it->first.data, it->first.size);

			for (size_t i = 0; i < aggregates_size; ++i)
				(*aggregate_columns[i])[j] = it->second + offsets_of_aggregate_states[i];
		}
	}
	else if (data_variants.type == AggregatedDataVariants::HASHED)
	{
		AggregatedDataHashed & data = data_variants.hashed;

		size_t j = with_totals ? 1 : 0;
		for (AggregatedDataHashed::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
		{
			for (size_t i = 0; i < keys_size; ++i)
				key_columns[i]->insert(it->second.first[i]);

			for (size_t i = 0; i < aggregates_size; ++i)
				(*aggregate_columns[i])[j] = it->second.second + offsets_of_aggregate_states[i];
		}
	}
	else if (data_variants.type == AggregatedDataVariants::GENERIC)
	{
		AggregatedData & data = data_variants.generic;
		size_t j = with_totals ? 1 : 0;
		for (AggregatedData::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
		{
			for (size_t i = 0; i < keys_size; ++i)
				key_columns[i]->insert(it->first[i]);

			for (size_t i = 0; i < aggregates_size; ++i)
				(*aggregate_columns[i])[j] = it->second + offsets_of_aggregate_states[i];
		}
	}
	else if (data_variants.type != AggregatedDataVariants::WITHOUT_KEY)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе
	data_variants.aggregator = NULL;

	/// Изменяем размер столбцов-констант в блоке.
	size_t columns = res.columns();
	for (size_t i = 0; i < columns; ++i)
		if (res.getByPosition(i).column->isConst())
			res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

	double elapsed_seconds = watch.elapsedSeconds();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Converted aggregated data to block. "
		<< rows << " rows, " << res.bytes() / 1048576.0 << " MiB"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec., " << res.bytes() / elapsed_seconds / 1048576.0 << " MiB/sec.)");

	return res;
}


AggregatedDataVariantsPtr Aggregator::merge(ManyAggregatedDataVariants & data_variants)
{
	if (data_variants.empty())
 		throw Exception("Empty data passed to Aggregator::merge().", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	Stopwatch watch;

	AggregatedDataVariantsPtr res = data_variants[0];

	/// Все результаты агрегации соединяем с первым.
	size_t rows = res->size();
	for (size_t i = 1, size = data_variants.size(); i < size; ++i)
	{
		rows += data_variants[i]->size();
		AggregatedDataVariants & current = *data_variants[i];

		res->aggregates_pools.insert(res->aggregates_pools.end(), current.aggregates_pools.begin(), current.aggregates_pools.end());

		if (current.empty())
			continue;

		if (res->empty())
		{
			res = data_variants[i];
			continue;
		}

		if (res->type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		/// В какой структуре данных агрегированы данные?
		if (res->type == AggregatedDataVariants::WITHOUT_KEY || with_totals)
		{
			AggregatedDataWithoutKey & res_data = res->without_key;
			AggregatedDataWithoutKey & current_data = current.without_key;

			for (size_t i = 0; i < aggregates_size; ++i)
			{
				aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i]);
				aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);
			}
		}
		
		if (res->type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res_data = res->key64;
			AggregatedDataWithUInt64Key & current_data = current.key64;

			for (AggregatedDataWithUInt64Key::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataWithUInt64Key::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					for (size_t i = 0; i < aggregates_size; ++i)
					{
						aggregate_functions[i]->merge(res_it->second + offsets_of_aggregate_states[i], it->second + offsets_of_aggregate_states[i]);
						aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
					}
				}
				else
					res_it->second = it->second;
			}
		}
		else if (res->type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res_data = res->key_string;
			AggregatedDataWithStringKey & current_data = current.key_string;
			
			for (AggregatedDataWithStringKey::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataWithStringKey::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					for (size_t i = 0; i < aggregates_size; ++i)
					{
						aggregate_functions[i]->merge(res_it->second + offsets_of_aggregate_states[i], it->second + offsets_of_aggregate_states[i]);
						aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
					}
				}
				else
					res_it->second = it->second;
			}
		}
		else if (res->type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res_data = res->hashed;
			AggregatedDataHashed & current_data = current.hashed;

			for (AggregatedDataHashed::iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregatedDataHashed::iterator res_it;
				bool inserted;
				res_data.emplace(it->first, res_it, inserted);

				if (!inserted)
				{
					for (size_t i = 0; i < aggregates_size; ++i)
					{
						aggregate_functions[i]->merge(res_it->second.second + offsets_of_aggregate_states[i], it->second.second + offsets_of_aggregate_states[i]);
						aggregate_functions[i]->destroy(it->second.second + offsets_of_aggregate_states[i]);
					}
				}
				else
				{
					res_it->second = it->second;
					it->second.first = NULL;
				}
			}
		}
		else if (res->type == AggregatedDataVariants::GENERIC)
		{
			AggregatedData & res_data = res->generic;
			AggregatedData & current_data = current.generic;

			for (AggregatedData::const_iterator it = current_data.begin(); it != current_data.end(); ++it)
			{
				AggregateDataPtr & res_ptr = res_data[it->first];
				if (res_ptr != NULL)
				{
					for (size_t i = 0; i < aggregates_size; ++i)
					{
						aggregate_functions[i]->merge(res_ptr + offsets_of_aggregate_states[i], it->second + offsets_of_aggregate_states[i]);
						aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
					}
				}
				else
					res_ptr = it->second;
			}
		}
		else if (res->type != AggregatedDataVariants::WITHOUT_KEY)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		/// current не будет уничтожать состояния агрегатных функций в деструкторе
		current.aggregator = NULL;
	}

	double elapsed_seconds = watch.elapsedSeconds();
	size_t res_rows = res->size();
	
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Merged aggregated data. "
		<< "From " << rows << " to " << res_rows << " rows (efficiency: " << static_cast<double>(rows) / res_rows << ")"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec.)");

	return res;
}


void Aggregator::merge(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	Row key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	typedef ColumnAggregateFunction::Container_t * AggregateColumn;
	typedef std::vector<AggregateColumn> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

	Block empty_block;
	initialize(empty_block);

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		LOG_TRACE(log, "Merging aggregated block");
		
		if (!sample)
			for (size_t i = 0; i < keys_size + aggregates_size; ++i)
				sample.insert(block.getByPosition(i).cloneEmpty());
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i] = block.getByPosition(i).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_columns[i] = &dynamic_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		bool keys_fit_128_bits = false;
		Sizes key_sizes;
		result.type = chooseAggregationMethod(key_columns, keys_fit_128_bits, key_sizes);
		result.keys_size = keys_size;

		if (result.type == AggregatedDataVariants::WITHOUT_KEY || with_totals)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (!res)
			{
				res = result.aggregates_pool->alloc(total_size_of_aggregate_states);
				
				for (size_t i = 0; i < aggregates_size; ++i)
					aggregate_functions[i]->create(res + offsets_of_aggregate_states[i]);
			}

			/// Добавляем значения
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0]);
		}

		if (result.type == AggregatedDataVariants::KEY_64)
		{
			AggregatedDataWithUInt64Key & res = result.key64;
			const IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = with_totals ? 1 : 0; i < rows; ++i)
			{
				/// Строим ключ
				UInt64 key = get<UInt64>(column[i]);
				
				AggregatedDataWithUInt64Key::iterator it;
				bool inserted;
				res.emplace(key, it, inserted);

				if (inserted)
				{
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->merge(it->second + offsets_of_aggregate_states[j], (*aggregate_columns[j])[i]);
			}
		}
		else if (result.type == AggregatedDataVariants::KEY_STRING)
		{
			AggregatedDataWithStringKey & res = result.key_string;
			const IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
            {
                const ColumnString::Offsets_t & offsets = column_string->getOffsets();
                const ColumnString::Chars_t & data = column_string->getChars();

				/// Для всех строчек
				for (size_t i = with_totals ? 1 : 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

						for (size_t j = 0; j < aggregates_size; ++j)
							aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->merge(it->second + offsets_of_aggregate_states[j], (*aggregate_columns[j])[i]);
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
            {
                size_t n = column_string->getN();
                const ColumnFixedString::Chars_t & data = column_string->getChars();

				/// Для всех строчек
				for (size_t i = with_totals ? 1 : 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i * n], n);

					AggregatedDataWithStringKey::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
					{
						it->first.data = result.string_pool.insert(ref.data, ref.size);
						it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

						for (size_t j = 0; j < aggregates_size; ++j)
							aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
					}

					/// Добавляем значения
					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->merge(it->second + offsets_of_aggregate_states[j], (*aggregate_columns[j])[i]);
				}
			}
			else
				throw Exception("Illegal type of column when aggregating by string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (result.type == AggregatedDataVariants::HASHED)
		{
			AggregatedDataHashed & res = result.hashed;

			/// Для всех строчек
			for (size_t i = with_totals ? 1 : 0; i < rows; ++i)
			{
				AggregatedDataHashed::iterator it;
				bool inserted;
				res.emplace(pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes), it, inserted);

				if (inserted)
				{
					/// Выделяем место для ключей в пуле.
					char * keys_place = result.keys_pool.alloc(keys_size * sizeof(Field));
					it->second.first = reinterpret_cast<Field*>(keys_place);

					/// Деструктивно переносим туда ключи. Делается допущение о том, что Field является relocatable типом.
					memcpy(keys_place, reinterpret_cast<const char *>(&key[0]), keys_size * sizeof(Field));
					/// А также, что field, инициализированный нулями, содержит значение Null.
					memset(reinterpret_cast<char *>(&key[0]), 0, keys_size * sizeof(Field));
					
					it->second.second = result.aggregates_pool->alloc(total_size_of_aggregate_states);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second.second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->merge(it->second.second + offsets_of_aggregate_states[j], (*aggregate_columns[j])[i]);
			}
		}
		else if (result.type == AggregatedDataVariants::GENERIC)
		{
			/// Общий способ
			AggregatedData & res = result.generic;

			/// Для всех строчек
			for (size_t i = with_totals ? 1 : 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key_columns[j]->get(i, key[j]);

				AggregatedData::iterator it = res.find(key);
				if (it == res.end())
				{
					it = res.insert(AggregatedData::value_type(key, NULL)).first;
					it->second = result.aggregates_pool->alloc(total_size_of_aggregate_states);
					key.resize(keys_size);

					for (size_t j = 0; j < aggregates_size; ++j)
						aggregate_functions[j]->create(it->second + offsets_of_aggregate_states[j]);
				}

				/// Добавляем значения
				for (size_t j = 0; j < aggregates_size; ++j)
					aggregate_functions[j]->merge(it->second + offsets_of_aggregate_states[j], (*aggregate_columns[j])[i]);
			}
		}
		else if (result.type != AggregatedDataVariants::WITHOUT_KEY)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		LOG_TRACE(log, "Merged aggregated block");
	}
}


void Aggregator::destroyAggregateStates(AggregatedDataVariants & result)
{
	if (result.size() == 0)
		return;

	LOG_TRACE(log, "Destroying aggregate states because query execution was cancelled");

	/// В какой структуре данных агрегированы данные?
	if (result.type == AggregatedDataVariants::WITHOUT_KEY || with_totals)
	{
		AggregatedDataWithoutKey & res_data = result.without_key;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);
	}
	if (result.type == AggregatedDataVariants::KEY_64)
	{
		AggregatedDataWithUInt64Key & res_data = result.key64;

		for (AggregatedDataWithUInt64Key::const_iterator it = res_data.begin(); it != res_data.end(); ++it)
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
	}
	else if (result.type == AggregatedDataVariants::KEY_STRING)
	{
		AggregatedDataWithStringKey & res_data = result.key_string;

		for (AggregatedDataWithStringKey::const_iterator it = res_data.begin(); it != res_data.end(); ++it)
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
	}
	else if (result.type == AggregatedDataVariants::HASHED)
	{
		AggregatedDataHashed & res_data = result.hashed;

		for (AggregatedDataHashed::iterator it = res_data.begin(); it != res_data.end(); ++it)
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(it->second.second + offsets_of_aggregate_states[i]);
	}
	else if (result.type == AggregatedDataVariants::GENERIC)
	{
		AggregatedData & res_data = result.generic;

		for (AggregatedData::const_iterator it = res_data.begin(); it != res_data.end(); ++it)
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(it->second + offsets_of_aggregate_states[i]);
	}
}


String Aggregator::getID() const
{
	std::stringstream res;
	
	if (keys.empty())
	{
		res << "key_names";
		for (size_t i = 0; i < key_names.size(); ++i)
			res << ", " << key_names[i];
	}
	else
	{
		res << "keys";
		for (size_t i = 0; i < keys.size(); ++i)
			res << ", " << keys[i];
	}

	res << ", aggregates";
	for (size_t i = 0; i < aggregates.size(); ++i)
		res << ", " << aggregates[i].column_name;

	return res.str();
}

}
