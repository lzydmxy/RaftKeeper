#include <iomanip>
#include <thread>
#include <future>

#include <statdaemons/Stopwatch.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


AggregatedDataVariants::~AggregatedDataVariants()
{
	if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
	{
		try
		{
			aggregator->destroyAllAggregateStates(*this);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
}


void AggregatedDataVariants::convertToTwoLevel()
{
	if (aggregator)
		LOG_TRACE(aggregator->log, "Converting aggregation data to two-level.");

	switch (type)
	{
	#define M(NAME) \
		case Type::NAME: \
			NAME ## _two_level.reset(new decltype(NAME ## _two_level)::element_type(*NAME)); \
			NAME.reset(); \
			type = Type::NAME ## _two_level; \
			break;

		APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

	#undef M

		default:
			throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
	}
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
	all_aggregates_has_trivial_destructor = true;

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
		total_size_of_aggregate_states += aggregates[i].function->sizeOfData();

		if (!aggregates[i].function->hasTrivialDestructor())
			all_aggregates_has_trivial_destructor = false;
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

			col.type = new DataTypeAggregateFunction(aggregates[i].function, argument_types, aggregates[i].parameters);
			col.column = col.type->createColumn();

			sample.insert(col);
		}
	}
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes)
{
	bool keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isFixed())
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
		return AggregatedDataVariants::Type::without_key;

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
	{
		size_t size_of_field = key_columns[0]->sizeOfField();
		if (size_of_field == 1)
			return AggregatedDataVariants::Type::key8;
		if (size_of_field == 2)
			return AggregatedDataVariants::Type::key16;
		if (size_of_field == 4)
			return AggregatedDataVariants::Type::key32;
		if (size_of_field == 8)
			return AggregatedDataVariants::Type::key64;
		throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
	}

	/// Если ключи помещаются в 128 бит, будем использовать хэш-таблицу по упакованным в 128-бит ключам
	if (keys_fit_128_bits)
		return AggregatedDataVariants::Type::keys128;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1 && typeid_cast<const ColumnString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_string;

	if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_fixed_string;

	/// Иначе будем агрегировать по хэшу от ключей.
	return AggregatedDataVariants::Type::hashed;
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
	for (size_t j = 0; j < aggregates_size; ++j)
	{
		try
		{
			/** Может возникнуть исключение при нехватке памяти.
			  * Для того, чтобы потом всё правильно уничтожилось, "откатываем" часть созданных состояний.
			  * Код не очень удобный.
			  */
			aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
		}
		catch (...)
		{
			for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
				aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

			aggregate_data = nullptr;
			throw;
		}
	}
}


/** Интересно - если убрать noinline, то gcc зачем-то инлайнит эту функцию, и производительность уменьшается (~10%).
  * (Возможно из-за того, что после инлайна этой функции, перестают инлайниться более внутренние функции.)
  * Инлайнить не имеет смысла, так как внутренний цикл находится целиком внутри этой функции.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
	Method & method,
	Arena * aggregates_pool,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumns & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	bool no_more_keys,
	AggregateDataPtr overflow_row) const
{
	method.init(key_columns);

	if (!no_more_keys)
		executeImplCase<false>(method, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
	else
		executeImplCase<true>(method, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

template <bool no_more_keys, typename Method>
void NO_INLINE Aggregator::executeImplCase(
	Method & method,
	Arena * aggregates_pool,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumns & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	AggregateDataPtr overflow_row) const
{
	/// Для всех строчек.
	typename Method::iterator it;
	typename Method::Key prev_key;
	for (size_t i = 0; i < rows; ++i)
	{
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?
		bool overflow = false;	/// Новый ключ не поместился в хэш-таблицу из-за no_more_keys.

		/// Получаем ключ для вставки в хэш-таблицу.
		typename Method::Key key = method.getKey(key_columns, keys_size, i, key_sizes, keys);

		if (!no_more_keys)	/// Вставляем.
		{
			/// Оптимизация для часто повторяющихся ключей.
			if (i != 0 && key == prev_key)
			{
				/// Добавляем значения в агрегатные функции.
				AggregateDataPtr value = Method::getAggregateData(it->second);
				for (size_t j = 0; j < aggregates_size; ++j)	/// NOTE: Заменить индекс на два указателя?
					aggregate_functions[j]->add(value + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);

				continue;
			}
			else
				prev_key = key;

			method.data.emplace(key, it, inserted);
		}
		else
		{
			/// Будем добавлять только если ключ уже есть.
			inserted = false;
			it = method.data.find(key);
			if (method.data.end() == it)
				overflow = true;
		}

		/// Если ключ не поместился, и данные не надо агрегировать в отдельную строку, то делать нечего.
		if (no_more_keys && overflow && !overflow_row)
			continue;

		/// Если вставили новый ключ - инициализируем состояния агрегатных функций, и возможно, что-нибудь связанное с ключом.
		if (inserted)
		{
			method.onNewKey(it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
			aggregate_data = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(aggregate_data);
		}

		AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Добавляем значения в агрегатные функции.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->add(value + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
	}
}

#pragma GCC diagnostic pop


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
	Table & table_dst,
	Table & table_src) const
{
	for (auto it = table_src.begin(); it != table_src.end(); ++it)
	{
		decltype(it) res_it;
		bool inserted;
		table_dst.emplace(it->first, res_it, inserted, it.getHash());

		if (!inserted)
		{
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(
					Method::getAggregateData(res_it->second) + offsets_of_aggregate_states[i],
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			Method::getAggregateData(it->second) = nullptr;
		}
		else
		{
			res_it->second = it->second;
		}
	}
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataWithoutKey & res_data = res->without_key;
		AggregatedDataWithoutKey & current_data = non_empty_data[i]->without_key;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

		current_data = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *non_empty_data[i];

		mergeDataImpl<Method>(
			getDataVariant<Method>(*res).data,
			getDataVariant<Method>(current).data);

		/// current не будет уничтожать состояния агрегатных функций в деструкторе
		current.aggregator = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeTwoLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data,
	boost::threadpool::pool * thread_pool) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Слияние распараллеливается по корзинам - первому уровню TwoLevelHashMap.
	auto merge_bucket = [&non_empty_data, &res, this](size_t bucket, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;

		/// Все результаты агрегации соединяем с первым.
		for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
		{
			AggregatedDataVariants & current = *non_empty_data[i];

			mergeDataImpl<Method>(
				getDataVariant<Method>(*res).data.impls[bucket],
				getDataVariant<Method>(current).data.impls[bucket]);

			/// current не будет уничтожать состояния агрегатных функций в деструкторе
			current.aggregator = nullptr;
		}
	};

	/// packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.

	std::vector<std::packaged_task<void()>> tasks;
	tasks.reserve(Method::Data::NUM_BUCKETS);

	for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
	{
		tasks.emplace_back(std::bind(merge_bucket, bucket, current_memory_tracker));

		if (thread_pool)
			thread_pool->schedule([bucket, &tasks] { tasks[bucket](); });
		else
			tasks[bucket]();
	}

	if (thread_pool)
		thread_pool->wait();

	for (auto & task : tasks)
		task.get_future().get();
}


template <typename Method>
void NO_INLINE Aggregator::mergeStreamsImpl(
	Method & method,
	Arena * aggregates_pool,
	size_t start_row,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys) const
{
	method.init(key_columns);

	/// Для всех строчек.
	for (size_t i = start_row; i < rows; ++i)
	{
		typename Method::iterator it;
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?

		/// Получаем ключ для вставки в хэш-таблицу.
		typename Method::Key key = method.getKey(key_columns, keys_size, i, key_sizes, keys);

		method.data.emplace(key, it, inserted);

		if (inserted)
		{
			method.onNewKey(it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
			aggregate_data = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(aggregate_data);
		}

		/// Мерджим состояния агрегатных функций.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->merge(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[j],
				(*aggregate_columns[j])[i]);
	}
}


template <typename Method>
void Aggregator::convertToBlockImpl(
	Method & method,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes,
	size_t start_row,
	bool final) const
{
	if (final)
		convertToBlockImplFinal(method, method.data, key_columns, aggregate_columns, final_aggregate_columns, key_sizes, start_row);
	else
		convertToBlockImplNotFinal(method, method.data, key_columns, aggregate_columns, final_aggregate_columns, key_sizes, start_row);
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes,
	size_t start_row) const
{
	for (typename Table::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		method.insertKeyIntoColumns(*it, key_columns, keys_size, key_sizes);

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->insertResultInto(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
				*final_aggregate_columns[i]);
	}
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes,
	size_t start_row) const
{
	size_t j = start_row;
	for (typename Table::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
	{
		method.insertKeyIntoColumns(*it, key_columns, keys_size, key_sizes);

		for (size_t i = 0; i < aggregates_size; ++i)
			(*aggregate_columns[i])[j] = Method::getAggregateData(it->second) + offsets_of_aggregate_states[i];
	}
}


template <typename Method>
void NO_INLINE Aggregator::destroyImpl(
	Method & method) const
{
	for (typename Method::const_iterator it = method.data.begin(); it != method.data.end(); ++it)
	{
		char * data = Method::getAggregateData(it->second);

		/** Если исключение (обычно нехватка памяти, кидается MemoryTracker-ом) возникло
		  *  после вставки ключа в хэш-таблицу, но до создания всех состояний агрегатных функций,
		  *  то data будет равен nullptr-у.
		  */
		if (nullptr == data)
			continue;

		for (size_t i = 0; i < aggregates_size; ++i)
			if (!aggregate_functions[i]->isState())
				aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);
	}
}


bool Aggregator::executeOnBlock(Block & block, AggregatedDataVariants & result,
	ConstColumnPlainPtrs & key_columns, AggregateColumns & aggregate_columns,
	Sizes & key_sizes, StringRefs & key,
	bool & no_more_keys)
{
	initialize(block);

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_columns[i].resize(aggregates[i].arguments.size());

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(keys[i]).column;

		if (key_columns[i]->isConst())
			throw Exception("Constants is not allowed as GROUP BY keys"
				" (but all of them must be eliminated in ExpressionAnalyzer)", ErrorCodes::ILLEGAL_COLUMN);
	}

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
		{
			aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

			/** Агрегатные функции рассчитывают, что в них передаются полноценные столбцы.
				* Поэтому, стобцы-константы не разрешены в качестве аргументов агрегатных функций.
				*/
			if (aggregate_columns[i][j]->isConst())
				throw Exception("Constants is not allowed as arguments of aggregate functions", ErrorCodes::ILLEGAL_COLUMN);
		}
	}

	size_t rows = block.rows();

	/// Каким способом выполнять агрегацию?
	if (result.empty())
	{
		result.init(chooseAggregationMethod(key_columns, key_sizes));
		result.keys_size = keys_size;
		result.key_sizes = key_sizes;
		LOG_TRACE(log, "Aggregation method: " << result.getMethodName());
	}

	if (overflow_row && !result.without_key)
	{
		result.without_key = result.aggregates_pool->alloc(total_size_of_aggregate_states);
		createAggregateStates(result.without_key);
	}

	if (result.type == AggregatedDataVariants::Type::without_key)
	{
		AggregatedDataWithoutKey & res = result.without_key;
		if (!res)
		{
			res = result.aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(res);
		}

		/// Оптимизация в случае единственной агрегатной функции count.
		AggregateFunctionCount * agg_count = aggregates_size == 1
			? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
			: NULL;

		if (agg_count)
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

	AggregateDataPtr overflow_row_ptr = overflow_row ? result.without_key : nullptr;

#define M(NAME, IS_TWO_LEVEL) \
	else if (result.type == AggregatedDataVariants::Type::NAME) \
		executeImpl(*result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
			result.key_sizes, key, no_more_keys, overflow_row_ptr);

	if (false) {}
	APPLY_FOR_AGGREGATED_VARIANT(M)
#undef M
	else if (result.type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	size_t result_size = result.size();

	/// Если результат уже достаточно большой, и его можно сконвертировать в двухуровневую хэш-таблицу.
	constexpr auto TWO_LEVEL_HASH_TABLE_THRESHOLD = 30000;

	/** Почему выбрано 30 000? Потому что при таком количестве элементов, в TwoLevelHashTable,
	  *  скорее всего, хватит места на все ключи, с размером таблицы по-умолчанию
	  *  (256 корзин по 256 ячеек, fill factor = 0.5)
	  * TODO Не конвертировать, если запрос выполняется в один поток.
	  */

	if (result.isConvertibleToTwoLevel() && result_size >= TWO_LEVEL_HASH_TABLE_THRESHOLD)
		result.convertToTwoLevel();

	/// Проверка ограничений.
	if (!no_more_keys && max_rows_to_group_by && result_size > max_rows_to_group_by)
	{
		if (group_by_overflow_mode == OverflowMode::THROW)
			throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
				+ " rows, maximum: " + toString(max_rows_to_group_by),
				ErrorCodes::TOO_MUCH_ROWS);
		else if (group_by_overflow_mode == OverflowMode::BREAK)
			return false;
		else if (group_by_overflow_mode == OverflowMode::ANY)
			no_more_keys = true;
		else
			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);
	AggregateColumns aggregate_columns(aggregates_size);
	Sizes key_sizes;

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

	/// Читаем все данные
	while (Block block = stream->read())
	{
		src_rows += block.rows();
		src_bytes += block.bytes();

		if (!executeOnBlock(block, result,
			key_columns, aggregate_columns, key_sizes, key,
			no_more_keys))
			break;
	}

	double elapsed_seconds = watch.elapsedSeconds();
	size_t rows = result.size();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


Block Aggregator::convertToBlock(AggregatedDataVariants & data_variants, bool final)
{
	Block res = sample.cloneEmpty();
	size_t rows = data_variants.size();

	LOG_TRACE(log, "Converting aggregated data to block");

	Stopwatch watch;

	/// В какой структуре данных агрегированы данные?
	if (data_variants.empty())
		return Block();

	ColumnPlainPtrs key_columns(keys_size);
	AggregateColumnsData aggregate_columns(aggregates_size);
	ColumnPlainPtrs final_aggregate_columns(aggregates_size);

	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = res.getByPosition(i).column;
		key_columns[i]->reserve(rows);
	}

	try
	{
		for (size_t i = 0; i < aggregates_size; ++i)
		{
			if (!final)
			{
				/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
				ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*res.getByPosition(i + keys_size).column);

				for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
					column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

				aggregate_columns[i] = &column_aggregate_func.getData();
				aggregate_columns[i]->resize(rows);
			}
			else
			{
				ColumnWithNameAndType & column = res.getByPosition(i + keys_size);
				column.type = aggregate_functions[i]->getReturnType();
				column.column = column.type->createColumn();
				column.column->reserve(rows);

				if (aggregate_functions[i]->isState())
				{
					/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
					ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*column.column);

					for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
						column_aggregate_func.addArena(data_variants.aggregates_pools[j]);
				}

				final_aggregate_columns[i] = column.column;
			}
		}

		if (data_variants.type == AggregatedDataVariants::Type::without_key || overflow_row)
		{
			AggregatedDataWithoutKey & data = data_variants.without_key;

			for (size_t i = 0; i < aggregates_size; ++i)
				if (!final)
					(*aggregate_columns[i])[0] = data + offsets_of_aggregate_states[i];
				else
					aggregate_functions[i]->insertResultInto(data + offsets_of_aggregate_states[i], *final_aggregate_columns[i]);

			if (overflow_row)
				for (size_t i = 0; i < keys_size; ++i)
					key_columns[i]->insertDefault();
		}

		size_t start_row = overflow_row ? 1 : 0;

	#define M(NAME, IS_TWO_LEVEL) \
		else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
			convertToBlockImpl(*data_variants.NAME, key_columns, aggregate_columns, \
				final_aggregate_columns, data_variants.key_sizes, start_row, final);

		if (false) {}
		APPLY_FOR_AGGREGATED_VARIANT(M)
	#undef M
		else if (data_variants.type != AggregatedDataVariants::Type::without_key)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
	catch (...)
	{
		/** Работа с состояниями агрегатных функций недостаточно exception-safe.
		  * Если часть столбцов aggregate_columns была resize-на, но значения не были вставлены,
		  *  то эти столбцы будут в некорректном состоянии
		  *  (ColumnAggregateFunction попытаются в деструкторе вызвать деструкторы у элементов, которых нет),
		  *  а также деструкторы будут вызываться у AggregatedDataVariants.
		  * Поэтому, вручную "откатываем" их.
		  */
		for (size_t i = 0; i < aggregates_size; ++i)
			if (aggregate_columns[i])
				aggregate_columns[i]->clear();

		throw;
	}

	if (!final)
	{
		/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе. Теперь состояниями владеют ColumnAggregateFunction.
		data_variants.aggregator = nullptr;
	}

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


AggregatedDataVariantsPtr Aggregator::merge(ManyAggregatedDataVariants & data_variants, size_t max_threads)
{
	if (data_variants.empty())
 		throw Exception("Empty data passed to Aggregator::merge().", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	Stopwatch watch;

	ManyAggregatedDataVariants non_empty_data;
	non_empty_data.reserve(data_variants.size());
	for (auto & data : data_variants)
		if (!data->empty())
			non_empty_data.push_back(data);

	if (non_empty_data.empty())
		return data_variants[0];

	if (non_empty_data.size() == 1)
		return non_empty_data[0];

	/// Если хотя бы один из вариантов двухуровневый, то переконвертируем все варианты в двухуровневые, если есть не такие.
	/// Замечание - возможно, было бы более оптимально не конвертировать одноуровневые варианты перед мерджем, а мерджить их отдельно, в конце.

	bool has_at_least_one_two_level = false;
	for (const auto & variant : non_empty_data)
	{
		if (variant->isTwoLevel())
		{
			has_at_least_one_two_level = true;
			break;
		}
	}

	if (has_at_least_one_two_level)
		for (auto & variant : non_empty_data)
			if (!variant->isTwoLevel())
				variant->convertToTwoLevel();

	AggregatedDataVariantsPtr & res = non_empty_data[0];

	size_t rows = res->size();
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		rows += non_empty_data[i]->size();
		AggregatedDataVariants & current = *non_empty_data[i];

		if (res->type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		res->aggregates_pools.insert(res->aggregates_pools.end(), current.aggregates_pools.begin(), current.aggregates_pools.end());
	}

	/// В какой структуре данных агрегированы данные?
	if (res->type == AggregatedDataVariants::Type::without_key || overflow_row)
		mergeWithoutKeyDataImpl(non_empty_data);

	boost::threadpool::pool * thread_pool = nullptr;
	if (max_threads > 1 && rows > 100000	/// TODO Сделать настраиваемый порог.
		&& res->isTwoLevel())
		thread_pool = new boost::threadpool::pool(max_threads);

	/// TODO Упростить.
	if (res->type == AggregatedDataVariants::Type::key8)
		mergeSingleLevelDataImpl<decltype(res->key8)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key16)
		mergeSingleLevelDataImpl<decltype(res->key16)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key32)
		mergeSingleLevelDataImpl<decltype(res->key32)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key64)
		mergeSingleLevelDataImpl<decltype(res->key64)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key_string)
		mergeSingleLevelDataImpl<decltype(res->key_string)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key_fixed_string)
		mergeSingleLevelDataImpl<decltype(res->key_fixed_string)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::keys128)
		mergeSingleLevelDataImpl<decltype(res->keys128)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::hashed)
		mergeSingleLevelDataImpl<decltype(res->hashed)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key32_two_level)
		mergeTwoLevelDataImpl<decltype(res->key32_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::Type::key64_two_level)
		mergeTwoLevelDataImpl<decltype(res->key64_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::Type::key_string_two_level)
		mergeTwoLevelDataImpl<decltype(res->key_string_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::Type::key_fixed_string_two_level)
		mergeTwoLevelDataImpl<decltype(res->key_fixed_string_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::Type::keys128_two_level)
		mergeTwoLevelDataImpl<decltype(res->keys128_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type == AggregatedDataVariants::Type::hashed_two_level)
		mergeTwoLevelDataImpl<decltype(res->hashed_two_level)::element_type>(non_empty_data, thread_pool);
	else if (res->type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

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
	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	AggregateColumnsData aggregate_columns(aggregates_size);

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
			aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

		size_t rows = block.rows();

		/// Каким способом выполнять агрегацию?
		Sizes key_sizes;
		AggregatedDataVariants::Type method = chooseAggregationMethod(key_columns, key_sizes);

		if (result.empty())
		{
			result.init(method);
			result.keys_size = keys_size;
			result.key_sizes = key_sizes;
		}

		if (result.type == AggregatedDataVariants::Type::without_key || overflow_row)
		{
			AggregatedDataWithoutKey & res = result.without_key;
			if (!res)
			{
				res = result.aggregates_pool->alloc(total_size_of_aggregate_states);
				createAggregateStates(res);
			}

			/// Добавляем значения
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0]);
		}

		size_t start_row = overflow_row ? 1 : 0;

	#define M(NAME, IS_TWO_LEVEL) \
		else if (result.type == AggregatedDataVariants::Type::NAME) \
			mergeStreamsImpl(*result.NAME, result.aggregates_pool, start_row, rows, key_columns, aggregate_columns, key_sizes, key);

		if (false) {}
		APPLY_FOR_AGGREGATED_VARIANT(M)
	#undef M
		else if (result.type != AggregatedDataVariants::Type::without_key)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

		LOG_TRACE(log, "Merged aggregated block");
	}
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
	if (result.size() == 0)
		return;

	LOG_TRACE(log, "Destroying aggregate states");

	/// В какой структуре данных агрегированы данные?
	if (result.type == AggregatedDataVariants::Type::without_key || overflow_row)
	{
		AggregatedDataWithoutKey & res_data = result.without_key;

		if (nullptr != res_data)
			for (size_t i = 0; i < aggregates_size; ++i)
				if (!aggregate_functions[i]->isState())
					aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);
	}

#define M(NAME, IS_TWO_LEVEL) \
	else if (result.type == AggregatedDataVariants::Type::NAME) \
		destroyImpl(*result.NAME);

	if (false) {}
	APPLY_FOR_AGGREGATED_VARIANT(M)
#undef M
	else if (result.type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
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
