#include <math.h>

#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArithmetic.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Functions/DataTypeTraits.h>
#include <DB/DataTypes/DataTypeEnum.h>
#include <DB/DataTypes/NullSymbol.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnNullable.h>
#include <common/ClickHouseRevision.h>
#include <ext/enumerate.hpp>


namespace DB
{


static inline UInt64 stringWidth(const UInt8 * pos, const UInt8 * end)
{
	UInt64 res = 0;
	for (; pos < end; ++pos)
	{
		if (*pos == '\b' || *pos == '\f' || *pos == '\n' || *pos == '\r' || *pos == '\t' || *pos == '\0' || *pos == '\'' || *pos == '\\')
			++res;
		if (*pos <= 0x7F || *pos >= 0xC0)
			++res;
	}
	return res;
}

static inline UInt64 stringWidthConstant(const String & data)
{
	return stringWidth(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data()) + data.size());
}

template <typename T>
static void numWidthVector(const PaddedPODArray<T> & a, PaddedPODArray<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		if (a[i] >= 0)
			c[i] = a[i] ? 1 + log10(a[i]) : 1;
		else if (std::is_signed<T>::value && a[i] == std::numeric_limits<T>::min())
			c[i] = 2 + log10(std::numeric_limits<T>::max());
		else
			c[i] = 2 + log10(-a[i]);
}

template <typename T>
static void numWidthConstant(T a, UInt64 & c)
{
	if (a >= 0)
		c = a ? 1 + log10(a) : 1;
	else if (std::is_signed<T>::value && a == std::numeric_limits<T>::min())
		c = 2 + log10(std::numeric_limits<T>::max());
	else
		c = 2 + log10(-a);
}

inline UInt64 floatWidth(const double x)
{
	DoubleConverter<false>::BufferType buffer;
	double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

	const auto result = DoubleConverter<false>::instance().ToShortest(x, &builder);

	if (!result)
		throw Exception("Cannot print double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	return builder.position();
}

inline UInt64 floatWidth(const float x)
{
	DoubleConverter<false>::BufferType buffer;
	double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

	const auto result = DoubleConverter<false>::instance().ToShortestSingle(x, &builder);

	if (!result)
		throw Exception("Cannot print float number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	return builder.position();
}

template <typename T>
static void floatWidthVector(const PaddedPODArray<T> & a, PaddedPODArray<UInt64> & c)
{
	size_t size = a.size();
	for (size_t i = 0; i < size; ++i)
		c[i] = floatWidth(a[i]);
}

template <typename T>
static void floatWidthConstant(T a, UInt64 & c)
{
	c = floatWidth(a);
}

template <> inline void numWidthVector<Float64>(const PaddedPODArray<Float64> & a, PaddedPODArray<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthVector<Float32>(const PaddedPODArray<Float32> & a, PaddedPODArray<UInt64> & c) { floatWidthVector(a, c); }
template <> inline void numWidthConstant<Float64>(Float64 a, UInt64 & c) { floatWidthConstant(a, c); }
template <> inline void numWidthConstant<Float32>(Float32 a, UInt64 & c) { floatWidthConstant(a, c); }


static inline void stringWidthVector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt64> & res)
{
	size_t size = offsets.size();

	size_t prev_offset = 0;
	for (size_t i = 0; i < size; ++i)
	{
		res[i] = stringWidth(&data[prev_offset], &data[offsets[i] - 1]);
		prev_offset = offsets[i];
	}
}

static inline void stringWidthFixedVector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
{
	size_t size = data.size() / n;
	for (size_t i = 0; i < size; ++i)
		res[i] = stringWidth(&data[i * n], &data[(i + 1) * n]);
}


namespace VisibleWidth
{
	template <typename T>
	static bool executeConstNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnConst<T> * col = typeid_cast<const ColumnConst<T> *>(column.get()))
		{
			UInt64 res = 0;
			numWidthConstant(col->getData(), res);
			block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(column->size(), res);
			return true;
		}
		else
			return false;
	}

	template <typename T>
	static bool executeNumber(Block & block, const ColumnPtr & column, size_t result)
	{
		if (const ColumnVector<T> * col = typeid_cast<const ColumnVector<T> *>(column.get()))
		{
			auto res = std::make_shared<ColumnUInt64>(column->size());
			block.getByPosition(result).column = res;
			numWidthVector(col->getData(), res->getData());
			return true;
		}
		else
			return false;
	}

	template <typename DataTypeEnum>
	static bool executeEnum(Block & block, const DataTypePtr & type_ptr, const ColumnPtr & column, const size_t result)
	{
		if (const auto type = typeid_cast<const DataTypeEnum *>(type_ptr.get()))
		{
			if (const auto col = typeid_cast<const typename DataTypeEnum::ColumnType *>(column.get()))
			{
				const auto res = std::make_shared<ColumnUInt64>(col->size());
				block.getByPosition(result).column = res;

				const auto & in = col->getData();
				auto & out = res->getData();

				for (const auto & idx_num : ext::enumerate(in))
				{
					StringRef name = type->getNameForValue(idx_num.second);
					out[idx_num.first] = stringWidth(
						reinterpret_cast<const UInt8 *>(name.data),
						reinterpret_cast<const UInt8 *>(name.data) + name.size);
				}

				return true;
			}
			else if (const auto col = typeid_cast<const typename DataTypeEnum::ConstColumnType *>(column.get()))
			{
				StringRef name = type->getNameForValue(col->getData());

				block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(
					col->size(), stringWidth(
						reinterpret_cast<const UInt8 *>(name.data),
						reinterpret_cast<const UInt8 *>(name.data) + name.size));

				return true;
			}
		}

		return false;
	}
}


void FunctionVisibleWidth::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	auto & element = block.getByPosition(arguments[0]);

	auto & res_element = block.getByPosition(result);
	auto & res_col = res_element.column;

	size_t row_count = block.rowsInFirstColumn();

	if (element.column->isNull())
	{
		/// The input column has the Null type.
		res_col = std::make_shared<ColumnConstUInt64>(row_count, strlen(NullSymbol::Escaped::name));
	}
	else if (element.column->isNullable())
	{
		/// Perform visibleWidth on a block that holds the nested column
		/// of the input column.
		auto & nullable_col = static_cast<ColumnNullable &>(*element.column);
		auto & nested_col = nullable_col.getNestedColumn();

		auto & nullable_type = static_cast<DataTypeNullable &>(*element.type);
		auto & nested_type = nullable_type.getNestedType();

		Block block_with_nested_col =
		{
			{
				nested_col,
				nested_type,
				element.name
			},

			{
				nullptr,
				res_element.type,
				""
			}
		};

		perform(block_with_nested_col, {0, 1}, 1);

		/// Create the result. If any row of the input column holds a NULL value,
		/// we assign the corresponding row of the result the length of the NULL
		/// symbol.
		res_col = std::make_shared<ColumnUInt64>(row_count);
		auto & res_data = static_cast<ColumnUInt64 &>(*res_col).getData();

		const auto & src = static_cast<const ColumnUInt64 &>(
			*block_with_nested_col.unsafeGetByPosition(1).column
		).getData();

		for (size_t row = 0; row < row_count; ++row)
		{
			if (nullable_col.isNullAt(row))
				res_data[row] = strlen(NullSymbol::Escaped::name);
			else
				res_data[row] = src[row];
		}
	}
	else
	{
		/// The input column has an ordinary type.
		perform(block, arguments, result);
	}
}


void FunctionVisibleWidth::perform(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.getByPosition(arguments[0]).column;
	const DataTypePtr type = block.getByPosition(arguments[0]).type;
	size_t rows = column->size();

	if (typeid_cast<const DataTypeDate *>(type.get()))
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(rows, strlen("0000-00-00"));
	}
	else if (typeid_cast<const DataTypeDateTime *>(type.get()))
	{
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(rows, strlen("0000-00-00 00:00:00"));
	}
	else if (VisibleWidth::executeEnum<DataTypeEnum8>(block, type, column, result)
		|| VisibleWidth::executeEnum<DataTypeEnum16>(block, type, column, result))
	{
	}
	else if (VisibleWidth::executeConstNumber<UInt8>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt16>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt32>(block, column, result)
		|| VisibleWidth::executeConstNumber<UInt64>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int8>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int16>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int32>(block, column, result)
		|| VisibleWidth::executeConstNumber<Int64>(block, column, result)
		|| VisibleWidth::executeConstNumber<Float32>(block, column, result)
		|| VisibleWidth::executeConstNumber<Float64>(block, column, result)
		|| VisibleWidth::executeNumber<UInt8>(block, column, result)
		|| VisibleWidth::executeNumber<UInt16>(block, column, result)
		|| VisibleWidth::executeNumber<UInt32>(block, column, result)
		|| VisibleWidth::executeNumber<UInt64>(block, column, result)
		|| VisibleWidth::executeNumber<Int8>(block, column, result)
		|| VisibleWidth::executeNumber<Int16>(block, column, result)
		|| VisibleWidth::executeNumber<Int32>(block, column, result)
		|| VisibleWidth::executeNumber<Int64>(block, column, result)
		|| VisibleWidth::executeNumber<Float32>(block, column, result)
		|| VisibleWidth::executeNumber<Float64>(block, column, result))
	{
	}
	else if (const ColumnString * col = typeid_cast<const ColumnString *>(column.get()))
	{
		auto res = std::make_shared<ColumnUInt64>(rows);
		block.getByPosition(result).column = res;
		stringWidthVector(col->getChars(), col->getOffsets(), res->getData());
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(column.get()))
	{
		auto res = std::make_shared<ColumnUInt64>(rows);
		block.getByPosition(result).column = res;
		stringWidthFixedVector(col->getChars(), col->getN(), res->getData());
	}
	else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(column.get()))
	{
		UInt64 res = 0;
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(rows, res);
		stringWidthConstant(col->getData(), res);
	}
	else if (const ColumnArray * col = typeid_cast<const ColumnArray *>(column.get()))
	{
		/// Calculate visible width of elements of array.
		Block nested_block;
		ColumnWithTypeAndName nested_values;
		nested_values.type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
		nested_values.column = col->getDataPtr();
		nested_block.insert(nested_values);

		ColumnWithTypeAndName nested_result;
		nested_result.type = std::make_shared<DataTypeUInt64>();
		nested_block.insert(nested_result);

		executeImpl(nested_block, {0}, 1);

		/// Then accumulate and place into result.
		auto res = std::make_shared<ColumnUInt64>(rows);
		block.getByPosition(result).column = res;
		ColumnUInt64::Container_t & vec = res->getData();

		/// If the elements of the array are nullable, we have to check whether
		/// an element is a NULL for it is not surrounded by a pair of quotes.
		const PaddedPODArray<UInt8> * null_map = nullptr;
		if (nested_values.type->isNullable())
		{
			const auto & nullable_col = static_cast<const ColumnNullable &>(col->getData());
			null_map = &static_cast<const ColumnUInt8 &>(*nullable_col.getNullValuesByteMap()).getData();
		}

		const auto & observed_type = DataTypeTraits::removeNullable(nested_values.type);

		size_t additional_symbols = 0;	/// Quotes.
		if (typeid_cast<const DataTypeDate *>(observed_type.get())
			|| typeid_cast<const DataTypeDateTime *>(observed_type.get())
			|| typeid_cast<const DataTypeString *>(observed_type.get())
			|| typeid_cast<const DataTypeFixedString *>(observed_type.get())
			|| typeid_cast<const DataTypeEnum8 *>(observed_type.get())
			|| typeid_cast<const DataTypeEnum16 *>(observed_type.get()))
			additional_symbols = 2;

		if (ColumnUInt64 * nested_result_column = typeid_cast<ColumnUInt64 *>(nested_block.getByPosition(1).column.get()))
		{
			ColumnUInt64::Container_t & nested_res = nested_result_column->getData();

			size_t j = 0;
			for (size_t i = 0; i < rows; ++i)
			{
				/** If empty array, then two characters: [];
				  * if not, then character '[', and one extra character for each element: either ',' or ']'.
				  */
				vec[i] = (j == col->getOffsets()[i]) ? 2 : 1;

				for (; j < col->getOffsets()[i]; ++j)
				{
					size_t effective_additional_symbols;
					if ((null_map != nullptr) && ((*null_map)[j] == 1))
					{
						/// The NULL value is not quoted.
						effective_additional_symbols = 0;
					}
					else
						effective_additional_symbols = additional_symbols;

					vec[i] += 1 + effective_additional_symbols + nested_res[j];
				}
			}
		}
		else if (ColumnConstUInt64 * nested_result_column = typeid_cast<ColumnConstUInt64 *>(nested_block.getByPosition(1).column.get()))
		{
			size_t j = 0;
			for (size_t i = 0; i < rows; ++i)
			{
				size_t width = 0;

				for (; j < col->getOffsets()[i]; ++j)
				{
					size_t effective_additional_symbols;
					if ((null_map != nullptr) && ((*null_map)[j] == 1))
					{
						/// The NULL value is not quoted.
						effective_additional_symbols = 0;
					}
					else
						effective_additional_symbols = additional_symbols;

					width += 1 + effective_additional_symbols + nested_result_column->getData();
				}

				vec[i] = 1 + std::max(static_cast<size_t>(1), width);
			}
		}
	}
	else if (const ColumnTuple * col = typeid_cast<const ColumnTuple *>(column.get()))
	{
		/// Calculate visible width for each nested column separately, and then accumulate.
		Block nested_block = col->getData();
		size_t columns = nested_block.columns();

		FunctionPlus func_plus;

		for (size_t i = 0; i < columns; ++i)
		{
			nested_block.getByPosition(i).type = static_cast<const DataTypeTuple &>(*type).getElements()[i];

			/** nested_block will consist of following columns:
			  * x1, x2, x3... , width1, width2, width1 + width2, width3, width1 + width2 + width3, ...
			  */

			ColumnWithTypeAndName nested_result;
			nested_result.type = std::make_shared<DataTypeUInt64>();
			nested_block.insert(nested_result);

			ColumnNumbers nested_argument_numbers(1, i);
			execute(nested_block, nested_argument_numbers, nested_block.columns() - 1);

			if (i != 0)
			{
				ColumnWithTypeAndName plus_result;
				plus_result.type = std::make_shared<DataTypeUInt64>();
				nested_block.insert(plus_result);

				ColumnNumbers plus_argument_numbers(2);
				plus_argument_numbers[0] = nested_block.columns() - 3;
				plus_argument_numbers[1] = nested_block.columns() - 2;
				func_plus.execute(nested_block, plus_argument_numbers, nested_block.columns() - 1);
			}
		}

		/// Add also number of characters for quotes and commas.

		size_t additional_symbols = columns - 1;	/// Commas.
		for (size_t i = 0; i < columns; ++i)
		{
			if (typeid_cast<const DataTypeDate *>(nested_block.getByPosition(i).type.get())
				|| typeid_cast<const DataTypeDateTime *>(nested_block.getByPosition(i).type.get())
				|| typeid_cast<const DataTypeString *>(nested_block.getByPosition(i).type.get())
				|| typeid_cast<const DataTypeFixedString *>(nested_block.getByPosition(i).type.get())
				|| typeid_cast<const DataTypeEnum8 *>(nested_block.getByPosition(i).type.get())
				|| typeid_cast<const DataTypeEnum16 *>(nested_block.getByPosition(i).type.get()))
				additional_symbols += 2;			/// Quotes.
		}

		ColumnPtr & nested_result_column = nested_block.getByPosition(nested_block.columns() - 1).column;

		if (nested_result_column->isConst())
		{
			ColumnConstUInt64 & nested_result_column_const = typeid_cast<ColumnConstUInt64 &>(*nested_result_column);
			if (nested_result_column_const.size())
				nested_result_column_const.getData() += 2 + additional_symbols;
		}
		else
		{
			ColumnUInt64 & nested_result_column_vec = typeid_cast<ColumnUInt64 &>(*nested_result_column);
			ColumnUInt64::Container_t & nested_res = nested_result_column_vec.getData();

			for (size_t i = 0; i < rows; ++i)
				nested_res[i] += 2 + additional_symbols;
		}

		block.getByPosition(result).column = nested_result_column;
	}
	else if (typeid_cast<const ColumnConstArray *>(column.get())
		|| typeid_cast<const ColumnConstTuple *>(column.get()))
	{
		String s;
		{
			WriteBufferFromString wb(s);
			type->serializeTextEscaped(*column->cut(0, 1)->convertToFullColumnIfConst(), 0, wb);
		}

		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(rows, s.size());
	}
	else if (typeid_cast<const ColumnAggregateFunction *>(column.get()))
	{
		/** Return obviously wrong (arbitary) value for states of aggregate functions.
		  * Result of visibleWidth is used for presentation purposes,
		  *  and state of aggregate function is presented as unreadable sequence of bytes,
		  *  so using wrong calculation of its displayed width don't make presentation much worse.
		  */
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt64>(rows, 10);
	}
	else
	   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
}


void FunctionHasColumnInTable::getReturnTypeAndPrerequisitesImpl(
	const ColumnsWithTypeAndName & arguments,
	DataTypePtr & out_return_type,
	ExpressionActions::Actions & out_prerequisites)
{
	static const std::string arg_pos_description[] = {"First", "Second", "Third"};
	for (size_t i = 0; i < getNumberOfArguments(); ++i)
	{
		const ColumnWithTypeAndName & argument = arguments[i];

		const ColumnConstString * column = typeid_cast<const ColumnConstString *>(argument.column.get());
		if (!column)
		{
			throw Exception(
				arg_pos_description[i] + " argument for function " + getName() + " must be const String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
	}

	out_return_type = std::make_shared<DataTypeUInt8>();
}


void FunctionHasColumnInTable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	auto get_string_from_block =
		[&](size_t column_pos) -> const String &
		{
			DB::ColumnPtr column = block.getByPosition(column_pos).column;
			const ColumnConstString * const_column = typeid_cast<const ColumnConstString *>(column.get());
			return const_column->getData();
		};

	const DB::String & database_name = get_string_from_block(arguments[0]);
	const DB::String & table_name = get_string_from_block(arguments[1]);
	const DB::String & column_name = get_string_from_block(arguments[2]);

	const DB::StoragePtr & table = global_context.getTable(database_name, table_name);
	const bool has_column = table->hasColumn(column_name);

	block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(
		block.rowsInFirstColumn(), has_column);
}


std::string FunctionVersion::getVersion() const
{
	std::ostringstream os;
	os << DBMS_VERSION_MAJOR << "." << DBMS_VERSION_MINOR << "." << ClickHouseRevision::get();
	return os.str();
}


void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
	factory.registerFunction<FunctionCurrentDatabase>();
	factory.registerFunction<FunctionHostName>();
	factory.registerFunction<FunctionVisibleWidth>();
	factory.registerFunction<FunctionToTypeName>();
	factory.registerFunction<FunctionToColumnTypeName>();
	factory.registerFunction<FunctionBlockSize>();
	factory.registerFunction<FunctionBlockNumber>();
	factory.registerFunction<FunctionRowNumberInBlock>();
	factory.registerFunction<FunctionRowNumberInAllBlocks>();
	factory.registerFunction<FunctionSleep>();
	factory.registerFunction<FunctionMaterialize>();
	factory.registerFunction<FunctionIgnore>();
	factory.registerFunction<FunctionIndexHint>();
	factory.registerFunction<FunctionIdentity>();
	factory.registerFunction<FunctionArrayJoin>();
	factory.registerFunction<FunctionReplicate>();
	factory.registerFunction<FunctionBar>();
	factory.registerFunction<FunctionHasColumnInTable>();

	factory.registerFunction<FunctionTuple>();
	factory.registerFunction<FunctionTupleElement>();
	factory.registerFunction<FunctionIn<false, false>>();
	factory.registerFunction<FunctionIn<false, true>>();
	factory.registerFunction<FunctionIn<true, false>>();
	factory.registerFunction<FunctionIn<true, true>>();

	factory.registerFunction<FunctionIsFinite>();
	factory.registerFunction<FunctionIsInfinite>();
	factory.registerFunction<FunctionIsNaN>();

	factory.registerFunction<FunctionVersion>();
	factory.registerFunction<FunctionUptime>();

	factory.registerFunction<FunctionRunningAccumulate>();
	factory.registerFunction<FunctionRunningDifference>();
	factory.registerFunction<FunctionFinalizeAggregation>();
}

}
