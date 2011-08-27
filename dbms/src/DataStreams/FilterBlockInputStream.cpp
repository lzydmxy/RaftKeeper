#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/FilterBlockInputStream.h>


namespace DB
{

FilterBlockInputStream::FilterBlockInputStream(SharedPtr<IBlockInputStream> input_, size_t filter_column_)
	: input(input_), filter_column(filter_column_)
{
}

Block FilterBlockInputStream::read()
{
	Block res = input->read();
	if (!res)
		return res;

	size_t columns = res.columns();

	ColumnConstUInt8 * column_const = dynamic_cast<ColumnConstUInt8 *>(&*res.getByPosition(filter_column).column);
	if (column_const)
	{
		return column_const->getData()
			? res
			: Block();
	}

	ColumnUInt8 * column = dynamic_cast<ColumnUInt8 *>(&*res.getByPosition(filter_column).column);
	if (!column)
		throw Exception("Illegal type of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

	IColumn::Filter & filter = column->getData();

	for (size_t i = 0; i < columns; ++i)
		if (i != filter_column)
			res.getByPosition(i).column->filter(filter);

	res.erase(filter_column);
	return res;
}

}
