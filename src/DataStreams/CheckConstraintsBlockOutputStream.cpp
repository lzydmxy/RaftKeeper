#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
    extern const int UNSUPPORTED_METHOD;
}


CheckConstraintsBlockOutputStream::CheckConstraintsBlockOutputStream(
    const StorageID & table_id_,
    const BlockOutputStreamPtr & output_,
    const Block & header_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : table_id(table_id_),
    output(output_),
    header(header_),
    constraints(constraints_),
    expressions(constraints_.getExpressions(context_, header.getNamesAndTypesList()))
{
}


void CheckConstraintsBlockOutputStream::write(const Block & block)
{
    if (block.rows() > 0)
    {
        Block block_to_calculate = block;
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            auto constraint_expr = expressions[i];
            constraint_expr->execute(block_to_calculate);

            auto * constraint_ptr = constraints.constraints[i]->as<ASTConstraintDeclaration>();

            ColumnWithTypeAndName res_column = block_to_calculate.getByName(constraint_ptr->expr->getColumnName());

            auto result_type = removeNullable(removeLowCardinality(res_column.type));
            auto result_column = res_column.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

            if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(*result_column))
                result_column = column_nullable->getNestedColumnPtr();

            if (!isUInt8(result_type))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Constraint {} does not return a value of type UInt8",
                    backQuote(constraint_ptr->name));

            const ColumnUInt8 & res_column_uint8 = assert_cast<const ColumnUInt8 &>(*result_column);

            const UInt8 * data = res_column_uint8.getData().data();
            size_t size = res_column_uint8.size();

            std::cerr << std::endl;
            for (size_t j = 0; j < size; ++j)
                std::cerr << data[j] << " ";
            std::cerr << std::endl;

            /// Is violated.
            if (!memoryIsByte(data, size, 1))
            {
                size_t row_idx = 0;
                for (; row_idx < size; ++row_idx)
                    if (data[row_idx] != 1)
                        break;

                Names related_columns = constraint_expr->getRequiredColumns();

                bool first = true;
                String column_values_msg;
                constexpr size_t approx_bytes_for_col = 32;
                column_values_msg.reserve(approx_bytes_for_col * related_columns.size());
                for (const auto & name : related_columns)
                {
                    const IColumn & column = *block.getByName(name).column;
                    assert(row_idx < column.size());

                    if (!first)
                        column_values_msg.append(", ");
                    column_values_msg.append(backQuoteIfNeed(name));
                    column_values_msg.append(" = ");
                    column_values_msg.append(applyVisitor(FieldVisitorToString(), column[row_idx]));
                    first = false;
                }

                throw Exception(
                    ErrorCodes::VIOLATED_CONSTRAINT,
                    "Constraint {} for table {} is violated at row {}. Expression: ({}). Column values: {}",
                    backQuote(constraint_ptr->name),
                    table_id.getNameForLogs(),
                    rows_written + row_idx + 1,
                    serializeAST(*(constraint_ptr->expr), true),
                    column_values_msg);
            }
        }
    }

    output->write(block);
    rows_written += block.rows();
}

void CheckConstraintsBlockOutputStream::flush()
{
    output->flush();
}

void CheckConstraintsBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void CheckConstraintsBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

}
