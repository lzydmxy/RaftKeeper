#pragma once
#include <Processors/ISimpleTransform.h>
#include <Core/SortDescription.h>
#include <Common/FillingRow.h>

namespace DB
{

/** Implements the WITH FILL part of ORDER BY operation.
*/
class FillingTransform : public ISimpleTransform
{
public:
    FillingTransform(const Block & header_, const SortDescription & fill_description_);

    String getName() const override { return "FillingTransform"; }

    Status prepare() override;

protected:
    void transform(Chunk & Chunk) override;

private:
    void setResultColumns(Chunk & chunk, MutableColumns & fill_columns, MutableColumns & other_columns) const;

    const SortDescription sort_description; /// Contains only rows with WITH FILL.
    FillingRow filling_row; /// Current row, which is used to fill gaps.
    FillingRow next_row; /// Row to which we need to generate filling rows.

    using Positions = std::vector<size_t>;
    Positions fill_column_positions;
    Positions other_column_positions;
    bool first = true;
    bool generate_suffix = false;

    /// Determines should we insert filling row before start generating next rows.
    bool should_insert_first = false;
};

}
