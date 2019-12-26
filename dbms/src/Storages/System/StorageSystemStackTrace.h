#pragma once

#ifdef OS_LINUX /// Because of 'sigqueue' functions and RT signals.

#include <mutex>
#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Allows to introspect stack trace of all server threads.
/// It acts like an embedded debugger.
/// More than one instance of this table cannot be used.
class StorageSystemStackTrace : public ext::shared_ptr_helper<StorageSystemStackTrace>, public IStorageSystemOneBlock<StorageSystemStackTrace>
{
    friend struct ext::shared_ptr_helper<StorageSystemStackTrace>;
public:
    String getName() const override { return "SystemStackTrace"; }
    static NamesAndTypesList getNamesAndTypes();

    StorageSystemStackTrace(const String & name_);

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    mutable std::mutex mutex;
};

}

#endif
