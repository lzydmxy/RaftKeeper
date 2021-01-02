#include "ComplexKeyDirectDictionary.h"
#include <IO/WriteHelpers.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include <Core/Defines.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}


ComplexKeyDirectDictionary::ComplexKeyDirectDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    BlockPtr saved_block_)
    : IDictionaryBase(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , saved_block{std::move(saved_block_)}
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{full_name + ": source cannot be used with ComplexKeyDirectDictionary", ErrorCodes::UNSUPPORTED_METHOD};


    createAttributes();
}

ColumnPtr ComplexKeyDirectDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr &,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr default_untyped) const
{
    dict_struct.validateKeyTypes(key_types);

    ColumnPtr result;

    const auto & attribute = getAttribute(attribute_name);

    /// TODO: Check that attribute type is same as result type
    /// TODO: Check if const will work as expected

    auto size = key_columns.front()->size();

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        if constexpr (std::is_same_v<AttributeType, String>)
        {
            auto column_string = ColumnString::create();
            auto * out = column_string.get();

            if (default_untyped != nullptr)
            {
                if (const auto * const default_col = checkAndGetColumn<ColumnString>(*default_untyped))
                {
                    getItemsImpl<String, String>(
                        attribute,
                        key_columns,
                        [&](const size_t, const String value)
                        {
                            const auto ref = StringRef{value};
                            out->insertData(ref.data, ref.size);
                        },
                        [&](const size_t row)
                        {
                            const auto ref = default_col->getDataAt(row);
                            return String(ref.data, ref.size);
                        });
                }
                else if (const auto * const default_col_const = checkAndGetColumnConst<ColumnString>(default_untyped.get()))
                {
                    const auto & def = default_col_const->template getValue<String>();

                    getItemsImpl<String, String>(
                        attribute,
                        key_columns,
                        [&](const size_t, const String value)
                        {
                            const auto ref = StringRef{value};
                            out->insertData(ref.data, ref.size);
                        },
                        [&](const size_t) { return def; });
                }
            }
            else
            {
                const auto & null_value = std::get<StringRef>(attribute.null_values);

                getItemsImpl<String, String>(
                    attribute,
                    key_columns,
                    [&](const size_t, const String value)
                    {
                        const auto ref = StringRef{value};
                        out->insertData(ref.data, ref.size);
                    },
                    [&](const size_t) { return String(null_value.data, null_value.size); });
            }

            result = std::move(column_string);
        }
        else
        {
            using ResultColumnType
                = std::conditional_t<IsDecimalNumber<AttributeType>, ColumnDecimal<AttributeType>, ColumnVector<AttributeType>>;
            using ResultColumnPtr = typename ResultColumnType::MutablePtr;

            ResultColumnPtr column;

            if constexpr (IsDecimalNumber<AttributeType>)
            {
                // auto scale = getDecimalScale(*attribute.type);
                column = ColumnDecimal<AttributeType>::create(size, 0);
            }
            else if constexpr (IsNumber<AttributeType>)
                column = ColumnVector<AttributeType>::create(size);

            auto & out = column->getData();

            if (default_untyped != nullptr)
            {
                if (const auto * const default_col = checkAndGetColumn<ResultColumnType>(*default_untyped))
                {
                    getItemsImpl<AttributeType, AttributeType>(
                        attribute,
                        key_columns,
                        [&](const size_t row, const auto value) { return out[row] = value; },
                        [&](const size_t row) { return default_col->getData()[row]; }
                    );
                }
                else if (const auto * const default_col_const = checkAndGetColumnConst<ResultColumnType>(default_untyped.get()))
                {
                    const auto & def = default_col_const->template getValue<AttributeType>();

                    getItemsImpl<AttributeType, AttributeType>(
                        attribute,
                        key_columns,
                        [&](const size_t row, const auto value) { return out[row] = value; },
                        [&](const size_t) { return def; }
                    );
                }
            }
            else
            {
                const auto null_value = std::get<AttributeType>(attribute.null_values);

                getItemsImpl<AttributeType, AttributeType>(
                    attribute,
                    key_columns,
                    [&](const size_t row, const auto value) { return out[row] = value; },
                    [&](const size_t) { return null_value; }
                );
            }

            result = std::move(column);
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    return result;
}

ColumnUInt8::Ptr ComplexKeyDirectDictionary::has(const Columns & key_columns, const DataTypes & key_types) const
{
    dict_struct.validateKeyTypes(key_types);

    auto size = key_columns.front()->size();
    auto result = ColumnUInt8::create(size);
    auto& out = result->getData();

    const auto rows = key_columns.front()->size();
    const auto keys_size = dict_struct.key->size();
    StringRefs keys_array(keys_size);
    MapType<UInt8> has_key;
    Arena temporary_keys_pool;
    std::vector<size_t> to_load(rows);
    PODArray<StringRef> keys(rows);

    for (const auto row : ext::range(0, rows))
    {
        const StringRef key = placeKeysInPool(row, key_columns, keys_array, *dict_struct.key, temporary_keys_pool);
        keys[row] = key;
        has_key[key] = 0;
        to_load[row] = row;
    }

    auto stream = source_ptr->loadKeys(key_columns, to_load);

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto columns = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        Arena pool;

        StringRefs keys_temp(keys_size);

        const auto columns_size = columns.front()->size();

        for (const auto row_idx : ext::range(0, columns_size))
        {
            const StringRef key = placeKeysInPool(row_idx, columns, keys_temp, *dict_struct.key, pool);
            if (has_key.has(key))
            {
                has_key[key] = 1;
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
    {
        out[row] = has_key[keys[row]];
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);

    return result;
}

void ComplexKeyDirectDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attribute_name_by_index.emplace(attributes.size(), attribute.name);
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value, attribute.name));

        if (attribute.hierarchical)
            throw Exception{full_name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

template <typename T>
void ComplexKeyDirectDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
}

template <>
void ComplexKeyDirectDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    const String & string = null_value.get<String>();
    const char * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    attribute.null_values.emplace<StringRef>(string_in_arena, string.size());
}


ComplexKeyDirectDictionary::Attribute ComplexKeyDirectDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value, const std::string & attr_name)
{
    Attribute attr{type, {}, {}, attr_name};

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        createAttributeImpl<AttributeType>(attr, null_value);
    };

    callOnDictionaryAttributeType(type, type_call);

    return attr;
}

template <typename Pool>
StringRef ComplexKeyDirectDictionary::placeKeysInPool(
    const size_t row, const Columns & key_columns, StringRefs & keys, const std::vector<DictionaryAttribute> & key_attributes, Pool & pool) const
{
    const auto keys_size = key_columns.size();
    size_t sum_keys_size{};

    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->getDataAt(row);
        sum_keys_size += keys[j].size;
        if (key_attributes[j].underlying_type == AttributeUnderlyingType::utString)
            sum_keys_size += sizeof(size_t) + 1;
    }

    auto place = pool.alloc(sum_keys_size);

    auto key_start = place;
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (key_attributes[j].underlying_type == AttributeUnderlyingType::utString)
        {
            auto start = key_start;
            auto key_size = keys[j].size + 1;
            memcpy(key_start, &key_size, sizeof(size_t));
            key_start += sizeof(size_t);
            memcpy(key_start, keys[j].data, keys[j].size);
            key_start += keys[j].size;
            *key_start = '\0';
            ++key_start;
            keys[j].data = start;
            keys[j].size += sizeof(size_t) + 1;
        }
        else
        {
            memcpy(key_start, keys[j].data, keys[j].size);
            keys[j].data = key_start;
            key_start += keys[j].size;
        }
    }

    return {place, sum_keys_size};
}


template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void ComplexKeyDirectDictionary::getItemsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto rows = key_columns.front()->size();
    const auto keys_size = dict_struct.key->size();
    StringRefs keys_array(keys_size);
    MapType<OutputType> value_by_key;
    Arena temporary_keys_pool;
    std::vector<size_t> to_load(rows);
    PODArray<StringRef> keys(rows);

    for (const auto row : ext::range(0, rows))
    {
        const StringRef key = placeKeysInPool(row, key_columns, keys_array, *dict_struct.key, temporary_keys_pool);
        keys[row] = key;
        value_by_key[key] = get_default(row);
        to_load[row] = row;
    }

    auto stream = source_ptr->loadKeys(key_columns, to_load);
    const auto attributes_size = attributes.size();

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto columns = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        const auto attribute_columns = ext::map<Columns>(ext::range(0, attributes_size), [&](const size_t attribute_idx)
            {
                return block.safeGetByPosition(keys_size + attribute_idx).column;
            });
        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            if (attribute.name != attribute_name_by_index.at(attribute_idx))
            {
                continue;
            }

            const IColumn & attribute_column = *attribute_columns[attribute_idx];
            Arena pool;

            StringRefs keys_temp(keys_size);

            const auto columns_size = columns.front()->size();

            for (const auto row_idx : ext::range(0, columns_size))
            {
                const StringRef key = placeKeysInPool(row_idx, columns, keys_temp, *dict_struct.key, pool);
                if (value_by_key.has(key))
                {
                    if (attribute.type == AttributeUnderlyingType::utFloat32)
                    {
                        value_by_key[key] = static_cast<Float32>(attribute_column[row_idx].template get<Float64>());
                    }
                    else
                    {
                        value_by_key[key] = static_cast<OutputType>(attribute_column[row_idx].template get<AttributeType>());
                    }

                }
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
    {
        set_value(row, value_by_key[keys[row]]);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

const ComplexKeyDirectDictionary::Attribute & ComplexKeyDirectDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

BlockInputStreamPtr ComplexKeyDirectDictionary::getBlockInputStream(const Names & /* column_names */, size_t /* max_block_size */) const
{
    return source_ptr->loadAll();
}


void registerDictionaryComplexKeyDirect(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (!dict_struct.key)
            throw Exception{"'key' is required for dictionary of layout 'complex_key_direct'", ErrorCodes::BAD_ARGUMENTS};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        if (config.has(config_prefix + ".lifetime.min") || config.has(config_prefix + ".lifetime.max"))
            throw Exception{"'lifetime' parameter is redundant for the dictionary' of layout 'direct'", ErrorCodes::BAD_ARGUMENTS};


        return std::make_unique<ComplexKeyDirectDictionary>(dict_id, dict_struct, std::move(source_ptr));
    };
    factory.registerLayout("complex_key_direct", create_layout, true);
}


}
