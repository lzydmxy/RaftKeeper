#include "SSDCacheDictionary.h"

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <DataStreams/IBlockInputStream.h>
#include "DictionaryFactory.h"
#include <IO/WriteHelpers.h>
#include <ext/chrono_io.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>

namespace ProfileEvents
{
    extern const Event DictCacheKeysRequested;
    extern const Event DictCacheKeysRequestedMiss;
    extern const Event DictCacheKeysRequestedFound;
    extern const Event DictCacheKeysExpired;
    extern const Event DictCacheKeysNotFound;
    extern const Event DictCacheKeysHit;
    extern const Event DictCacheRequestTimeNs;
    extern const Event DictCacheRequests;
    extern const Event DictCacheLockWriteNs;
    extern const Event DictCacheLockReadNs;
}

namespace CurrentMetrics
{
    extern const Metric DictCacheRequests;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_SMALL_BUFFER_SIZE;
}

namespace
{
    constexpr size_t INMEMORY = (1ULL << 63ULL);
    const std::string BIN_FILE_EXT = ".bin";
    const std::string IND_FILE_EXT = ".idx";
}

CachePartition::CachePartition(
        const AttributeUnderlyingType & /* key_structure */, const std::vector<AttributeUnderlyingType> & attributes_structure,
        const std::string & dir_path, const size_t file_id_, const size_t max_size_, const size_t buffer_size_)
    : file_id(file_id_), max_size(max_size_), buffer_size(buffer_size_), path(dir_path + "/" + std::to_string(file_id))
{
    keys_buffer.type = AttributeUnderlyingType::utUInt64;
    keys_buffer.values = std::vector<UInt64>();
    for (const auto & type : attributes_structure)
    {
        switch (type)
        {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
        attributes_buffer.emplace_back(); \
        attributes_buffer.back().type = type; \
        attributes_buffer.back().values = std::vector<TYPE>(); \
        break;

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
#undef DISPATCH

        case AttributeUnderlyingType::utString:
            // TODO: string support
            break;
        }
    }
}

void CachePartition::appendBlock(const Attribute & new_keys, const Attributes & new_attributes)
{
    if (new_attributes.size() != attributes_buffer.size())
        throw Exception{"Wrong columns number in block.", ErrorCodes::BAD_ARGUMENTS};

    const auto & ids = std::get<Attribute::Container<UInt64>>(new_keys.values);

    size_t start_size = ids.size();

    appendValuesToBufferAttribute(keys_buffer, new_keys);
    for (size_t i = 0; i < attributes_buffer.size(); ++i)
    {
        appendValuesToBufferAttribute(attributes_buffer[i], new_attributes[i]);
        //bytes += buffer[i]->byteSize();
    }

    for (size_t i = 0; i < ids.size(); ++i)
    {
        key_to_file_offset[ids[i]] = (start_size + i) | INMEMORY;
    }

    //if (bytes >= buffer_size)
    {
        //flush();
    }
}

void CachePartition::appendValuesToBufferAttribute(Attribute & to, const Attribute & from)
{
    switch (to.type)
    {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
        { \
            auto &to_values = std::get<Attribute::Container<TYPE>>(to.values); \
            auto &from_values = std::get<Attribute::Container<TYPE>>(from.values); \
            size_t prev_size = to_values.size(); \
            to_values.resize(to_values.size() + from_values.size()); \
            memcpy(to_values.data() + prev_size * sizeof(TYPE), from_values.data(), from_values.size() * sizeof(TYPE)); \
        } \
        break;

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
#undef DISPATCH

    case AttributeUnderlyingType::utString:
            // TODO: string support
            break;
    }
}

void CachePartition::flush()
{
    if (!write_data_buffer)
    {
        write_data_buffer = std::make_unique<WriteBufferAIO>(path + BIN_FILE_EXT, buffer_size, O_RDWR | O_CREAT | O_TRUNC);
        // TODO: не перетирать + seek в конец файла
    }

    const auto & ids = std::get<Attribute::Container<UInt64>>(keys_buffer.values);

    std::vector<size_t> offsets;

    size_t prev_size = 0;
    for (size_t row = 0; row < ids.size(); ++row)
    {
        offsets.push_back((offsets.empty() ? write_data_buffer->getPositionInFile() : offsets.back()) + prev_size);
        prev_size = 0;

        for (size_t col = 0; col < attributes_buffer.size(); ++col)
        {
            const auto & attribute = attributes_buffer[col];

            switch (attribute.type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                { \
                    const auto & values = std::get<Attribute::Container<TYPE>>(attribute.values); \
                    writeBinary(values[row], *static_cast<WriteBuffer*>(write_data_buffer.get())); \
                } \
                break;

                DISPATCH(UInt8)
                DISPATCH(UInt16)
                DISPATCH(UInt32)
                DISPATCH(UInt64)
                DISPATCH(UInt128)
                DISPATCH(Int8)
                DISPATCH(Int16)
                DISPATCH(Int32)
                DISPATCH(Int64)
                DISPATCH(Decimal32)
                DISPATCH(Decimal64)
                DISPATCH(Decimal128)
                DISPATCH(Float32)
                DISPATCH(Float64)
#undef DISPATCH

            case AttributeUnderlyingType::utString:
                // TODO: string support
                break;
            }
        }
    }
    write_data_buffer->sync();

    /// commit changes in index
    for (size_t row = 0; row < ids.size(); ++row)
        key_to_file_offset[ids[row]] = offsets[row];

    /// clear buffer
    std::visit([](auto & attr) { attr.clear(); }, keys_buffer.values);
    for (auto & attribute : attributes_buffer)
        std::visit([](auto & attr) { attr.clear(); }, attribute.values);
}

template <typename Out, typename Key>
void CachePartition::getValue(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
              ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const
{
    UNUSED(attribute_index);
    UNUSED(out);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        not_found[ids[i]].push_back(i);
    }
}

void CachePartition::has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out) const
{
    UNUSED(ids);
    UNUSED(out);
}

CacheStorage::CacheStorage(SSDCacheDictionary & dictionary_, const std::string & path_, const size_t partitions_count_, const size_t partition_max_size_)
    : dictionary(dictionary_)
    , path(path_)
    , partition_max_size(partition_max_size_)
    , log(&Poco::Logger::get("CacheStorage"))
{
    std::vector<AttributeUnderlyingType> structure;
    for (const auto & item : dictionary.getStructure().attributes)
    {
        structure.push_back(item.underlying_type);
    }
    for (size_t partition_id = 0; partition_id < partitions_count_; ++partition_id)
        partitions.emplace_back(std::make_unique<CachePartition>(AttributeUnderlyingType::utUInt64, structure, path_, partition_id, partition_max_size));
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheStorage::update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
        PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_ids.size());

    std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
    for (const auto id : requested_ids)
        remaining_ids.insert({id, 0});

    const auto now = std::chrono::system_clock::now();

    const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    if (now > backoff_end_time)
    {
        try
        {
            if (update_error_count)
            {
                /// Recover after error: we have to clone the source here because
                /// it could keep connections which should be reset after error.
                source_ptr = source_ptr->clone();
            }

            Stopwatch watch;
            auto stream = source_ptr->loadIds(requested_ids);
            stream->readPrefix();

            while (const auto block = stream->read())
            {
                const auto new_keys = createAttributesFromBlock(block, { AttributeUnderlyingType::utUInt64 }).front();
                const auto new_attributes = createAttributesFromBlock(
                        block, ext::map<std::vector>(dictionary.getAttributes(), [](const auto & attribute) { return attribute.type; }));
                const auto & ids = std::get<CachePartition::Attribute::Container<UInt64>>(new_keys.values);

                for (const auto i : ext::range(0, ids.size()))
                {
                    /// mark corresponding id as found
                    on_updated(ids[i], i, new_attributes);
                    remaining_ids[ids[i]] = 1;
                }

                /// TODO: Add TTL to block
                partitions[0]->appendBlock(new_keys, new_attributes);
            }

            stream->readSuffix();

            update_error_count = 0;
            last_update_exception = std::exception_ptr{};
            backoff_end_time = std::chrono::system_clock::time_point{};

            ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
        }
        catch (...)
        {
            ++update_error_count;
            last_update_exception = std::current_exception();
            backoff_end_time = now + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, update_error_count));

            tryLogException(last_update_exception, log, "Could not update cache dictionary '" + dictionary.getName() +
                                                 "', next update is scheduled at " + ext::to_string(backoff_end_time));
        }
    }

    size_t not_found_num = 0, found_num = 0;

    /// Check which ids have not been found and require setting null_value
    CachePartition::Attribute new_keys;
    new_keys.type = AttributeUnderlyingType::utUInt64;
    new_keys.values = std::vector<UInt64>();
    CachePartition::Attributes new_attributes;
    {
        /// TODO: create attributes from structure
        for (const auto & attribute : dictionary.getAttributes())
        {
            switch (attribute.type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                new_attributes.emplace_back(); \
                new_attributes.back().type = attribute.type; \
                new_attributes.back().values = std::vector<TYPE>(); \
                break;

                DISPATCH(UInt8)
                DISPATCH(UInt16)
                DISPATCH(UInt32)
                DISPATCH(UInt64)
                DISPATCH(UInt128)
                DISPATCH(Int8)
                DISPATCH(Int16)
                DISPATCH(Int32)
                DISPATCH(Int64)
                DISPATCH(Decimal32)
                DISPATCH(Decimal64)
                DISPATCH(Decimal128)
                DISPATCH(Float32)
                DISPATCH(Float64)
#undef DISPATCH

            case AttributeUnderlyingType::utString:
                // TODO: string support
                break;
            }
        }
    }
    for (const auto & id_found_pair : remaining_ids)
    {
        if (id_found_pair.second)
        {
            ++found_num;
            continue;
        }
        ++not_found_num;

        const auto id = id_found_pair.first;

        if (update_error_count)
        {
            /// TODO: юзать старые значения.

            /// We don't have expired data for that `id` so all we can do is to rethrow `last_exception`.
            std::rethrow_exception(last_update_exception);
        }

        /// TODO: Add TTL

        // Set key
        std::get<std::vector<UInt64>>(new_keys.values).push_back(id);

        /// Set null_value for each attribute
        const auto & attributes = dictionary.getAttributes();
        for (size_t i = 0; i < attributes.size(); ++i)
        {
            const auto & attribute = attributes[i];
            // append null
            switch (attribute.type)
            {
#define DISPATCH(TYPE) \
            case AttributeUnderlyingType::ut##TYPE: \
                { \
                    auto & to_values = std::get<std::vector<TYPE>>(new_attributes[i].values); \
                    auto & null_value = std::get<TYPE>(attribute.null_value); \
                    to_values.push_back(null_value); \
                } \
                break;

                DISPATCH(UInt8)
                DISPATCH(UInt16)
                DISPATCH(UInt32)
                DISPATCH(UInt64)
                DISPATCH(UInt128)
                DISPATCH(Int8)
                DISPATCH(Int16)
                DISPATCH(Int32)
                DISPATCH(Int64)
                DISPATCH(Decimal32)
                DISPATCH(Decimal64)
                DISPATCH(Decimal128)
                DISPATCH(Float32)
                DISPATCH(Float64)
#undef DISPATCH

            case AttributeUnderlyingType::utString:
                // TODO: string support
                break;
            }
        }

        /// inform caller that the cell has not been found
        on_id_not_found(id);
    }
    partitions[0]->appendBlock(new_keys, new_attributes);

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}

CachePartition::Attributes CacheStorage::createAttributesFromBlock(
        const Block & block, const std::vector<AttributeUnderlyingType> & structure)
{
    CachePartition::Attributes attributes;

    const auto columns = block.getColumns();
    for (size_t i = 0; i < structure.size(); ++i)
    {
        const auto & column = columns[i];
        switch (structure[i])
        {
#define DISPATCH(TYPE) \
        case AttributeUnderlyingType::ut##TYPE: \
            { \
                std::vector<TYPE> values(column->size()); \
                const auto raw_data = column->getRawData(); \
                memcpy(values.data(), raw_data.data, raw_data.size); \
                attributes.emplace_back(); \
                attributes.back().type = structure[i]; \
                attributes.back().values = std::move(values); \
            } \
            break;

            DISPATCH(UInt8)
            DISPATCH(UInt16)
            DISPATCH(UInt32)
            DISPATCH(UInt64)
            DISPATCH(UInt128)
            DISPATCH(Int8)
            DISPATCH(Int16)
            DISPATCH(Int32)
            DISPATCH(Int64)
            DISPATCH(Decimal32)
            DISPATCH(Decimal64)
            DISPATCH(Decimal128)
            DISPATCH(Float32)
            DISPATCH(Float64)
#undef DISPATCH

        case AttributeUnderlyingType::utString:
            // TODO: string support
            break;
        }
    }

    return attributes;
}

SSDCacheDictionary::SSDCacheDictionary(
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    const std::string & path_,
    const size_t partition_max_size_)
    : name(name_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , path(path_)
    , partition_max_size(partition_max_size_)
    , storage(*this, path, 1, partition_max_size)
    , log(&Poco::Logger::get("SSDCacheDictionary"))
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        const auto null_value = std::get<TYPE>(attributes[index].null_value); \
        getItemsNumberImpl<TYPE, TYPE>( \
                index, \
                ids, \
                out, \
                [&](const size_t) { return null_value; }); \
    }

    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>( \
            index, \
            ids, \
            out, \
            [&](const size_t row) { return def[row]; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
        getItemsNumberImpl<TYPE, TYPE>( \
            index, \
            ids, \
            out, \
            [&](const size_t) { return def; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void SSDCacheDictionary::getItemsNumberImpl(
        const size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    std::unordered_map<Key, std::vector<size_t>> not_found_ids;
    storage.getValue<OutputType>(attribute_index, ids, out, not_found_ids);
    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](const auto & pair) { return pair.first; });

    storage.update(
            source_ptr,
            required_ids,
            [&](const auto id, const auto row, const auto & new_attributes) {
                Poco::Logger::get("update:").information(std::to_string(id) + " " + std::to_string(row));
                for (const size_t out_row : not_found_ids[id])
                    out[out_row] = std::get<std::vector<OutputType>>(new_attributes[attribute_index].values)[row];
            },
            [&](const size_t id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = get_default(row);
            });
}

void SSDCacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, attributes[index].type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(attributes[index].null_value)};

    getItemsString(index, ids, out, [&](const size_t) { return null_value; });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, attributes[index].type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    const auto index = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, attributes[index].type, AttributeUnderlyingType::utString);

    getItemsString(index, ids, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void SSDCacheDictionary::getItemsString(const size_t attribute_index, const PaddedPODArray<Key> & ids,
        ColumnString * out, DefaultGetter && get_default) const
{
    UNUSED(attribute_index);
    UNUSED(ids);
    UNUSED(out);
    UNUSED(get_default);
}

size_t SSDCacheDictionary::getAttributeIndex(const std::string & attr_name) const
{
    auto it = attribute_index_by_name.find(attr_name);
    if (it == std::end(attribute_index_by_name))
        throw  Exception{"Attribute `" + name + "` does not exist.", ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

SSDCacheDictionary::Attribute & SSDCacheDictionary::getAttribute(const std::string & attr_name)
{
    return attributes[getAttributeIndex(attr_name)];
}

const SSDCacheDictionary::Attribute & SSDCacheDictionary::getAttribute(const std::string & attr_name) const
{
    return attributes[getAttributeIndex(attr_name)];
}

const SSDCacheDictionary::Attributes & SSDCacheDictionary::getAttributes() const
{
    return attributes;
}

template <typename T>
SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithTypeImpl(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}};
    attr.null_value = static_cast<T>(null_value.get<NearestFieldType<T>>());
    bytes_allocated += sizeof(T);
    return attr;
}

template <>
SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithTypeImpl<String>(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}};
    attr.null_value = null_value.get<String>();
    bytes_allocated += sizeof(StringRef);
    //if (!string_arena)
    //    string_arena = std::make_unique<ArenaWithFreeLists>();
    return attr;
}

SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
case AttributeUnderlyingType::ut##TYPE: \
    return createAttributeWithTypeImpl<TYPE>(type, null_value);

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
        DISPATCH(String)
#undef DISPATCH
    }
    throw Exception{"Unknown attribute type: " + std::to_string(static_cast<int>(type)), ErrorCodes::TYPE_MISMATCH};
}

void SSDCacheDictionary::createAttributes()
{
    attributes.reserve(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        const auto & attribute = dict_struct.attributes[i];

        attribute_index_by_name.emplace(attribute.name, i);
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void registerDictionarySSDCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'cache'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";
        const auto max_partition_size = config.getInt(layout_prefix + ".ssd.max_partition_size");
        if (max_partition_size == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have 0 cells", ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        const auto path = config.getString(layout_prefix + ".ssd.path");
        if (path.empty())
            throw Exception{name + ": dictionary of layout 'cache' cannot have empty path",
                            ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SSDCacheDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, path, max_partition_size);
    };
    factory.registerLayout("ssd", create_layout, false);
}

}
