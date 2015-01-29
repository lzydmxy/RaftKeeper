#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionarySourceFactory.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Common/HashTable/HashMap.h>
#include <statdaemons/ext/range.hpp>
#include <statdaemons/ext/memory.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class HashedDictionary final : public IDictionary
{
public:
	HashedDictionary(const DictionaryStructure & dict_struct, const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix, DictionarySourcePtr source_ptr)
		: source_ptr{std::move(source_ptr)}
	{
		const auto size = dict_struct.attributes.size();
		attributes.reserve(size);
		for (const auto & attribute : dict_struct.attributes)
		{
			attribute_index_by_name.emplace(attribute.name, attributes.size());
			attributes.push_back(std::move(createAttributeWithType(getAttributeTypeByName(attribute.type),
				attribute.null_value)));

			if (attribute.hierarchical)
				hierarchical_attribute = &attributes.back();
		}

		auto stream = this->source_ptr->loadAll();

		while (const auto block = stream->read())
		{
			const auto & id_column = *block.getByPosition(0).column;

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *block.getByPosition(attribute_idx + 1).column;
				auto & attribute = attributes[attribute_idx];

				for (const auto row_idx : ext::range(0, id_column.size()))
					setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
			}
		}
	}

	std::string getTypeName() const override { return "HashedDictionary"; }

	bool hasHierarchy() const override { return hierarchical_attribute; }

	id_t toParent(const id_t id) const override
	{
		const auto attr = hierarchical_attribute;

		switch (hierarchical_attribute->type)
		{
			case attribute_type::uint8:
			{
				const auto it = attr->uint8_map->find(id);
				return it != attr->uint8_map->end() ? it->second : attr->uint8_null_value;
			}
			case attribute_type::uint16:
			{
				const auto it = attr->uint16_map->find(id);
				return it != attr->uint16_map->end() ? it->second : attr->uint16_null_value;
			}
			case attribute_type::uint32:
			{
				const auto it = attr->uint32_map->find(id);
				return it != attr->uint32_map->end() ? it->second : attr->uint32_null_value;
			}
			case attribute_type::uint64:
			{
				const auto it = attr->uint64_map->find(id);
				return it != attr->uint64_map->end() ? it->second : attr->uint64_null_value;
			}
			case attribute_type::int8:
			{
				const auto it = attr->int8_map->find(id);
				return it != attr->int8_map->end() ? it->second : attr->int8_null_value;
			}
			case attribute_type::int16:
			{
				const auto it = attr->int16_map->find(id);
				return it != attr->int16_map->end() ? it->second : attr->int16_null_value;
			}
			case attribute_type::int32:
			{
				const auto it = attr->int32_map->find(id);
				return it != attr->int32_map->end() ? it->second : attr->int32_null_value;
			}
			case attribute_type::int64:
			{
				const auto it = attr->int64_map->find(id);
				return it != attr->int64_map->end() ? it->second : attr->int64_null_value;
			}
			case attribute_type::float32:
			case attribute_type::float64:
			case attribute_type::string:
				break;
		};

		throw Exception{
			"Hierarchical attribute has non-integer type " + toString(hierarchical_attribute->type),
			ErrorCodes::TYPE_MISMATCH
		};
	}

#define DECLARE_SAFE_GETTER(TYPE, NAME, LC_TYPE) \
	TYPE get##NAME(const std::string & attribute_name, const id_t id) const override\
	{\
		const auto idx = getAttributeIndex(attribute_name);\
		const auto & attribute = attributes[idx];\
		if (attribute.type != attribute_type::LC_TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		const auto it = attribute.LC_TYPE##_map->find(id);\
		if (it != attribute.LC_TYPE##_map->end())\
			return it->second;\
		return attribute.LC_TYPE##_null_value;\
	}
	DECLARE_SAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_SAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_SAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_SAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_SAFE_GETTER(Int8, Int8, int8)
	DECLARE_SAFE_GETTER(Int16, Int16, int16)
	DECLARE_SAFE_GETTER(Int32, Int32, int32)
	DECLARE_SAFE_GETTER(Int64, Int64, int64)
	DECLARE_SAFE_GETTER(Float32, Float32, float32)
	DECLARE_SAFE_GETTER(Float64, Float64, float64)
	DECLARE_SAFE_GETTER(StringRef, String, string)
#undef DECLARE_SAFE_GETTER

	std::size_t getAttributeIndex(const std::string & attribute_name) const override
	{
		const auto it = attribute_index_by_name.find(attribute_name);
		if (it == std::end(attribute_index_by_name))
			throw Exception{
				"No such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return it->second;
	}

#define DECLARE_TYPE_CHECKER(NAME, LC_NAME)\
	bool is##NAME(const std::size_t attribute_idx) const override\
	{\
		return attributes[attribute_idx].type == attribute_type::LC_NAME;\
	}
	DECLARE_TYPE_CHECKER(UInt8, uint8)
	DECLARE_TYPE_CHECKER(UInt16, uint16)
	DECLARE_TYPE_CHECKER(UInt32, uint32)
	DECLARE_TYPE_CHECKER(UInt64, uint64)
	DECLARE_TYPE_CHECKER(Int8, int8)
	DECLARE_TYPE_CHECKER(Int16, int16)
	DECLARE_TYPE_CHECKER(Int32, int32)
	DECLARE_TYPE_CHECKER(Int64, int64)
	DECLARE_TYPE_CHECKER(Float32, float32)
	DECLARE_TYPE_CHECKER(Float64, float64)
	DECLARE_TYPE_CHECKER(String, string)
#undef DECLARE_TYPE_CHECKER

#define DECLARE_UNSAFE_GETTER(TYPE, NAME, LC_NAME)\
	TYPE get##NAME##Unsafe(const std::size_t attribute_idx, const id_t id) const override\
	{\
		const auto & attribute = attributes[attribute_idx];\
		const auto it = attribute.LC_NAME##_map->find(id);\
		if (it != attribute.LC_NAME##_map->end())\
			return it->second;\
		return attribute.LC_NAME##_null_value;\
	}
	DECLARE_UNSAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_UNSAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_UNSAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_UNSAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_UNSAFE_GETTER(Int8, Int8, int8)
	DECLARE_UNSAFE_GETTER(Int16, Int16, int16)
	DECLARE_UNSAFE_GETTER(Int32, Int32, int32)
	DECLARE_UNSAFE_GETTER(Int64, Int64, int64)
	DECLARE_UNSAFE_GETTER(Float32, Float32, float32)
	DECLARE_UNSAFE_GETTER(Float64, Float64, float64)
	DECLARE_UNSAFE_GETTER(StringRef, String, string)
#undef DECLARE_UNSAFE_GETTER

	bool isComplete() const override { return true; }

	struct attribute_t
	{
		attribute_type type;
		UInt8 uint8_null_value;
		UInt16 uint16_null_value;
		UInt32 uint32_null_value;
		UInt64 uint64_null_value;
		Int8 int8_null_value;
		Int16 int16_null_value;
		Int32 int32_null_value;
		Int64 int64_null_value;
		Float32 float32_null_value;
		Float64 float64_null_value;
		String string_null_value;
		std::unique_ptr<HashMap<UInt64, UInt8>> uint8_map;
		std::unique_ptr<HashMap<UInt64, UInt16>> uint16_map;
		std::unique_ptr<HashMap<UInt64, UInt32>> uint32_map;
		std::unique_ptr<HashMap<UInt64, UInt64>> uint64_map;
		std::unique_ptr<HashMap<UInt64, Int8>> int8_map;
		std::unique_ptr<HashMap<UInt64, Int16>> int16_map;
		std::unique_ptr<HashMap<UInt64, Int32>> int32_map;
		std::unique_ptr<HashMap<UInt64, Int64>> int64_map;
		std::unique_ptr<HashMap<UInt64, Float32>> float32_map;
		std::unique_ptr<HashMap<UInt64, Float64>> float64_map;
		std::unique_ptr<Arena> string_arena;
		std::unique_ptr<HashMap<UInt64, StringRef>> string_map;
	};

	attribute_t createAttributeWithType(const attribute_type type, const std::string & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case attribute_type::uint8:
				attr.uint8_null_value = DB::parse<UInt8>(null_value);
				attr.uint8_map.reset(new HashMap<UInt64, UInt8>);
				break;
			case attribute_type::uint16:
				attr.uint16_null_value = DB::parse<UInt16>(null_value);
				attr.uint16_map.reset(new HashMap<UInt64, UInt16>);
				break;
			case attribute_type::uint32:
				attr.uint32_null_value = DB::parse<UInt32>(null_value);
				attr.uint32_map.reset(new HashMap<UInt64, UInt32>);
				break;
			case attribute_type::uint64:
				attr.uint64_null_value = DB::parse<UInt64>(null_value);
				attr.uint64_map.reset(new HashMap<UInt64, UInt64>);
				break;
			case attribute_type::int8:
				attr.int8_null_value = DB::parse<Int8>(null_value);
				attr.int8_map.reset(new HashMap<UInt64, Int8>);
				break;
			case attribute_type::int16:
				attr.int16_null_value = DB::parse<Int16>(null_value);
				attr.int16_map.reset(new HashMap<UInt64, Int16>);
				break;
			case attribute_type::int32:
				attr.int32_null_value = DB::parse<Int32>(null_value);
				attr.int32_map.reset(new HashMap<UInt64, Int32>);
				break;
			case attribute_type::int64:
				attr.int64_null_value = DB::parse<Int64>(null_value);
				attr.int64_map.reset(new HashMap<UInt64, Int64>);
				break;
			case attribute_type::float32:
				attr.float32_null_value = DB::parse<Float32>(null_value);
				attr.float32_map.reset(new HashMap<UInt64, Float32>);
				break;
			case attribute_type::float64:
				attr.float64_null_value = DB::parse<Float64>(null_value);
				attr.float64_map.reset(new HashMap<UInt64, Float64>);
				break;
			case attribute_type::string:
				attr.string_null_value = null_value;
				attr.string_arena.reset(new Arena);
				attr.string_map.reset(new HashMap<UInt64, StringRef>);
				break;
		}

		return attr;
	}

	void setAttributeValue(attribute_t & attribute, const id_t id, const Field & value)
	{
		switch (attribute.type)
		{
			case attribute_type::uint8:
			{
				attribute.uint8_map->insert({ id, value.get<UInt64>() });
				break;
			}
			case attribute_type::uint16:
			{
				attribute.uint16_map->insert({ id, value.get<UInt64>() });
				break;
			}
			case attribute_type::uint32:
			{
				attribute.uint32_map->insert({ id, value.get<UInt64>() });
				break;
			}
			case attribute_type::uint64:
			{
				attribute.uint64_map->insert({ id, value.get<UInt64>() });
				break;
			}
			case attribute_type::int8:
			{
				attribute.int8_map->insert({ id, value.get<Int64>() });
				break;
			}
			case attribute_type::int16:
			{
				attribute.int16_map->insert({ id, value.get<Int64>() });
				break;
			}
			case attribute_type::int32:
			{
				attribute.int32_map->insert({ id, value.get<Int64>() });
				break;
			}
			case attribute_type::int64:
			{
				attribute.int64_map->insert({ id, value.get<Int64>() });
				break;
			}
			case attribute_type::float32:
			{
				attribute.float32_map->insert({ id, value.get<Float64>() });
				break;
			}
			case attribute_type::float64:
			{
				attribute.float64_map->insert({ id, value.get<Float64>() });
				break;
			}
			case attribute_type::string:
			{
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				attribute.string_map->insert({ id, StringRef{string_in_arena, string.size()} });
				break;
			}
		};
	}

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	const attribute_t * hierarchical_attribute = nullptr;

	DictionarySourcePtr source_ptr;
};

}
