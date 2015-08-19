#include <DB/Dictionaries/DictionaryFactory.h>
#include <DB/Dictionaries/DictionarySourceFactory.h>
#include <DB/Dictionaries/FlatDictionary.h>
#include <DB/Dictionaries/HashedDictionary.h>
#include <DB/Dictionaries/CacheDictionary.h>
#include <DB/Dictionaries/RangeHashedDictionary.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <memory>


namespace DB
{


DictionaryPtr DictionaryFactory::create(const std::string & name, Poco::Util::AbstractConfiguration & config,
	const std::string & config_prefix, Context & context) const
{
	Poco::Util::AbstractConfiguration::Keys keys;
	const auto & layout_prefix = config_prefix + ".layout";
	config.keys(layout_prefix, keys);
	if (keys.size() != 1)
		throw Exception{
			name + ": element dictionary.layout should have exactly one child element",
			ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG
		};

	const DictionaryStructure dict_struct{config, config_prefix + ".structure"};

	auto source_ptr = DictionarySourceFactory::instance().create(
		name, config, config_prefix + ".source", dict_struct, context);

	const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

	const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

	const auto & layout_type = keys.front();

	if ("range_hashed" == layout_type)
	{
		if (dict_struct.range_min.empty() || dict_struct.range_min.empty())
			throw Exception{
				name + ": dictionary of layout 'range_hashed' requires .structure.range_min and .structure.range_max",
				ErrorCodes::BAD_ARGUMENTS
			};

		return std::make_unique<RangeHashedDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
	}
	else
	{
		if (!dict_struct.range_min.empty() || !dict_struct.range_min.empty())
			throw Exception{
				name + ": elements .structure.range_min and .structure.range_max should be defined only "
					"for a dictionary of layout 'range_hashed'",
				ErrorCodes::BAD_ARGUMENTS
			};

		if ("flat" == layout_type)
		{
			return std::make_unique<FlatDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
		}
		else if ("hashed" == layout_type)
		{
			return std::make_unique<HashedDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
		}
		else if ("cache" == layout_type)
		{
			const auto size = config.getInt(layout_prefix + ".cache.size_in_cells");
			if (size == 0)
				throw Exception{
					name + ": dictionary of layout 'cache' cannot have 0 cells",
					ErrorCodes::TOO_SMALL_BUFFER_SIZE
				};

			if (require_nonempty)
				throw Exception{
					name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
					ErrorCodes::BAD_ARGUMENTS
				};

			return std::make_unique<CacheDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, size);
		}
	}

	throw Exception{
		name + ": unknown dictionary layout type: " + layout_type,
		ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG
	};
};


}
