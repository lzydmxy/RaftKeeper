# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Data
    contrib/libs/poco/Data/ODBC
    contrib/libs/poco/MongoDB
    contrib/libs/poco/Redis
    contrib/libs/sparsehash
)

NO_COMPILER_WARNINGS()

SRCS(
    CacheDictionary.cpp
    CacheDictionary_generate1.cpp
    CacheDictionary_generate2.cpp
    CacheDictionary_generate3.cpp
    CassandraBlockInputStream.cpp
    CassandraDictionarySource.cpp
    CassandraHelpers.cpp
    ClickHouseDictionarySource.cpp
    ComplexKeyCacheDictionary.cpp
    ComplexKeyCacheDictionary_createAttributeWithType.cpp
    ComplexKeyCacheDictionary_generate1.cpp
    ComplexKeyCacheDictionary_generate2.cpp
    ComplexKeyCacheDictionary_generate3.cpp
    ComplexKeyCacheDictionary_setAttributeValue.cpp
    ComplexKeyCacheDictionary_setDefaultAttributeValue.cpp
    ComplexKeyDirectDictionary.cpp
    ComplexKeyHashedDictionary.cpp
    DictionaryBlockInputStreamBase.cpp
    DictionaryFactory.cpp
    DictionarySourceFactory.cpp
    DictionarySourceHelpers.cpp
    DictionaryStructure.cpp
    DirectDictionary.cpp
    Embedded/GeodataProviders/HierarchiesProvider.cpp
    Embedded/GeodataProviders/HierarchyFormatReader.cpp
    Embedded/GeodataProviders/NamesFormatReader.cpp
    Embedded/GeodataProviders/NamesProvider.cpp
    Embedded/GeoDictionariesLoader.cpp
    Embedded/RegionsHierarchies.cpp
    Embedded/RegionsHierarchy.cpp
    Embedded/RegionsNames.cpp
    ExecutableDictionarySource.cpp
    ExternalQueryBuilder.cpp
    FileDictionarySource.cpp
    FlatDictionary.cpp
    getDictionaryConfigurationFromAST.cpp
    HashedDictionary.cpp
    HTTPDictionarySource.cpp
    LibraryDictionarySource.cpp
    LibraryDictionarySourceExternal.cpp
    MongoDBBlockInputStream.cpp
    MongoDBDictionarySource.cpp
    MySQLDictionarySource.cpp
    PolygonDictionary.cpp
    RangeHashedDictionary.cpp
    readInvalidateQuery.cpp
    RedisBlockInputStream.cpp
    RedisDictionarySource.cpp
    registerDictionaries.cpp
    writeParenthesisedString.cpp
    XDBCDictionarySource.cpp

)

END()
