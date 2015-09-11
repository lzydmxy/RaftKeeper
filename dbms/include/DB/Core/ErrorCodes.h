#pragma once


namespace DB
{

namespace ErrorCodes
{
	enum ErrorCodes
	{
		UNSUPPORTED_METHOD = 1,
		UNSUPPORTED_PARAMETER = 2,
		UNEXPECTED_END_OF_FILE = 3,
		EXPECTED_END_OF_FILE = 4,
		CANNOT_PARSE_TEXT = 6,
		INCORRECT_NUMBER_OF_COLUMNS = 7,
		THERE_IS_NO_COLUMN = 8,
		SIZES_OF_COLUMNS_DOESNT_MATCH = 9,
		NOT_FOUND_COLUMN_IN_BLOCK = 10,
		POSITION_OUT_OF_BOUND = 11,
		PARAMETER_OUT_OF_BOUND = 12,
		SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH = 13,
		EMPTY_TUPLE = 14,
		DUPLICATE_COLUMN = 15,
		NO_SUCH_COLUMN_IN_TABLE = 16,
		DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH = 17,
		CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN = 18,
		SIZE_OF_ARRAY_DOESNT_MATCH_SIZE_OF_FIXEDARRAY_COLUMN = 19,
		NUMBER_OF_COLUMNS_DOESNT_MATCH = 20,
		CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT = 21,
		CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT = 22,
		CANNOT_READ_FROM_ISTREAM = 23,
		CANNOT_WRITE_TO_OSTREAM = 24,
		CANNOT_PARSE_ESCAPE_SEQUENCE = 25,
		CANNOT_PARSE_QUOTED_STRING = 26,
		CANNOT_PARSE_INPUT_ASSERTION_FAILED = 27,
		CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER = 28,
		CANNOT_PRINT_INTEGER = 29,
		CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK = 30,
		CANNOT_READ_COMPRESSED_CHUNK = 31,
		ATTEMPT_TO_READ_AFTER_EOF = 32,
		CANNOT_READ_ALL_DATA = 33,
		TOO_MUCH_ARGUMENTS_FOR_FUNCTION = 34,
		TOO_LESS_ARGUMENTS_FOR_FUNCTION = 35,
		BAD_ARGUMENTS = 36,
		UNKNOWN_ELEMENT_IN_AST = 37,
		CANNOT_PARSE_DATE = 38,
		TOO_LARGE_SIZE_COMPRESSED = 39,
		CHECKSUM_DOESNT_MATCH = 40,
		CANNOT_PARSE_DATETIME = 41,
		NUMBER_OF_ARGUMENTS_DOESNT_MATCH = 42,
		ILLEGAL_TYPE_OF_ARGUMENT = 43,
		ILLEGAL_COLUMN = 44,
		ILLEGAL_NUMBER_OF_RESULT_COLUMNS = 45,
		UNKNOWN_FUNCTION = 46,
		UNKNOWN_IDENTIFIER = 47,
		NOT_IMPLEMENTED = 48,
		LOGICAL_ERROR = 49,
		UNKNOWN_TYPE = 50,
		EMPTY_LIST_OF_COLUMNS_QUERIED = 51,
		COLUMN_QUERIED_MORE_THAN_ONCE = 52,
		TYPE_MISMATCH = 53,
		STORAGE_DOESNT_ALLOW_PARAMETERS = 54,
		STORAGE_REQUIRES_PARAMETER = 55,
		UNKNOWN_STORAGE = 56,
		TABLE_ALREADY_EXISTS = 57,
		TABLE_METADATA_ALREADY_EXISTS = 58,
		ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER = 59,
		UNKNOWN_TABLE = 60,
		ONLY_FILTER_COLUMN_IN_BLOCK = 61,
		SYNTAX_ERROR = 62,
		UNKNOWN_AGGREGATE_FUNCTION = 63,
		CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT = 64,
		CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT = 65,
		NOT_A_COLUMN = 66,
		ILLEGAL_KEY_OF_AGGREGATION = 67,
		CANNOT_GET_SIZE_OF_FIELD = 68,
		ARGUMENT_OUT_OF_BOUND = 69,
		CANNOT_CONVERT_TYPE = 70,
		CANNOT_WRITE_AFTER_END_OF_BUFFER = 71,
		CANNOT_PARSE_NUMBER = 72,
		UNKNOWN_FORMAT = 73,
		CANNOT_READ_FROM_FILE_DESCRIPTOR = 74,
		CANNOT_WRITE_TO_FILE_DESCRIPTOR = 75,
		CANNOT_OPEN_FILE = 76,
		CANNOT_CLOSE_FILE = 77,
		UNKNOWN_TYPE_OF_QUERY = 78,
		INCORRECT_FILE_NAME = 79,
		INCORRECT_QUERY = 80,
		UNKNOWN_DATABASE = 81,
		DATABASE_ALREADY_EXISTS = 82,
		DIRECTORY_DOESNT_EXIST = 83,
		DIRECTORY_ALREADY_EXISTS = 84,
		FORMAT_IS_NOT_SUITABLE_FOR_INPUT = 85,
		RECEIVED_ERROR_FROM_REMOTE_IO_SERVER = 86,
		CANNOT_SEEK_THROUGH_FILE = 87,
		CANNOT_TRUNCATE_FILE = 88,
		UNKNOWN_COMPRESSION_METHOD = 89,
		EMPTY_LIST_OF_COLUMNS_PASSED = 90,
		SIZES_OF_MARKS_FILES_ARE_INCONSISTENT = 91,
		EMPTY_DATA_PASSED = 92,
		UNKNOWN_AGGREGATED_DATA_VARIANT = 93,
		CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS = 94,
		CANNOT_READ_FROM_SOCKET = 95,
		CANNOT_WRITE_TO_SOCKET = 96,
		CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT = 97,
		CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM = 98,
		UNKNOWN_PACKET_FROM_CLIENT = 99,
		UNKNOWN_PACKET_FROM_SERVER = 100,
		UNEXPECTED_PACKET_FROM_CLIENT = 101,
		UNEXPECTED_PACKET_FROM_SERVER = 102,
		RECEIVED_DATA_FOR_WRONG_QUERY_ID = 103,
		TOO_SMALL_BUFFER_SIZE = 104,
		CANNOT_READ_HISTORY = 105,
		CANNOT_APPEND_HISTORY = 106,
		FILE_DOESNT_EXIST = 107,
		NO_DATA_TO_INSERT = 108,
		CANNOT_BLOCK_SIGNAL = 109,
		CANNOT_UNBLOCK_SIGNAL = 110,
		CANNOT_MANIPULATE_SIGSET = 111,
		CANNOT_WAIT_FOR_SIGNAL = 112,
		THERE_IS_NO_SESSION = 113,
		CANNOT_CLOCK_GETTIME = 114,
		UNKNOWN_SETTING = 115,
		THERE_IS_NO_DEFAULT_VALUE = 116,
		INCORRECT_DATA = 117,
		TABLE_METADATA_DOESNT_EXIST = 118,
		ENGINE_REQUIRED = 119,
		CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE = 120,
		UNKNOWN_SET_DATA_VARIANT = 121,
		INCOMPATIBLE_COLUMNS = 122,
		UNKNOWN_TYPE_OF_AST_NODE = 123,
		INCORRECT_ELEMENT_OF_SET = 124,
		INCORRECT_RESULT_OF_SCALAR_SUBQUERY = 125,
		CANNOT_GET_RETURN_TYPE = 126,
		ILLEGAL_INDEX = 127,
		TOO_LARGE_ARRAY_SIZE = 128,
		FUNCTION_IS_SPECIAL = 129,
		CANNOT_READ_ARRAY_FROM_TEXT = 130,
		TOO_LARGE_STRING_SIZE = 131,
		CANNOT_CREATE_TABLE_FROM_METADATA = 132,
		AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS = 133,
		PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS = 134,
		ZERO_ARRAY_OR_TUPLE_INDEX = 135,
		UNKNOWN_ELEMENT_IN_CONFIG = 137,
		EXCESSIVE_ELEMENT_IN_CONFIG = 138,
		NO_ELEMENTS_IN_CONFIG = 139,
		ALL_REQUESTED_COLUMNS_ARE_MISSING = 140,
		SAMPLING_NOT_SUPPORTED = 141,
		NOT_FOUND_NODE = 142,
		FOUND_MORE_THAN_ONE_NODE = 143,
		FIRST_DATE_IS_BIGGER_THAN_LAST_DATE = 144,
		UNKNOWN_OVERFLOW_MODE = 145,
		QUERY_SECTION_DOESNT_MAKE_SENSE = 146,
		NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE = 147,
		NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION = 148,
		NOT_FOUND_RHS_ELEMENT_FOR_CONDITION = 149,
		NO_ATTRIBUTES_LISTED = 150,
		INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE = 151,
		UNKNOWN_DIRECTION_OF_SORTING = 152,
		ILLEGAL_DIVISION = 153,
		AGGREGATE_FUNCTION_NOT_APPLICABLE = 154,
		UNKNOWN_RELATION = 155,
		DICTIONARIES_WAS_NOT_LOADED = 156,
		ILLEGAL_OVERFLOW_MODE = 157,
		TOO_MUCH_ROWS = 158,
		TIMEOUT_EXCEEDED = 159,
		TOO_SLOW = 160,
		TOO_MUCH_COLUMNS = 161,
		TOO_DEEP_SUBQUERIES = 162,
		TOO_DEEP_PIPELINE = 163,
		READONLY = 164,
		TOO_MUCH_TEMPORARY_COLUMNS = 165,
		TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS = 166,
		TOO_DEEP_AST = 167,
		TOO_BIG_AST = 168,
		BAD_TYPE_OF_FIELD = 169,
		BAD_GET = 170,
		BLOCKS_HAS_DIFFERENT_STRUCTURE = 171,
		CANNOT_CREATE_DIRECTORY = 172,
		CANNOT_ALLOCATE_MEMORY = 173,
		CYCLIC_ALIASES = 174,
		CHUNK_NOT_FOUND = 176,
		DUPLICATE_CHUNK_NAME = 177,
		MULTIPLE_ALIASES_FOR_EXPRESSION = 178,
		MULTIPLE_EXPRESSIONS_FOR_ALIAS = 179,
		THERE_IS_NO_PROFILE = 180,
		ILLEGAL_FINAL = 181,
		ILLEGAL_PREWHERE = 182,
		UNEXPECTED_EXPRESSION = 183,
		ILLEGAL_AGGREGATION = 184,
		UNSUPPORTED_MYISAM_BLOCK_TYPE = 185,
		UNSUPPORTED_COLLATION_LOCALE = 186,
		COLLATION_COMPARISON_FAILED = 187,
		UNKNOWN_ACTION = 188,
		TABLE_MUST_NOT_BE_CREATED_MANUALLY = 189,
		SIZES_OF_ARRAYS_DOESNT_MATCH = 190,
		SET_SIZE_LIMIT_EXCEEDED = 191,
		UNKNOWN_USER = 192,
		WRONG_PASSWORD = 193,
		REQUIRED_PASSWORD = 194,
		IP_ADDRESS_NOT_ALLOWED = 195,
		UNKNOWN_ADDRESS_PATTERN_TYPE = 196,
		SERVER_REVISION_IS_TOO_OLD = 197,
		DNS_ERROR = 198,
		UNKNOWN_QUOTA = 199,
		QUOTA_DOESNT_ALLOW_KEYS = 200,
		QUOTA_EXPIRED = 201,
		TOO_MUCH_SIMULTANEOUS_QUERIES = 202,
		NO_FREE_CONNECTION = 203,
		CANNOT_FSYNC = 204,
		NESTED_TYPE_TOO_DEEP = 205,
		ALIAS_REQUIRED = 206,
		AMBIGUOUS_IDENTIFIER = 207,
		EMPTY_NESTED_TABLE = 208,
		SOCKET_TIMEOUT = 209,
		NETWORK_ERROR = 210,
		EMPTY_QUERY = 211,
		UNKNOWN_LOAD_BALANCING = 212,
		UNKNOWN_TOTALS_MODE = 213,
		CANNOT_STATVFS = 214,
		NOT_AN_AGGREGATE = 215,
		QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING = 216,
		CLIENT_HAS_CONNECTED_TO_WRONG_PORT = 217,
		TABLE_IS_DROPPED = 218,
		DATABASE_NOT_EMPTY = 219,
		DUPLICATE_INTERSERVER_IO_ENDPOINT = 220,
		NO_SUCH_INTERSERVER_IO_ENDPOINT = 221,
		ADDING_REPLICA_TO_NON_EMPTY_TABLE = 222,
		UNEXPECTED_AST_STRUCTURE = 223,
		REPLICA_IS_ALREADY_ACTIVE = 224,
		NO_ZOOKEEPER = 225,
		NO_FILE_IN_DATA_PART = 226,
		UNEXPECTED_FILE_IN_DATA_PART = 227,
		BAD_SIZE_OF_FILE_IN_DATA_PART = 228,
		QUERY_IS_TOO_LARGE = 229,
		NOT_FOUND_EXPECTED_DATA_PART = 230,
		TOO_MANY_UNEXPECTED_DATA_PARTS = 231,
		NO_SUCH_DATA_PART = 232,
		BAD_DATA_PART_NAME = 233,
		NO_REPLICA_HAS_PART = 234,
		DUPLICATE_DATA_PART = 235,
		ABORTED = 236,
		NO_REPLICA_NAME_GIVEN = 237,
		FORMAT_VERSION_TOO_OLD = 238,
		CANNOT_MUNMAP = 239,
		CANNOT_MREMAP = 240,
		MEMORY_LIMIT_EXCEEDED = 241,
		TABLE_IS_READ_ONLY = 242,
		NOT_ENOUGH_SPACE = 243,
		UNEXPECTED_ZOOKEEPER_ERROR = 244,
		INVALID_NESTED_NAME = 245,
		CORRUPTED_DATA = 246,
		INCORRECT_MARK = 247,
		INVALID_PARTITION_NAME = 248,
		NOT_LEADER = 249,
		NOT_ENOUGH_BLOCK_NUMBERS = 250,
		NO_SUCH_REPLICA = 251,
		TOO_MUCH_PARTS = 252,
		REPLICA_IS_ALREADY_EXIST = 253,
		NO_ACTIVE_REPLICAS = 254,
		TOO_MUCH_RETRIES_TO_FETCH_PARTS = 255,
		PARTITION_ALREADY_EXISTS = 256,
		PARTITION_DOESNT_EXIST = 257,
		UNION_ALL_RESULT_STRUCTURES_MISMATCH = 258,
		UNION_ALL_COLUMN_ALIAS_MISMATCH = 259,
		CLIENT_OUTPUT_FORMAT_SPECIFIED = 260,
		UNKNOWN_BLOCK_INFO_FIELD = 261,
		BAD_COLLATION = 262,
		CANNOT_COMPILE_CODE = 263,
		INCOMPATIBLE_TYPE_OF_JOIN = 264,
		NO_AVAILABLE_REPLICA = 265,
		MISMATCH_REPLICAS_DATA_SOURCES = 266,
		STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS = 267,
		CPUID_ERROR = 268,
		INFINITE_LOOP = 269,
		CANNOT_COMPRESS = 270,
		CANNOT_DECOMPRESS = 271,
		AIO_SUBMIT_ERROR = 272,
		AIO_COMPLETION_ERROR = 273,
		AIO_READ_ERROR = 274,
		AIO_WRITE_ERROR = 275,
		INDEX_NOT_USED = 277,
		LEADERSHIP_LOST = 278,
		ALL_CONNECTION_TRIES_FAILED = 279,
		NO_AVAILABLE_DATA = 280,
		DICTIONARY_IS_EMPTY = 281,
		INCORRECT_INDEX = 282,
		UNKNOWN_GLOBAL_SUBQUERIES_METHOD = 284,
		TOO_LESS_LIVE_REPLICAS = 285,
		UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE = 286,

		KEEPER_EXCEPTION = 999,
		POCO_EXCEPTION = 1000,
		STD_EXCEPTION = 1001,
		UNKNOWN_EXCEPTION = 1002,
	};
}

}
