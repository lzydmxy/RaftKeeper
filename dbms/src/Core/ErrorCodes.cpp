namespace DB
{

namespace ErrorCodes
{
	/** Раньше эти константы были расположены в одном enum-е.
	  * Но в этом случае возникает проблема: при добавлении новой константы необходимо перекомпилировать
	  *  все translation unit-ы, которые используют хотя бы одну константу (почти весь проект).
	  * Поэтому сделано так, что определения констант расположены здесь, в одном файле,
	  *  а их объявления - в разных файлах, по месту использования.
	  */

	extern const int UNSUPPORTED_METHOD = 1;
	extern const int UNSUPPORTED_PARAMETER = 2;
	extern const int UNEXPECTED_END_OF_FILE = 3;
	extern const int EXPECTED_END_OF_FILE = 4;
	extern const int CANNOT_PARSE_TEXT = 6;
	extern const int INCORRECT_NUMBER_OF_COLUMNS = 7;
	extern const int THERE_IS_NO_COLUMN = 8;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH = 9;
	extern const int NOT_FOUND_COLUMN_IN_BLOCK = 10;
	extern const int POSITION_OUT_OF_BOUND = 11;
	extern const int PARAMETER_OUT_OF_BOUND = 12;
	extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH = 13;
	extern const int DUPLICATE_COLUMN = 15;
	extern const int NO_SUCH_COLUMN_IN_TABLE = 16;
	extern const int DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH = 17;
	extern const int CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN = 18;
	extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH = 19;
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH = 20;
	extern const int CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT = 21;
	extern const int CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT = 22;
	extern const int CANNOT_READ_FROM_ISTREAM = 23;
	extern const int CANNOT_WRITE_TO_OSTREAM = 24;
	extern const int CANNOT_PARSE_ESCAPE_SEQUENCE = 25;
	extern const int CANNOT_PARSE_QUOTED_STRING = 26;
	extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED = 27;
	extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER = 28;
	extern const int CANNOT_PRINT_INTEGER = 29;
	extern const int CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK = 30;
	extern const int CANNOT_READ_COMPRESSED_CHUNK = 31;
	extern const int ATTEMPT_TO_READ_AFTER_EOF = 32;
	extern const int CANNOT_READ_ALL_DATA = 33;
	extern const int TOO_MUCH_ARGUMENTS_FOR_FUNCTION = 34;
	extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION = 35;
	extern const int BAD_ARGUMENTS = 36;
	extern const int UNKNOWN_ELEMENT_IN_AST = 37;
	extern const int CANNOT_PARSE_DATE = 38;
	extern const int TOO_LARGE_SIZE_COMPRESSED = 39;
	extern const int CHECKSUM_DOESNT_MATCH = 40;
	extern const int CANNOT_PARSE_DATETIME = 41;
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH = 42;
	extern const int ILLEGAL_TYPE_OF_ARGUMENT = 43;
	extern const int ILLEGAL_COLUMN = 44;
	extern const int ILLEGAL_NUMBER_OF_RESULT_COLUMNS = 45;
	extern const int UNKNOWN_FUNCTION = 46;
	extern const int UNKNOWN_IDENTIFIER = 47;
	extern const int NOT_IMPLEMENTED = 48;
	extern const int LOGICAL_ERROR = 49;
	extern const int UNKNOWN_TYPE = 50;
	extern const int EMPTY_LIST_OF_COLUMNS_QUERIED = 51;
	extern const int COLUMN_QUERIED_MORE_THAN_ONCE = 52;
	extern const int TYPE_MISMATCH = 53;
	extern const int STORAGE_DOESNT_ALLOW_PARAMETERS = 54;
	extern const int STORAGE_REQUIRES_PARAMETER = 55;
	extern const int UNKNOWN_STORAGE = 56;
	extern const int TABLE_ALREADY_EXISTS = 57;
	extern const int TABLE_METADATA_ALREADY_EXISTS = 58;
	extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER = 59;
	extern const int UNKNOWN_TABLE = 60;
	extern const int ONLY_FILTER_COLUMN_IN_BLOCK = 61;
	extern const int SYNTAX_ERROR = 62;
	extern const int UNKNOWN_AGGREGATE_FUNCTION = 63;
	extern const int CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT = 64;
	extern const int CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT = 65;
	extern const int NOT_A_COLUMN = 66;
	extern const int ILLEGAL_KEY_OF_AGGREGATION = 67;
	extern const int CANNOT_GET_SIZE_OF_FIELD = 68;
	extern const int ARGUMENT_OUT_OF_BOUND = 69;
	extern const int CANNOT_CONVERT_TYPE = 70;
	extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER = 71;
	extern const int CANNOT_PARSE_NUMBER = 72;
	extern const int UNKNOWN_FORMAT = 73;
	extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR = 74;
	extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR = 75;
	extern const int CANNOT_OPEN_FILE = 76;
	extern const int CANNOT_CLOSE_FILE = 77;
	extern const int UNKNOWN_TYPE_OF_QUERY = 78;
	extern const int INCORRECT_FILE_NAME = 79;
	extern const int INCORRECT_QUERY = 80;
	extern const int UNKNOWN_DATABASE = 81;
	extern const int DATABASE_ALREADY_EXISTS = 82;
	extern const int DIRECTORY_DOESNT_EXIST = 83;
	extern const int DIRECTORY_ALREADY_EXISTS = 84;
	extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT = 85;
	extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER = 86;
	extern const int CANNOT_SEEK_THROUGH_FILE = 87;
	extern const int CANNOT_TRUNCATE_FILE = 88;
	extern const int UNKNOWN_COMPRESSION_METHOD = 89;
	extern const int EMPTY_LIST_OF_COLUMNS_PASSED = 90;
	extern const int SIZES_OF_MARKS_FILES_ARE_INCONSISTENT = 91;
	extern const int EMPTY_DATA_PASSED = 92;
	extern const int UNKNOWN_AGGREGATED_DATA_VARIANT = 93;
	extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS = 94;
	extern const int CANNOT_READ_FROM_SOCKET = 95;
	extern const int CANNOT_WRITE_TO_SOCKET = 96;
	extern const int CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT = 97;
	extern const int CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM = 98;
	extern const int UNKNOWN_PACKET_FROM_CLIENT = 99;
	extern const int UNKNOWN_PACKET_FROM_SERVER = 100;
	extern const int UNEXPECTED_PACKET_FROM_CLIENT = 101;
	extern const int UNEXPECTED_PACKET_FROM_SERVER = 102;
	extern const int RECEIVED_DATA_FOR_WRONG_QUERY_ID = 103;
	extern const int TOO_SMALL_BUFFER_SIZE = 104;
	extern const int CANNOT_READ_HISTORY = 105;
	extern const int CANNOT_APPEND_HISTORY = 106;
	extern const int FILE_DOESNT_EXIST = 107;
	extern const int NO_DATA_TO_INSERT = 108;
	extern const int CANNOT_BLOCK_SIGNAL = 109;
	extern const int CANNOT_UNBLOCK_SIGNAL = 110;
	extern const int CANNOT_MANIPULATE_SIGSET = 111;
	extern const int CANNOT_WAIT_FOR_SIGNAL = 112;
	extern const int THERE_IS_NO_SESSION = 113;
	extern const int CANNOT_CLOCK_GETTIME = 114;
	extern const int UNKNOWN_SETTING = 115;
	extern const int THERE_IS_NO_DEFAULT_VALUE = 116;
	extern const int INCORRECT_DATA = 117;
	extern const int TABLE_METADATA_DOESNT_EXIST = 118;
	extern const int ENGINE_REQUIRED = 119;
	extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE = 120;
	extern const int UNKNOWN_SET_DATA_VARIANT = 121;
	extern const int INCOMPATIBLE_COLUMNS = 122;
	extern const int UNKNOWN_TYPE_OF_AST_NODE = 123;
	extern const int INCORRECT_ELEMENT_OF_SET = 124;
	extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY = 125;
	extern const int CANNOT_GET_RETURN_TYPE = 126;
	extern const int ILLEGAL_INDEX = 127;
	extern const int TOO_LARGE_ARRAY_SIZE = 128;
	extern const int FUNCTION_IS_SPECIAL = 129;
	extern const int CANNOT_READ_ARRAY_FROM_TEXT = 130;
	extern const int TOO_LARGE_STRING_SIZE = 131;
	extern const int CANNOT_CREATE_TABLE_FROM_METADATA = 132;
	extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS = 133;
	extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS = 134;
	extern const int ZERO_ARRAY_OR_TUPLE_INDEX = 135;
	extern const int UNKNOWN_ELEMENT_IN_CONFIG = 137;
	extern const int EXCESSIVE_ELEMENT_IN_CONFIG = 138;
	extern const int NO_ELEMENTS_IN_CONFIG = 139;
	extern const int ALL_REQUESTED_COLUMNS_ARE_MISSING = 140;
	extern const int SAMPLING_NOT_SUPPORTED = 141;
	extern const int NOT_FOUND_NODE = 142;
	extern const int FOUND_MORE_THAN_ONE_NODE = 143;
	extern const int FIRST_DATE_IS_BIGGER_THAN_LAST_DATE = 144;
	extern const int UNKNOWN_OVERFLOW_MODE = 145;
	extern const int QUERY_SECTION_DOESNT_MAKE_SENSE = 146;
	extern const int NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE = 147;
	extern const int NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION = 148;
	extern const int NOT_FOUND_RHS_ELEMENT_FOR_CONDITION = 149;
	extern const int NO_ATTRIBUTES_LISTED = 150;
	extern const int INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE = 151;
	extern const int UNKNOWN_DIRECTION_OF_SORTING = 152;
	extern const int ILLEGAL_DIVISION = 153;
	extern const int AGGREGATE_FUNCTION_NOT_APPLICABLE = 154;
	extern const int UNKNOWN_RELATION = 155;
	extern const int DICTIONARIES_WAS_NOT_LOADED = 156;
	extern const int ILLEGAL_OVERFLOW_MODE = 157;
	extern const int TOO_MUCH_ROWS = 158;
	extern const int TIMEOUT_EXCEEDED = 159;
	extern const int TOO_SLOW = 160;
	extern const int TOO_MUCH_COLUMNS = 161;
	extern const int TOO_DEEP_SUBQUERIES = 162;
	extern const int TOO_DEEP_PIPELINE = 163;
	extern const int READONLY = 164;
	extern const int TOO_MUCH_TEMPORARY_COLUMNS = 165;
	extern const int TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS = 166;
	extern const int TOO_DEEP_AST = 167;
	extern const int TOO_BIG_AST = 168;
	extern const int BAD_TYPE_OF_FIELD = 169;
	extern const int BAD_GET = 170;
	extern const int BLOCKS_HAS_DIFFERENT_STRUCTURE = 171;
	extern const int CANNOT_CREATE_DIRECTORY = 172;
	extern const int CANNOT_ALLOCATE_MEMORY = 173;
	extern const int CYCLIC_ALIASES = 174;
	extern const int CHUNK_NOT_FOUND = 176;
	extern const int DUPLICATE_CHUNK_NAME = 177;
	extern const int MULTIPLE_ALIASES_FOR_EXPRESSION = 178;
	extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS = 179;
	extern const int THERE_IS_NO_PROFILE = 180;
	extern const int ILLEGAL_FINAL = 181;
	extern const int ILLEGAL_PREWHERE = 182;
	extern const int UNEXPECTED_EXPRESSION = 183;
	extern const int ILLEGAL_AGGREGATION = 184;
	extern const int UNSUPPORTED_MYISAM_BLOCK_TYPE = 185;
	extern const int UNSUPPORTED_COLLATION_LOCALE = 186;
	extern const int COLLATION_COMPARISON_FAILED = 187;
	extern const int UNKNOWN_ACTION = 188;
	extern const int TABLE_MUST_NOT_BE_CREATED_MANUALLY = 189;
	extern const int SIZES_OF_ARRAYS_DOESNT_MATCH = 190;
	extern const int SET_SIZE_LIMIT_EXCEEDED = 191;
	extern const int UNKNOWN_USER = 192;
	extern const int WRONG_PASSWORD = 193;
	extern const int REQUIRED_PASSWORD = 194;
	extern const int IP_ADDRESS_NOT_ALLOWED = 195;
	extern const int UNKNOWN_ADDRESS_PATTERN_TYPE = 196;
	extern const int SERVER_REVISION_IS_TOO_OLD = 197;
	extern const int DNS_ERROR = 198;
	extern const int UNKNOWN_QUOTA = 199;
	extern const int QUOTA_DOESNT_ALLOW_KEYS = 200;
	extern const int QUOTA_EXPIRED = 201;
	extern const int TOO_MUCH_SIMULTANEOUS_QUERIES = 202;
	extern const int NO_FREE_CONNECTION = 203;
	extern const int CANNOT_FSYNC = 204;
	extern const int NESTED_TYPE_TOO_DEEP = 205;
	extern const int ALIAS_REQUIRED = 206;
	extern const int AMBIGUOUS_IDENTIFIER = 207;
	extern const int EMPTY_NESTED_TABLE = 208;
	extern const int SOCKET_TIMEOUT = 209;
	extern const int NETWORK_ERROR = 210;
	extern const int EMPTY_QUERY = 211;
	extern const int UNKNOWN_LOAD_BALANCING = 212;
	extern const int UNKNOWN_TOTALS_MODE = 213;
	extern const int CANNOT_STATVFS = 214;
	extern const int NOT_AN_AGGREGATE = 215;
	extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING = 216;
	extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT = 217;
	extern const int TABLE_IS_DROPPED = 218;
	extern const int DATABASE_NOT_EMPTY = 219;
	extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT = 220;
	extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT = 221;
	extern const int ADDING_REPLICA_TO_NON_EMPTY_TABLE = 222;
	extern const int UNEXPECTED_AST_STRUCTURE = 223;
	extern const int REPLICA_IS_ALREADY_ACTIVE = 224;
	extern const int NO_ZOOKEEPER = 225;
	extern const int NO_FILE_IN_DATA_PART = 226;
	extern const int UNEXPECTED_FILE_IN_DATA_PART = 227;
	extern const int BAD_SIZE_OF_FILE_IN_DATA_PART = 228;
	extern const int QUERY_IS_TOO_LARGE = 229;
	extern const int NOT_FOUND_EXPECTED_DATA_PART = 230;
	extern const int TOO_MANY_UNEXPECTED_DATA_PARTS = 231;
	extern const int NO_SUCH_DATA_PART = 232;
	extern const int BAD_DATA_PART_NAME = 233;
	extern const int NO_REPLICA_HAS_PART = 234;
	extern const int DUPLICATE_DATA_PART = 235;
	extern const int ABORTED = 236;
	extern const int NO_REPLICA_NAME_GIVEN = 237;
	extern const int FORMAT_VERSION_TOO_OLD = 238;
	extern const int CANNOT_MUNMAP = 239;
	extern const int CANNOT_MREMAP = 240;
	extern const int MEMORY_LIMIT_EXCEEDED = 241;
	extern const int TABLE_IS_READ_ONLY = 242;
	extern const int NOT_ENOUGH_SPACE = 243;
	extern const int UNEXPECTED_ZOOKEEPER_ERROR = 244;
	extern const int INVALID_NESTED_NAME = 245;
	extern const int CORRUPTED_DATA = 246;
	extern const int INCORRECT_MARK = 247;
	extern const int INVALID_PARTITION_NAME = 248;
	extern const int NOT_ENOUGH_BLOCK_NUMBERS = 250;
	extern const int NO_SUCH_REPLICA = 251;
	extern const int TOO_MUCH_PARTS = 252;
	extern const int REPLICA_IS_ALREADY_EXIST = 253;
	extern const int NO_ACTIVE_REPLICAS = 254;
	extern const int TOO_MUCH_RETRIES_TO_FETCH_PARTS = 255;
	extern const int PARTITION_ALREADY_EXISTS = 256;
	extern const int PARTITION_DOESNT_EXIST = 257;
	extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH = 258;
	extern const int UNION_ALL_COLUMN_ALIAS_MISMATCH = 259;
	extern const int CLIENT_OUTPUT_FORMAT_SPECIFIED = 260;
	extern const int UNKNOWN_BLOCK_INFO_FIELD = 261;
	extern const int BAD_COLLATION = 262;
	extern const int CANNOT_COMPILE_CODE = 263;
	extern const int INCOMPATIBLE_TYPE_OF_JOIN = 264;
	extern const int NO_AVAILABLE_REPLICA = 265;
	extern const int MISMATCH_REPLICAS_DATA_SOURCES = 266;
	extern const int STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS = 267;
	extern const int CPUID_ERROR = 268;
	extern const int INFINITE_LOOP = 269;
	extern const int CANNOT_COMPRESS = 270;
	extern const int CANNOT_DECOMPRESS = 271;
	extern const int AIO_SUBMIT_ERROR = 272;
	extern const int AIO_COMPLETION_ERROR = 273;
	extern const int AIO_READ_ERROR = 274;
	extern const int AIO_WRITE_ERROR = 275;
	extern const int INDEX_NOT_USED = 277;
	extern const int LEADERSHIP_LOST = 278;
	extern const int ALL_CONNECTION_TRIES_FAILED = 279;
	extern const int NO_AVAILABLE_DATA = 280;
	extern const int DICTIONARY_IS_EMPTY = 281;
	extern const int INCORRECT_INDEX = 282;
	extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE = 283;
	extern const int UNKNOWN_GLOBAL_SUBQUERIES_METHOD = 284;
	extern const int TOO_LESS_LIVE_REPLICAS = 285;
	extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE = 286;
	extern const int UNKNOWN_FORMAT_VERSION = 287;
	extern const int DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED = 288;
	extern const int REPLICA_IS_NOT_IN_QUORUM = 289;
	extern const int LIMIT_EXCEEDED = 290;
	extern const int DATABASE_ACCESS_DENIED = 291;
	extern const int LEADERSHIP_CHANGED = 292;
	extern const int MONGODB_CANNOT_AUTHENTICATE = 293;
	extern const int INVALID_BLOCK_EXTRA_INFO = 294;
	extern const int RECEIVED_EMPTY_DATA = 295;
	extern const int NO_REMOTE_SHARD_FOUND = 296;
	extern const int SHARD_HAS_NO_CONNECTIONS = 297;
	extern const int CANNOT_PIPE = 298;
	extern const int CANNOT_FORK = 299;
	extern const int CANNOT_DLSYM = 300;
	extern const int CANNOT_CREATE_CHILD_PROCESS = 301;
	extern const int CHILD_WAS_NOT_EXITED_NORMALLY = 302;
	extern const int CANNOT_SELECT = 303;
	extern const int CANNOT_WAITPID = 304;
	extern const int TABLE_WAS_NOT_DROPPED = 305;
	extern const int TOO_DEEP_RECURSION = 306;
	extern const int TOO_MUCH_BYTES = 307;
	extern const int UNEXPECTED_NODE_IN_ZOOKEEPER = 308;
	extern const int FUNCTION_CANNOT_HAVE_PARAMETERS = 309;
	extern const int INCONSISTENT_TABLE_ACCROSS_SHARDS = 310;
	extern const int INSUFFICIENT_SPACE_FOR_RESHARDING = 311;
	extern const int PARTITION_COPY_FAILED = 312;
	extern const int PARTITION_ATTACH_FAILED = 313;
	extern const int RESHARDING_NO_WORKER = 314;
	extern const int INVALID_PARTITIONS_INTERVAL = 315;
	extern const int RESHARDING_INVALID_PARAMETERS = 316;
	extern const int INVALID_SHARD_WEIGHT = 317;
	extern const int INVALID_CONFIG_PARAMETER = 318;
	extern const int UNKNOWN_STATUS_OF_INSERT = 319;
	extern const int DUPLICATE_SHARD_PATHS = 320;
	extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE = 321;
	extern const int RESHARDING_BUSY_CLUSTER = 322;
	extern const int RESHARDING_BUSY_SHARD = 323;
	extern const int RESHARDING_NO_SUCH_COORDINATOR = 324;
	extern const int RESHARDING_NO_COORDINATOR_MEMBERSHIP = 325;
	extern const int RESHARDING_ALREADY_SUBSCRIBED = 326;
	extern const int RESHARDING_REMOTE_NODE_UNAVAILABLE = 327;
	extern const int RESHARDING_REMOTE_NODE_ERROR = 328;
	extern const int RESHARDING_COORDINATOR_DELETED = 329;
	extern const int RESHARDING_DISTRIBUTED_JOB_ON_HOLD = 330;
	extern const int RESHARDING_INVALID_QUERY = 331;
	extern const int RESHARDING_INITIATOR_CHECK_FAILED = 332;
	extern const int RWLOCK_ALREADY_HELD = 333;
	extern const int RWLOCK_NO_SUCH_LOCK = 334;
	extern const int BARRIER_TIMEOUT = 335;
	extern const int UNKNOWN_DATABASE_ENGINE = 336;
	extern const int DDL_GUARD_IS_ACTIVE = 337;
	extern const int NO_SUCH_BARRIER = 338;
	extern const int RESHARDING_ILL_FORMED_LOG = 339;
	extern const int NO_ZOOKEEPER_ACCESSOR = 340;
	extern const int UNFINISHED = 341;
	extern const int METADATA_MISMATCH = 342;
	extern const int SUPPORT_IS_DISABLED = 344;
	extern const int TABLE_DIFFERS_TOO_MUCH = 345;
	extern const int CANNOT_ICONV = 346;
	extern const int CANNOT_LOAD_CONFIG = 347;
	extern const int RESHARDING_NULLABLE_SHARDING_KEY = 348;
	extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN = 349;
	extern const int INCOMPATIBLE_SOURCE_TABLES = 350;
	extern const int AMBIGUOUS_TABLE_NAME = 351;
	extern const int AMBIGUOUS_COLUMN_NAME = 352;
	extern const int INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE = 353;
	extern const int ZLIB_INFLATE_FAILED = 354;
	extern const int ZLIB_DEFLATE_FAILED = 355;

	extern const int KEEPER_EXCEPTION = 999;
	extern const int POCO_EXCEPTION = 1000;
	extern const int STD_EXCEPTION = 1001;
	extern const int UNKNOWN_EXCEPTION = 1002;
}

}
