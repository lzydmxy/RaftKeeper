#include <DB/Common/ProfileEvents.h>


/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
	M(Query) \
	M(SelectQuery) \
	M(InsertQuery) \
	M(FileOpen) \
	M(Seek) \
	M(ReadBufferFromFileDescriptorRead) \
	M(ReadBufferFromFileDescriptorReadBytes) \
	M(WriteBufferFromFileDescriptorWrite) \
	M(WriteBufferFromFileDescriptorWriteBytes) \
	M(ReadBufferAIORead) \
	M(ReadBufferAIOReadBytes) \
	M(WriteBufferAIOWrite) \
	M(WriteBufferAIOWriteBytes) \
	M(ReadCompressedBytes) \
	M(CompressedReadBufferBlocks) \
	M(CompressedReadBufferBytes) \
	M(UncompressedCacheHits) \
	M(UncompressedCacheMisses) \
	M(UncompressedCacheWeightLost) \
	M(IOBufferAllocs) \
	M(IOBufferAllocBytes) \
	M(ArenaAllocChunks) \
	M(ArenaAllocBytes) \
	M(FunctionExecute) \
	M(MarkCacheHits) \
	M(MarkCacheMisses) \
	M(CreatedReadBufferOrdinary) \
	M(CreatedReadBufferAIO) \
	M(CreatedWriteBufferOrdinary) \
	M(CreatedWriteBufferAIO) \
	\
	M(ReplicatedPartFetches) \
	M(ReplicatedPartFailedFetches) \
	M(ObsoleteReplicatedParts) \
	M(ReplicatedPartMerges) \
	M(ReplicatedPartFetchesOfMerged) \
	M(ReplicatedPartChecks) \
	M(ReplicatedPartChecksFailed) \
	M(ReplicatedDataLoss) \
	\
	M(DelayedInserts) \
	M(RejectedInserts) \
	M(DelayedInsertsMilliseconds) \
	M(SynchronousMergeOnInsert) \
	\
	M(ZooKeeperInit) \
	M(ZooKeeperTransactions) \
	M(ZooKeeperGetChildren) \
	M(ZooKeeperCreate) \
	M(ZooKeeperRemove) \
	M(ZooKeeperExists) \
	M(ZooKeeperGet) \
	M(ZooKeeperSet) \
	M(ZooKeeperMulti) \
	M(ZooKeeperExceptions) \
	\
	M(DistributedConnectionFailTry) \
	M(DistributedConnectionFailAtAll) \
	\
	M(CompileAttempt) \
	M(CompileSuccess) \
	\
	M(ExternalSortWritePart) \
	M(ExternalSortMerge) \
	M(ExternalAggregationWritePart) \
	M(ExternalAggregationMerge) \
	M(ExternalAggregationCompressedBytes) \
	M(ExternalAggregationUncompressedBytes) \
	\
	M(SlowRead) \
	M(ReadBackoff) \
	\
	M(ReplicaYieldLeadership) \
	M(ReplicaPartialShutdown) \
	\
	M(SelectedParts) \
	M(SelectedRanges) \
	M(SelectedMarks) \
	\
	M(MergedRows) \
	M(MergedUncompressedBytes) \
	\
	M(MergeTreeDataWriterRows) \
	M(MergeTreeDataWriterUncompressedBytes) \
	M(MergeTreeDataWriterCompressedBytes) \
	M(MergeTreeDataWriterBlocks) \
	M(MergeTreeDataWriterBlocksAlreadySorted) \
	\
	M(ObsoleteEphemeralNode) \
	M(CannotRemoveEphemeralNode) \
	M(LeaderElectionAcquiredLeadership) \
	\
	M(RegexpCreated) \

namespace ProfileEvents
{
	#define M(NAME) extern const Event NAME = __COUNTER__;
		APPLY_FOR_EVENTS(M)
	#undef M
	constexpr Event END = __COUNTER__;

	std::atomic<Count> counters[END] {};	/// Global variable, initialized by zeros.

	const char * getDescription(Event event)
	{
		static const char * descriptions[] =
		{
		#define M(NAME) #NAME,
			APPLY_FOR_EVENTS(M)
		#undef M
		};

		return descriptions[event];
	}

	Event end() { return END; }
}

#undef APPLY_FOR_EVENTS
