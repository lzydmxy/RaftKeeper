# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/sparsehash
    contrib/libs/poco/MongoDB
)

CFLAGS(-g0)

SRCS(
    AlterCommands.cpp
    ColumnDefault.cpp
    ColumnsDescription.cpp
    ConstraintsDescription.cpp
    Distributed/DirectoryMonitor.cpp
    Distributed/DistributedBlockOutputStream.cpp
    extractKeyExpressionList.cpp
    getStructureOfRemoteTable.cpp
    IndicesDescription.cpp
    IStorage.cpp
    JoinSettings.cpp
    KeyDescription.cpp
    LiveView/StorageLiveView.cpp
    LiveView/TemporaryLiveViewCleaner.cpp
    MergeTree/ActiveDataPartSet.cpp
    MergeTree/AllMergeSelector.cpp
    MergeTree/BackgroundProcessingPool.cpp
    MergeTree/BoolMask.cpp
    MergeTree/checkDataPart.cpp
    MergeTree/DataPartsExchange.cpp
    MergeTree/EphemeralLockInZooKeeper.cpp
    MergeTree/IMergedBlockOutputStream.cpp
    MergeTree/IMergeTreeDataPart.cpp
    MergeTree/IMergeTreeDataPartWriter.cpp
    MergeTree/IMergeTreeReader.cpp
    MergeTree/KeyCondition.cpp
    MergeTree/LevelMergeSelector.cpp
    MergeTree/localBackup.cpp
    MergeTree/MergeAlgorithm.cpp
    MergeTree/MergedBlockOutputStream.cpp
    MergeTree/MergedColumnOnlyOutputStream.cpp
    MergeTree/MergeList.cpp
    MergeTree/MergeTreeBaseSelectProcessor.cpp
    MergeTree/MergeTreeBlockOutputStream.cpp
    MergeTree/MergeTreeBlockReadUtils.cpp
    MergeTree/MergeTreeData.cpp
    MergeTree/MergeTreeDataMergerMutator.cpp
    MergeTree/MergeTreeDataPartChecksum.cpp
    MergeTree/MergeTreeDataPartCompact.cpp
    MergeTree/MergeTreeDataPartInMemory.cpp
    MergeTree/MergeTreeDataPartTTLInfo.cpp
    MergeTree/MergeTreeDataPartType.cpp
    MergeTree/MergeTreeDataPartWide.cpp
    MergeTree/MergeTreeDataPartWriterCompact.cpp
    MergeTree/MergeTreeDataPartWriterInMemory.cpp
    MergeTree/MergeTreeDataPartWriterOnDisk.cpp
    MergeTree/MergeTreeDataPartWriterWide.cpp
    MergeTree/MergeTreeDataSelectExecutor.cpp
    MergeTree/MergeTreeDataWriter.cpp
    MergeTree/MergeTreeIndexAggregatorBloomFilter.cpp
    MergeTree/MergeTreeIndexBloomFilter.cpp
    MergeTree/MergeTreeIndexConditionBloomFilter.cpp
    MergeTree/MergeTreeIndexFullText.cpp
    MergeTree/MergeTreeIndexGranularity.cpp
    MergeTree/MergeTreeIndexGranularityInfo.cpp
    MergeTree/MergeTreeIndexGranuleBloomFilter.cpp
    MergeTree/MergeTreeIndexMinMax.cpp
    MergeTree/MergeTreeIndexReader.cpp
    MergeTree/MergeTreeIndexSet.cpp
    MergeTree/MergeTreeIndices.cpp
    MergeTree/MergeTreeMarksLoader.cpp
    MergeTree/MergeTreeMutationEntry.cpp
    MergeTree/MergeTreeMutationStatus.cpp
    MergeTree/MergeTreePartInfo.cpp
    MergeTree/MergeTreePartition.cpp
    MergeTree/MergeTreePartsMover.cpp
    MergeTree/MergeTreeRangeReader.cpp
    MergeTree/MergeTreeReaderCompact.cpp
    MergeTree/MergeTreeReaderInMemory.cpp
    MergeTree/MergeTreeReaderStream.cpp
    MergeTree/MergeTreeReaderWide.cpp
    MergeTree/MergeTreeReadPool.cpp
    MergeTree/MergeTreeReverseSelectProcessor.cpp
    MergeTree/MergeTreeSelectProcessor.cpp
    MergeTree/MergeTreeSequentialSource.cpp
    MergeTree/MergeTreeSettings.cpp
    MergeTree/MergeTreeThreadSelectBlockInputProcessor.cpp
    MergeTree/MergeTreeWhereOptimizer.cpp
    MergeTree/MergeTreeWriteAheadLog.cpp
    MergeTree/MergeType.cpp
    MergeTree/registerStorageMergeTree.cpp
    MergeTree/ReplicatedFetchesList.cpp
    MergeTree/ReplicatedMergeTreeAddress.cpp
    MergeTree/ReplicatedMergeTreeAltersSequence.cpp
    MergeTree/ReplicatedMergeTreeBlockOutputStream.cpp
    MergeTree/ReplicatedMergeTreeCleanupThread.cpp
    MergeTree/ReplicatedMergeTreeLogEntry.cpp
    MergeTree/ReplicatedMergeTreeMutationEntry.cpp
    MergeTree/ReplicatedMergeTreePartCheckThread.cpp
    MergeTree/ReplicatedMergeTreePartHeader.cpp
    MergeTree/ReplicatedMergeTreeQueue.cpp
    MergeTree/ReplicatedMergeTreeRestartingThread.cpp
    MergeTree/ReplicatedMergeTreeTableMetadata.cpp
    MergeTree/SimpleMergeSelector.cpp
    MergeTree/TTLMergeSelector.cpp
    MutationCommands.cpp
    PartitionCommands.cpp
    ReadInOrderOptimizer.cpp
    registerStorages.cpp
    SelectQueryDescription.cpp
    SetSettings.cpp
    StorageBuffer.cpp
    StorageDictionary.cpp
    StorageDistributed.cpp
    StorageFactory.cpp
    StorageFile.cpp
    StorageGenerateRandom.cpp
    StorageInMemoryMetadata.cpp
    StorageInput.cpp
    StorageJoin.cpp
    StorageLog.cpp
    StorageLogSettings.cpp
    StorageMaterializedView.cpp
    StorageMaterializeMySQL.cpp
    StorageMemory.cpp
    StorageMerge.cpp
    StorageMergeTree.cpp
    StorageMongoDB.cpp
    StorageMySQL.cpp
    StorageNull.cpp
    StorageReplicatedMergeTree.cpp
    StorageSet.cpp
    StorageStripeLog.cpp
    StorageTinyLog.cpp
    StorageURL.cpp
    StorageValues.cpp
    StorageView.cpp
    StorageXDBC.cpp
    System/attachSystemTables.cpp
    System/StorageSystemAggregateFunctionCombinators.cpp
    System/StorageSystemAsynchronousMetrics.cpp
    System/StorageSystemBuildOptions.cpp
    System/StorageSystemClusters.cpp
    System/StorageSystemCollations.cpp
    System/StorageSystemColumns.cpp
    System/StorageSystemContributors.cpp
    System/StorageSystemContributors.generated.cpp
    System/StorageSystemCurrentRoles.cpp
    System/StorageSystemDatabases.cpp
    System/StorageSystemDataTypeFamilies.cpp
    System/StorageSystemDetachedParts.cpp
    System/StorageSystemDictionaries.cpp
    System/StorageSystemDisks.cpp
    System/StorageSystemDistributionQueue.cpp
    System/StorageSystemEnabledRoles.cpp
    System/StorageSystemEvents.cpp
    System/StorageSystemFetches.cpp
    System/StorageSystemFormats.cpp
    System/StorageSystemFunctions.cpp
    System/StorageSystemGrants.cpp
    System/StorageSystemGraphite.cpp
    System/StorageSystemMacros.cpp
    System/StorageSystemMerges.cpp
    System/StorageSystemMergeTreeSettings.cpp
    System/StorageSystemMetrics.cpp
    System/StorageSystemModels.cpp
    System/StorageSystemMutations.cpp
    System/StorageSystemNumbers.cpp
    System/StorageSystemOne.cpp
    System/StorageSystemPartsBase.cpp
    System/StorageSystemPartsColumns.cpp
    System/StorageSystemParts.cpp
    System/StorageSystemPrivileges.cpp
    System/StorageSystemProcesses.cpp
    System/StorageSystemQuotaLimits.cpp
    System/StorageSystemQuotas.cpp
    System/StorageSystemQuotasUsage.cpp
    System/StorageSystemQuotaUsage.cpp
    System/StorageSystemReplicas.cpp
    System/StorageSystemReplicationQueue.cpp
    System/StorageSystemRoleGrants.cpp
    System/StorageSystemRoles.cpp
    System/StorageSystemRowPolicies.cpp
    System/StorageSystemSettings.cpp
    System/StorageSystemSettingsProfileElements.cpp
    System/StorageSystemSettingsProfiles.cpp
    System/StorageSystemStackTrace.cpp
    System/StorageSystemStoragePolicies.cpp
    System/StorageSystemTableEngines.cpp
    System/StorageSystemTableFunctions.cpp
    System/StorageSystemTables.cpp
    System/StorageSystemUserDirectories.cpp
    System/StorageSystemUsers.cpp
    System/StorageSystemZeros.cpp
    System/StorageSystemZooKeeper.cpp
    transformQueryForExternalDatabase.cpp
    TTLDescription.cpp
    VirtualColumnUtils.cpp

)

END()
