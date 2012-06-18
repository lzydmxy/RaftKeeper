ATTACH TABLE hits
(
    WatchID UInt64
,     JavaEnable UInt8
,     Title String
,     GoodEvent UInt32
,     EventTime DateTime
,     CounterID UInt32
,     ClientIP UInt32
,     RegionID UInt32
,     UniqID UInt64
,     CounterClass UInt8
,     OS UInt8
,     UserAgent UInt8
,     URL String
,     Referer String
,     Refresh UInt8
,     ResolutionWidth UInt16
,     ResolutionHeight UInt16
,     ResolutionDepth UInt8
,     FlashMajor UInt8
,     FlashMinor UInt8
,     FlashMinor2 String
,     NetMajor UInt8
,     NetMinor UInt8
,     UserAgentMajor UInt16
,     UserAgentMinor FixedString(2)
,     CookieEnable UInt8
,     JavascriptEnable UInt8
,     IsMobile UInt8
,     MobilePhone UInt8
,     MobilePhoneModel String
,     Params String
,     IPNetworkID UInt32
,     TraficSourceID Int8
,     SearchEngineID UInt16
,     SearchPhrase String
,     AdvEngineID UInt8
,     IsArtifical UInt8
,     WindowClientWidth UInt16
,     WindowClientHeight UInt16
,     ClientTimeZone Int16
,     ClientEventTime DateTime
,     SilverlightVersion1 UInt8
,     SilverlightVersion2 UInt8
,     SilverlightVersion3 UInt32
,     SilverlightVersion4 UInt16
,     PageCharset String
,     CodeVersion UInt32
,     IsLink UInt8
,     IsDownload UInt8
,     IsNotBounce UInt8
,     FUniqID UInt64
,     OriginalURL String
,     HID UInt32
,     IsOldCounter UInt8
,     IsEvent UInt8
,     IsParameter UInt8
,     DontCountHits UInt8
,     WithHash UInt8
) ENGINE = Log
