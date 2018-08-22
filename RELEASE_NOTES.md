### 0.0.15
* Use direct mode for connection policy

### 0.0.15-alpha
* Added timestamp for tail tacking

### 0.0.14-alpha
* Fixed issue for missing partitions in progress handler.

### 0.0.13-alpha
* lower FSharp.Core version requirements

### 0.0.12-alpha
* Support .NET Standard 2.0

### 0.0.11.2-alpha
* Added timeout to trail tracking changefeed request as well as handler to prevent hanging.

### 0.0.11.1-alpha
* Fixed issue with AsyncSeq accumulator array aggregation, now only tracks latest state.

### 0.0.11-alpha
* Fixed issue with out of memory, now changefeed api require a merge function

### 0.0.10
* Added partition filtering for changefeed processor

### 0.0.9
* Added a retry delay for querying a partition when the changefeed processor reaches the tail of that partition.

### 0.0.8
* Fixed an issue where the changefeed processor always read one batch regardless of start and stop positions.

### 0.0.7
* Made changefeed processor handler function take range position as argument
* Added timestamp information to the changefeed processor tail tracker

### 0.0.6
* Added changefeed tail position tracking

### 0.0.5
* Fixed issue with partition workers exiting when reaching end of changefeed

### 0.0.4
* Changed `ChangeFeed` to `Changefeed` in type and module names for consistency

### 0.0.3
* Changed `PartitionPosition` to `RangePosition` (without `PartitionId`)
* Changed `ChangeFeedPosition` from `RangePosition list` to `RangePosition[]`
* Made `ChangeFeedPositionTracker` internal

### 0.0.2
* Assembly name fixup
* Dependencies minimum version numbers

### 0.0.1
* initial
