module Sagan.ChangefeedProcessor

open System
open System.Linq

open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client

open FSharp.Control

open Prelude.AsyncExtensions

type CosmosEndpoint = {
  uri : Uri
  authKey : string
  databaseName : string
  collectionName : string
}

type RangePosition = {
  RangeMin : int64
  RangeMax : int64
  LastLSN : int64
}

type ChangefeedPosition = RangePosition[]

/// ChangefeedProcessor configuration
type Config = {
  /// MaxItemCount fetched from a partition per batch
  BatchSize : int

  /// Interval between invocations of `progressHandler`
  ProgressInterval : TimeSpan

  /// Wait time for successive queries of a changefeed partition if its tail position is reached
  RetryDelay : TimeSpan

  /// Position in the Changefeed to begin processing from
  StartingPosition : StartingPosition

  /// Position in the Changefeed to stop processing at
  StoppingPosition : ChangefeedPosition option
}

/// ChangefeedProcessor starting position in the DocDB changefeed
and StartingPosition =
  | Beginning
  | ChangefeedPosition of ChangefeedPosition

type private State = {
  /// The client used to communicate with DocumentDB
  client : DocumentClient

  /// URI of the collection being processed
  collectionUri : Uri
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RangePosition =
  /// Returns true if the range, r, covers the semi-open interval [min, max)
  let inline rangeCoversMinMax (r:RangePosition) min max =
    (r.RangeMin <= min) && (r.RangeMax >= max)

  /// Returns true if the range of the second argument, y, is fully contained within the first one.
  let inline fstCoversSnd (x:RangePosition) (y:RangePosition) =
    rangeCoversMinMax x y.RangeMin y.RangeMax

  /// Converts a cosmos range string from hex to int64
  let rangeToInt64 str =
    match Int64.TryParse(str, Globalization.NumberStyles.HexNumber, Globalization.CultureInfo.InvariantCulture) with
    | true, 255L when str.ToLower() = "ff" -> Int64.MaxValue    // max for last partition
    | true, i -> i
    | false, _ when str = "" -> 0L
    | false, _ -> 0L  // NOTE: I am not sure if this should be the default or if we should return an option instead

  /// Converts a cosmos logical sequence number (LSN) from string to int64
  let lsnToInt64 str =
    match Int64.TryParse str with
    | true, i -> i
    | false, _ -> 0L

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ChangefeedPosition =
  let fstSucceedsSnd (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
    let successionViolationExists =
      seq { // test range covers
        for x in cfp1 do
          yield cfp2 |> Seq.exists (fun y -> (RangePosition.fstCoversSnd x y) && (x.LastLSN > y.LastLSN))
      }
      |> Seq.exists id // find any succession violations
    not successionViolationExists

  let tryPickLatest (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
    if fstSucceedsSnd cfp1 cfp2 then Some cfp1
    elif fstSucceedsSnd cfp2 cfp1 then Some cfp2
    else None

  let tryGetPartitionByRange min max (cfp:ChangefeedPosition) =
    cfp |> Array.tryFind (fun rp -> RangePosition.rangeCoversMinMax rp min max)

[<RequireQualifiedAccess>]
/// A PartitionSelector is a function that filters a list of CosmosDB partitions into ones that a single processor will handle
module PartitionSelectors =
  /// A PartitionSelector is a function that filters a list of CosmosDB partitions into ones that a single processor will handle
  type PartitionSelector = PartitionKeyRange[] -> PartitionKeyRange[]

  /// An identity partition selector. It returns all given partitions
  let allPartitions : PartitionSelector = id

  /// A partition selector that attempts to split the partitions across `totalProcessors`.
  /// The ChangefeedProcessor gets assigned every nth partition to process.
  let evenSplit totalProcessors n : PartitionSelector =
    fun allPartitions ->
      allPartitions
      |> Array.sortBy (fun pkr -> pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64)
      |> Array.mapi (fun i pkr -> if i % totalProcessors = n then Some pkr else None)
      |> Array.choose id

/// Returns the current set of partitions in docdb changefeed
let private getPartitions (st:State) = async {
  // TODO: set FeedOptions properly. Needed for resuming from a changefeed position
  let! response = st.client.ReadPartitionKeyRangeFeedAsync st.collectionUri |> Async.AwaitTaskCorrect
  return response.ToArray()
}

/// Reads a partition of the changefeed and returns an AsyncSeq<Document[] * RangePosition>
///   - Document[] is a batch of documents read from changefeed
///   - RangePosition is a record containing the partition's key range and the last logical sequence number in the batch.
/// This function attempts to read all documents in the semi-closed LSN range [start, end).
let rec private readPartition (config:Config) (st:State) (pkr:PartitionKeyRange) =
  let rangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
  let rangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64

  let continuationToken : string =
    match config.StartingPosition with
    | Beginning -> null
    | ChangefeedPosition cfp ->
      cfp
      |> ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax
      |> Option.map (fun rp -> string rp.LastLSN)
      |> Option.getValueOr null   // docdb starts at the the beginning of a partition if null
  let stoppingPosition =
    config.StoppingPosition
    |> Option.bind (ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax)
    |> Option.map (fun rp -> rp.LastLSN)
  let cfo =
    ChangeFeedOptions(
      PartitionKeyRangeId = pkr.Id,
      MaxItemCount = Nullable config.BatchSize,
      StartFromBeginning = true,   // TODO: double check that this is ignored by docdb if RequestContinuation is set
      RequestContinuation = continuationToken)
  use query = st.client.CreateDocumentChangeFeedQuery(st.collectionUri, cfo)

  let rec readPartition (query:Linq.IDocumentQuery<Document>) (pkr:PartitionKeyRange) = asyncSeq {
    let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask

    let rp : RangePosition = {
      RangeMin = rangeMin
      RangeMax = rangeMax
      LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
    }
    if response <> null then
      if response.Count > 0 then
        yield (response.ToArray(), rp)
      if query.HasMoreResults then
        match stoppingPosition with
        | Some stopLSN when rp.LastLSN >= stopLSN -> ()   // TODO: this can stop after the stop position, but this is ok for now. Fix later.
        | _ -> yield! readPartition query pkr
      else
        do! Async.Sleep (int config.RetryDelay.TotalMilliseconds)
        yield! readPartition query pkr
  }

  let startLSN =
    if continuationToken = null then Int64.MinValue
    else int64 continuationToken

  match stoppingPosition with
  | Some stopLSN when startLSN >= stopLSN -> AsyncSeq.empty
  | _ -> readPartition query pkr

/// Returns an async computation that runs a concurrent (per-docdb-partition) changefeed processor.
/// - `handle`: is an asynchronous function that takes a batch of documents and returns a result
/// - `progressHandler`: is an asynchronous function ('a * ChangefeedPosition) -> Async<unit>
/// - `outputMerge` : is a merge function that merges the output result 'a -> 'a -> 'a
///    that is called periodically with a list of outputs that were produced by the handle function since the
///    last invocation and the current position of the changefeedprocessor.
/// - `partitionSelector`: is a filtering function `PartitionKeyRange[] -> PartitionKeyRange` that is used to narrow down the list of partitions to process.
let go (cosmos:CosmosEndpoint) (config:Config) (partitionSelector:PartitionSelectors.PartitionSelector)
    (handle:Document[]*RangePosition -> Async<'a>) (progressHandler:'a * ChangefeedPosition -> Async<Unit>) (outputMerge:'a*'a -> 'a) = async {
  use client = 
    let connPolicy = 
        let cp = ConnectionPolicy.Default
        cp.ConnectionMode <- ConnectionMode.Direct
        cp.ConnectionProtocol <- Protocol.Tcp
        cp.MaxConnectionLimit <- 1000
        cp
    new DocumentClient(cosmos.uri, cosmos.authKey, connPolicy, Nullable ConsistencyLevel.Session)
  let state = {
    client = client
    collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
  }

  // updates the given partition position in the given changefeed position
  let updateChangefeedPosition (cfp:ChangefeedPosition) (rp:RangePosition) =
    let index = cfp |> Array.tryFindIndex (function x -> x.RangeMin=rp.RangeMin && x.RangeMax=rp.RangeMax)
    match index with
    | Some i ->
      let newCfp = Array.copy cfp
      newCfp.[i] <- rp
      newCfp
    | None ->
      let newCfp = Array.zeroCreate (cfp.Length+1)
      for i in 0..cfp.Length-1 do
        newCfp.[i] <- cfp.[i]
      newCfp.[cfp.Length] <- rp
      newCfp

  // wraps around the user's merge function with option type for AsyncSeq scan
  let optionMerge (input:'a option*'a option) : 'a option =
    match input with
    | None, None -> None
    | Some a, None -> Some a
    | None, Some b -> Some b
    | Some a, Some b -> (a,b) |> outputMerge |> Some

  // wraps around user's progressHandler function to only excute when a exists
  let reactorProgressHandler ((a,cf): 'a option*ChangefeedPosition) : Async<Unit> = async {
    match a with
    | None -> ()
    | Some a -> do! progressHandler (a,cf) }

  // updates changefeed position and add the new element to the list of outputs
  let accumPartitionsPositions (merge: 'a option*'a option -> 'a option) (output:'a option, cfp:ChangefeedPosition) (handlerOutput: 'a, pp:RangePosition) =
    merge (output,(Some handlerOutput)) , (updateChangefeedPosition cfp pp)

  //basically always takes the latter one (renew state)
  let stateFlatten _ out = out

  // used to accumulate the output of all the user handle functions
  let progressReactor = Reactor.mk

  // wrap the document handler so that it takes and passes out partition position
  let handle ((_doc, pp) as input) = async {
    let! ret = handle input
    (ret, pp) |> Reactor.send progressReactor
  }

  let initialPosition = 
    match config.StartingPosition with
    | Beginning -> [||]
    | ChangefeedPosition cfp -> cfp

  let! progressTracker =
    progressReactor
    |> Reactor.recv
    |> AsyncSeq.scan (accumPartitionsPositions optionMerge) (None,initialPosition)
    |> AsyncSeq.bufferByTimeFold (int config.ProgressInterval.TotalMilliseconds) stateFlatten (None,initialPosition)
    |> AsyncSeq.iterAsync reactorProgressHandler
    |> Async.StartChild

  let! partitions = getPartitions state
  let workers =
    partitions
    |> partitionSelector
    |> Array.map ((fun pkr -> readPartition config state pkr) >> AsyncSeq.iterAsync handle)
    |> Async.Parallel
    |> Async.Ignore

  return! Async.choose progressTracker workers
}

/// Periodically queries DocDB for the latest positions and timestamp of all partitions in its changefeed.
/// The `handler` function will be called periodically, once per `interval`, with an updated ChangefeedPosition and timestamp of last document
let trackTailPosition (cosmos:CosmosEndpoint) (interval:TimeSpan) (handler:DateTime*(DateTime*RangePosition)[] -> Async<unit>) = async {
  use client = 
    let connPolicy = 
        let cp = ConnectionPolicy.Default
        cp.ConnectionMode <- ConnectionMode.Direct
        cp.ConnectionProtocol <- Protocol.Tcp
        cp.MaxConnectionLimit <- 1000
        cp
    new DocumentClient(cosmos.uri, cosmos.authKey, connPolicy, Nullable ConsistencyLevel.Session)
  let state = {
    client = client
    collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
  }

  let getRecentPosition (pkr:PartitionKeyRange) = async {
    let cfo = ChangeFeedOptions(PartitionKeyRangeId = pkr.Id, StartTime = Nullable (DateTime.Now.AddHours(1.)))
    use query = client.CreateDocumentChangeFeedQuery(state.collectionUri, cfo)
    let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask
    let rp : RangePosition = {
      RangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
      RangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64
      LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
    }
    let cfoForLastDoc = ChangeFeedOptions(PartitionKeyRangeId = pkr.Id, RequestContinuation = string (rp.LastLSN - 1L))
    use queryForLastDoc = client.CreateDocumentChangeFeedQuery(state.collectionUri, cfoForLastDoc)
    let! responseForLastDoc = queryForLastDoc.ExecuteNextAsync<Document>() |> Async.AwaitTask
    let lastDocument = 
        Seq.last <| responseForLastDoc.ToArray()
    return lastDocument.Timestamp, rp
  }

  let getRecentPosition pkr =
    getRecentPosition pkr
    |> Async.timeoutAfter (TimeSpan.FromSeconds(3.)) // times out after 3 seconds, shouldnt take more than 1

  let handler input =
    handler input
    |> Async.timeoutAfter (TimeSpan.FromTicks(interval.Ticks * 3L))

  let! partitions = getPartitions state   // NOTE: partitions will only be fetched once. Consider moving this inside the query function in case of a docdb partition split.
  let queryPartitions dateTime = async {
    let! changefeedPosition =
      partitions
      |> Array.map getRecentPosition
      |> Async.Parallel
    return (dateTime, changefeedPosition)
  }

  return!
    AsyncSeq.intervalMs (int interval.TotalMilliseconds)
    |> AsyncSeq.mapAsyncParallel queryPartitions
    |> AsyncSeq.iterAsync handler
}