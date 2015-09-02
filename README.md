# DynamoDBStream

A wrapper around low level aws sdk that makes it easy to consume a dynamodb-stream.

### Example: Replicating small tables

fetchStreamState() should be invoked whenever the consumer wishes to get the updates.

When a consumer needs to maintain a replica of the table data, fetchStreamState() is invoked on regular intervals.

The current best practice for replication is to manage the state of the stream as it relates to the consumer in a separate dynamodb table (shard iterators/sequence numbers etc), so if a failure occurs, that consumer can get back to the point he was in the stream. However for small or even medium tables this is not necessary. One can simply reread the entire table on startup.

This different approach make things more "stateless" and slightly simpler (in my view):

- call fetchStreamState() first, one may safely disregard any events that are emitted at this stage. Under the hood DynamoDBStream uses ```ShardIteratorType: LATEST``` to get shard iterators for all the current shards of the stream. These iterators act as a "bookmark" in the stream.
- Obtain an initial copy of the table's data (via a dynamodb scan api call for example) and store it locally
- call fetchStreamState() again, at this point some of the events might already be included in the initial local copy of the data and some won't. Depending on the data structure thats houses the local copy of data, flitering might need to be applied.
- start polling on fetchStreamState() and blindly mutate the local copy according to the updates

Wrapping the initial data scan with fetchStreamState() calls insures that no changes will be missed. At worst, the second call might yield some duplicates.

```javascript

var aws = require('aws-sdk')
var DynamoDBStream = require('dynamodb-stream')
var schedule = require('tempus-fugit').schedule
var deepDiff = require('deep-diff').diff

var pk = 'id'
var ddb = new aws.DynamoDB()
var ddbStream = new DynamoDBStream(new aws.DynamoDBStreams(), 'my stream arn')

var localState = {}

// fetch stream state initially
ddbStream.fetchStreamState(function (err) {
    if (err) {
        console.error(err)
        return process.exit(1)
    }
    
    // fetch all the data
    ddb.scan({ TableName: 'foo' }, function (err, results) {
        localState = // parse result and store in localSate
        
        // do this every 1 minute, starting from the next round minute
        schedule({ minute: 1 }, function (job) {
            ddbStream.fetchStreamState(job.callback())
        })
    })    
})

ddbStream.on('insert record', function (data) {
    localState[data.id] = data
})

ddbStream.on('remove record', function (data) {
    delete localState[data.id]
})

ddbStream.on('modify record', function (newData, oldData) {
    var diff = deepDiff(oldData, newData)
    if (diff) {
        // handle the diffs
    }
})

ddbStream.on('new shards', function (shardIds) {})
ddbStream.on('remove shards', function (shardIds) {})

```

### Example: shards / iterator persistence on dynamodb

The fetchStreamState() can accept a callback function, that will be invoked prior to event emission. Among other things, this callback is designed to be used as a hook point for persisting the iterator's state

```javascript
var schedule = require('tempus-fugit').schedule
var aws = require('aws-sdk')
var DynamoDBStream = require('dynamodb-stream')
var ddb = new aws.DynamoDB()
var ddbStream = new DynamoDBStream(new aws.DynamoDBStreams(), 'my stream arn')

ddbStream.fetchStreamState(function (err) {
    if (err) {
        return console.error(err)
    }

    var state = ddbStream.getShardState()

    // save state here
})

```

#### TODO
 - make sure the aggregation of records is in order - the metadata from the stream might be helpful (order by sequence number?)
 - maybe only update the stream state after the callback of fetchStreamState():
 
 ```javascript
 // internally dont replace iterators before process is complete without errors
 ddbStream.fetchStreamState(function (err, callback) {
    // dont' replace iterators before callback is invoked?
    // what happens if fetchStreamState is call again when we're persisting the older state?
 })
 ```

#### Wishlist to DynamoDB team:
1. expose push interface so one won't need to poll the stream api
2. obtain a sequence number from a dynamodb api scan operation
