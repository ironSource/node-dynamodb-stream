# DynamoDBStream

A wrapper around low level aws sdk that makes it easy to consume a dynamodb-stream, even in a browser.

### Example: Replicating small tables

fetchStreamState() should be invoked whenever the consumer wishes to get the updates.

When a consumer needs to maintain a replica of the table data, fetchStreamState() is invoked on regular intervals.

The current best practice for replication is to manage the state of the stream as it relates to the consumer in a separate dynamodb table (shard iterators/sequence numbers etc), so if a failure occurs, that consumer can get back to the point he was in the stream. However for small or even medium tables this is not necessary. One can simply reread the entire table on startup.

This different approach make things more "stateless" and slightly simpler (in my view):

- call fetchStreamState() first, one may safely disregard any events that are emitted at this stage. Under the hood DynamoDBStream uses ```ShardIteratorType: LATEST``` to get shard iterators for all the current shards of the stream. These iterators act as a "bookmark" in the stream.
- Obtain an initial copy of the table's data (via a dynamodb scan api call for example) and store it locally
- call fetchStreamState() again, at this point some of the events might already be included in the initial local copy of the data and some won't. Depending on the data structure thats houses the local copy of data, some filtering might be needed.
- start polling on fetchStreamState() and blindly mutate the local copy according to the updates

Wrapping the initial data scan with fetchStreamState() calls insures that no changes will be missed. At worst, the second call might yield some duplicates.

```js
const DynamoDBStream = require('dynamodb-stream')
const { DynamoDB } = require('@aws-sdk/client-dynamodb')
const { DynamoDBStreams } = require('@aws-sdk/client-dynamodb-streams')
const { unmarshall } = require('@aws-sdk/util-dynamodb')

const STREAM_ARN = 'your stream ARN'
const TABLE_NAME = 'testDynamoDBStream'

async function main() {

  // table primary key is "pk"

  const ddb = new DynamoDB()
  const ddbStream = new DynamoDBStream(
    new DynamoDBStreams(),
    STREAM_ARN,
    unmarshall
  )

  const localState = new Map()
  await ddbStream.fetchStreamState()
  const { Items } = await ddb.scan({ TableName: TABLE_NAME })
  Items.map(unmarshall).forEach(item => localState.set(item.pk, item))
  
  // parse results and store in local state
  const watchStream = () => {
    console.log(localState)
    setTimeout(() => ddbStream.fetchStreamState().then(watchStream), 10 * 1000)
  }

  watchStream()

  ddbStream.on('insert record', (data, keys) => {
    localState.set(data.pk, data)
  })

  ddbStream.on('remove record', (data, keys) => {
    localState.remove(data.pk)
  })

  ddbStream.on('modify record', (newData, oldData, keys) => {
    localState.set(newData.pk, newData)
  })

  ddbStream.on('new shards', (shardIds) => {})
  ddbStream.on('remove shards', (shardIds) => {})
}

main()
```

### Example: shards / iterator persistence

If your program crash and you want to pick up where you left off then `setShardsState()` and `getShardState()` are here for the rescue (though, I haven't tested them yet but they should work... :) )

```js
const DynamoDBStream = require('dynamodb-stream')
const { DynamoDBStreams } = require('@aws-sdk/client-dynamodb-streams')
const { unmarshall } = require('@aws-sdk/util-dynamodb')
const fs = require('fs').promises

const STREAM_ARN = 'your stream ARN'
const FILE = 'shardState.json'

async function main() {
  const ddbStream = new DynamoDBStream(
    new DynamoDBStreams(),
    STREAM_ARN,
    unmarshall
  )

  // update the state so it will pick up from where it left last time
  // remember this has a limit of 24 hours or something along these lines
  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
  ddbStream.setShardState(await loadShardState())

  const fetchStreamState = () => {
    setTimeout(async () => {
      await ddbStream.fetchStreamState()
      const shardState = ddbStream.getShardState()
      await fs.writeFile(FILE, JSON.stringify(shardState))
      fetchStreamState()
    }, 1000 * 20)
  }

  fetchStreamState()
}

async function loadShardState() {
  try {
    return JSON.parse(await fs.readFile(FILE, 'utf8'))
  } catch (e) {
    if (e.code === 'ENOENT') return {}
    throw e
  }
}

main()
```

#### TODO
 - make sure the aggregation of records is in order - the metadata from the stream might be helpful (order by sequence number?)
 
#### Wishlist to DynamoDB team:
1. expose push interface so one won't need to poll the stream api
2. obtain a sequence number from a dynamodb api scan operation

[MIT](http://opensource.org/licenses/MIT) © ironSource ltd.
