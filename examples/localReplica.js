const DynamoDBStream = require('../index')
const { DynamoDB } = require('@aws-sdk/client-dynamodb')
const { DynamoDBStreams } = require('@aws-sdk/client-dynamodb-streams')
const { unmarshall } = require('@aws-sdk/util-dynamodb')

const STREAM_ARN = 'your stream ARN'
const TABLE_NAME = 'your table name'

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
	const watchStream = async () => {
		await ddbStream.fetchStreamState()
		setTimeout(watchStream, 10 * 1000)		
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