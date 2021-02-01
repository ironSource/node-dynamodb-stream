const DynamoDBStream = require('dynamodb-stream')
const { DynamoDBStreams } = require('@aws-sdk/client-dynamodb-streams')
const { unmarshall } = require('@aws-sdk/util-dynamodb')
const fs = require('fs').promises

const STREAM_ARN = 'your stream arn'
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

	const fetchStreamState = async () => {
		await ddbStream.fetchStreamState()
		const shardState = ddbStream.getShardState()
		await fs.writeFile(FILE, JSON.stringify(shardState))
		setTimeout(fetchStreamState, 1000 * 20)
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