const test = require('ava')
const DynamoDBStream = require('./index')
const aws = require('aws-sdk')
const debug = require('debug')('DynamoDBStream:test')

const ddbStreams = new aws.DynamoDBStreams()
const ddb = new aws.DynamoDB()

const TABLE_NAME = 'testDynamoDBStream'

test('reports the correct stream of changes', async t => {
	const { eventLog, ddbStream } = t.context
	const pk = Date.now().toString()

	await ddbStream.fetchStreamState()
	await putItem({ pk, data: '1' })
	await ddbStream.fetchStreamState()
	await putItem({ pk, data: '2' })
	await ddbStream.fetchStreamState()
	await putItem({ pk: 'a', data: '2' })
	await putItem({ pk: 'b', data: '2' })
	await ddbStream.fetchStreamState()
	await deleteItem('a')
	await ddbStream.fetchStreamState()

	t.deepEqual(eventLog, [
		{ eventName: 'insert record', record: { pk, data: '1' } },
		{ 
			eventName: 'modify record', 
			newRecord: { pk, data: '2' },
			oldRecord: { pk, data: '1' }
		},
		{ eventName: 'insert record', record: { pk: 'a', data: '2' } },
		{ eventName: 'insert record', record: { pk: 'b', data: '2' } },
		{ eventName: 'remove record', record: { pk: 'a', data: '2' } }
	])
})

test.beforeEach(async t => {

	t.context = {
		eventLog: []
	}

	await createTable()
	const arn = await findStreamArn()
	const ddbStream = t.context.ddbStream = new DynamoDBStream(ddbStreams, arn)

	ddbStream.on('insert record', (record) => {
		t.context.eventLog.push({ eventName: 'insert record', record })
	})

	ddbStream.on('modify record', (newRecord, oldRecord) => {
		t.context.eventLog.push({
			eventName: 'modify record',
			newRecord,
			oldRecord
		})
	})

	ddbStream.on('remove record', (record) => {
		t.context.eventLog.push({ eventName: 'remove record', record })
	})

	ddbStream.on('new shards', (newShards) => {
		t.context.shards = newShards
	})
})


/**
 * create the test table and wait for it to become active
 *
 */
async function createTable() {
	debug('creating table...')

	const params = {
		TableName: TABLE_NAME,
		KeySchema: [{
			AttributeName: 'pk',
			KeyType: 'HASH',
		}],
		AttributeDefinitions: [{
			AttributeName: 'pk',
			AttributeType: 'S', // (S | N | B) for string, number, binary
		}],
		ProvisionedThroughput: { // required provisioned throughput for the table
			ReadCapacityUnits: 1,
			WriteCapacityUnits: 1,
		},
		StreamSpecification: {
			StreamEnabled: true,
			StreamViewType: 'NEW_AND_OLD_IMAGES'
		}
	}
	try {
		await ddb.createTable(params).promise()
		debug('table created.')
		await waitForTable(true)
	} catch (e) {
		if (!isTableExistError(e)) {
			throw e
		}

		debug('table already exists, skipping creation.')
	}
}

async function findStreamArn(callback) {
	debug('finding the right stream arn')
	const { Streams } = await ddbStreams.listStreams().promise()

	debug('found %d streams', Streams.length)

	const stream = Streams.filter(item => item.TableName === TABLE_NAME)[0]

	debug(stream)

	if (!stream) {
		throw new Error('cannot find stream arn')
	}

	debug('stream arn for table %s was found', TABLE_NAME)
	return stream.StreamArn
}

/**
 * delete the test table and wait for its disappearance
 *
 */
async function deleteTable(callback) {
	const params = {
		TableName: TABLE_NAME
	}
	debug('deleting table %s', TABLE_NAME)
	await ddb.deleteTable(params).promise()
	await waitForTable(false)
}

/**
 * wait for a table's state (exist/dont exist)
 * if the table is already in that state this function should return quickly
 *
 */
async function waitForTable(exists) {
	debug('waiting for table %s...', exists ? 'to become available' : 'deletion')

	// Waits for table to become ACTIVE.  
	// Useful for waiting for table operations like CreateTable to complete. 
	const params = {
		TableName: TABLE_NAME
	}

	// Supports 'tableExists' and 'tableNotExists'
	await ddb.waitFor(exists ? 'tableExists' : 'tableNotExists', params).promise()
	debug('table %s.', exists ? 'available' : 'deleted')
}

function putItem(data, callback) {
	const params = {
		TableName: TABLE_NAME,
		Item: {
			pk: {
				S: data.pk
			},
			data: {
				S: data.data
			}
		}
	}

	debug('putting item %o', params)

	return ddb.putItem(params).promise()
}

function deleteItem(pk, callback) {
	const params = {
		TableName: TABLE_NAME,
		Key: { pk: { S: pk } }
	}

	debug('deleting item %o', params)

	return ddb.deleteItem(params).promise()
}

function isTableExistError(err) {
	return err && err.code === 'ResourceInUseException' && err.message && err.message.indexOf('Table already exists') > -1
}