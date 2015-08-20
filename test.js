var expect = require('chai').expect
var DynamoDBStream = require('./index.js')
var inspect = require('util').inspect
var aws = require('aws-sdk')
var config = require('rc')('dynamodb-stream')
var async = require('async')
var _ = require('lodash')
var debug = require('debug')('DynamoDBStream:test')

aws.config.update(config)

var ddbStreams = new aws.DynamoDBStreams()
var ddb = new aws.DynamoDB()

var TABLE_NAME = 'testDynamoDBStream'

describe('DynamoDBStream', function() {
	this.timeout(200000)

	var ds, results, shards

	it('reports the correct stream of changes', function(done) {
		var pk = Date.now().toString()

		async.series([
			_.bind(ds.fetchStreamState, ds),

			// put an item initiall then check the stream for an insert event
			_.partial(putItem, { pk: pk, data: '1' }),
			_.bind(ds.fetchStreamState, ds),

			// put another item, we expect the result not to include the initial insert
			// and also be a modify event rather than insert			
			_.partial(putItem, { pk: pk, data: '2' }),
			_.bind(ds.fetchStreamState, ds),

			// put two more items now, this should yield a stream with two update records
			_.partial(putItem, { pk: 'a', data: '2' }),
			_.partial(putItem, { pk: 'b', data: '2' }),
			_.bind(ds.fetchStreamState, ds),

			_.partial(deleteItem, 'a'),
			_.bind(ds.fetchStreamState, ds)

		], function (err) {
			if (err) return done(err)
				
			setImmediate(function () {

				expect(results[0]).to.have.property('eventName', 'insert record')
				expect(results[0]).to.have.property('newRecord').to.eql({ pk: pk, data: '1' })
				
				expect(results[1]).to.have.property('eventName', 'modify record')
				expect(results[1]).to.have.property('newRecord').to.eql({ pk: pk, data: '2' })
				expect(results[1]).to.have.property('oldRecord').to.eql({ pk: pk, data: '1' })

				expect(results[2]).to.have.property('eventName', 'insert record')
				expect(results[2]).to.have.property('newRecord').to.eql({ pk: 'a', data: '2' })
				
				expect(results[3]).to.have.property('eventName', 'insert record')
				expect(results[3]).to.have.property('newRecord').to.eql({ pk: 'b', data: '2' })
				
				expect(results[4]).to.have.property('eventName', 'remove record')
				expect(results[4]).to.have.property('oldRecord').to.eql({ pk: 'a', data: '2' })
				
				done()
			})
		})
	})

	beforeEach(function(done) {
		results = []
		shards = []
		
		async.series([
			createTable,
			findStreamArn
		], function(err, series) {
			if (err) return done(err)

			ds = new DynamoDBStream(ddbStreams, series[1])

			ds.on('insert record', function (record) {
				results.push({
					eventName: 'insert record',
					newRecord: record
				})
			})

			ds.on('modify record', function (newRecord, oldRecord) {
				results.push({
					eventName: 'modify record',
					newRecord: newRecord,
					oldRecord: oldRecord
				})
			})

			ds.on('remove record', function (record) {
				results.push({
					eventName: 'remove record',
					oldRecord: record
				})
			})

			ds.on('new shards', function (newShards) {
				shards = shards.concat(newShards)
			})


			done()
		})
	})

	afterEach(function(done) {
		deleteTable(done)
	})

	function getEvents(batchName) {
		return function (callback) {
			ds.getEvents(function (err, events) {
				if (err) return callback(err)
				results[batchName] = events
				callback(null, events)
			})
		}
	} 
})

/**
 * create the test table and wait for it to become active
 *
 */
function createTable(callback) {
	debug('creating table...')

	var params = {
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

	ddb.createTable(params, function(err, data) {
		if (err) {
			if (!isTableExistError(err)) {
				return callback(err)
			}

			debug('table already exists, skipping creation.')
		} else {
			debug('table created.')
		}

		waitForTable(true, callback)
	})
}

function findStreamArn(callback) {
	debug('finding the right stream arn')
	ddbStreams.listStreams(function(err, data) {
		if (err) return callback(err)

		debug('found %d streams', data.Streams.length)

		var stream = _.find(data.Streams, 'TableName', TABLE_NAME)

		debug(stream)

		if (!stream) {
			return callback(new Error('cannot find stream arn'))
		}

		debug('stream arn for table %s was found', TABLE_NAME)
		callback(null, stream.StreamArn)
	})
}

/**
 * delete the test table and wait for its disappearance
 *
 */
function deleteTable(callback) {
	var params = {
		TableName: TABLE_NAME
	}

	ddb.deleteTable(params, function(err) {
		if (err) return callback(err)

		waitForTable(false, callback)
	})
}

/**
 * wait for a table's state (exist/dont exist)
 * if the table is already in that state this function should return quickly
 *
 */
function waitForTable(exists, callback) {
	debug('waiting for table %s...', exists ? 'to become available' : 'deletion')
	
	// Waits for table to become ACTIVE.  
	// Useful for waiting for table operations like CreateTable to complete. 
	var params = {
		TableName: TABLE_NAME
	}

	// Supports 'tableExists' and 'tableNotExists'
	ddb.waitFor(exists ? 'tableExists' : 'tableNotExists', params, function(err) {
		if (err) return callback(err)
		debug('table %s.', exists ? 'available' : 'deleted')
		callback()
	})
}

function isTableExistError(err) {
	return err && err.code === 'ResourceInUseException' && err.message && err.message.indexOf('Table already exists') > -1
}

function putItem(data, callback) {
	var params = {
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

	debug('putting item %s', inspect(params))

	ddb.putItem(params, callback)
}

function deleteItem(pk, callback) {
	var params = {
		TableName: TABLE_NAME,
		Key: {  pk: { S: pk } }
	}

	ddb.deleteItem(params, callback)
}
