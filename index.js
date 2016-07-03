require('setimmediate')
var debug = require('debug')('DynamoDBStream')
var _ = require('lodash')
var async = require('async')
var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var DynamoDBValue = require('dynamodb-value')

module.exports = DynamoDBStream

inherits(DynamoDBStream, EventEmitter)
function DynamoDBStream(ddbStreams, streamArn) {
	if ( !(this instanceof DynamoDBStream) ) return new DynamoDBStream(ddbStreams, streamArn)

	EventEmitter.call(this)

	if (typeof ddbStreams !== 'object') throw new Error('missing DynamoDBStreams instance')
	if (typeof streamArn !== 'string') throw new Error('missing stream arn, expected string but it was ' + streamArn)

	this._ddbStreams = ddbStreams
	this._streamArn = streamArn
	this._shards = {}
	this._shardsCount = 0

	this._emitErrorFunctor = _.bind(this._emitErrorEvent, this)
	this._fetchStreamShardsFunctor = _.bind(this.fetchStreamShards, this)
	this._fetchStreamRecordsFunctor = _.bind(this.fetchStreamRecords, this)

	debug('created stream instance %s', streamArn)
}

/**
 * this will update the stream, shards and records included
 *
 */
DynamoDBStream.prototype.fetchStreamState = function (callback) {
	debug('fetchStreamState')

	if (callback && typeof callback !== 'function') {
		throw new Error('invalid argument, can be a callback function or nothing')
	}

	async.series([
		this._fetchStreamShardsFunctor,
		this._fetchStreamRecordsFunctor
	], callback || this._emitErrorFunctor)
}

/**
 * get latest updates from the underlying stream
 *
 */
DynamoDBStream.prototype.fetchStreamRecords = function (callback) {
	debug('fetchStreamRecords')

	if (callback && typeof callback !== 'function') {
		throw new Error('invalid argument, can be a callback function or nothing')
	}

	if (this._shardsCount === 0) {
		debug('no shards found')
		return setImmediate(callback)
	}

	var params = {
		StreamArn: this._streamArn
	}

	var self = this

	var work = [
		_.bind(this._getShardIterators, this),
		_.bind(this._getRecords, this)
	]

	async.waterfall(work, function (err, records) {
		if (err) {
			return callback ? callback(err) : self._emitErrorEvent(err)
		}

		debug(records)

		self._trimShards()

		setImmediate(_.bind(self._emitRecordEvents, self, records))

		if (callback) {
			callback(null, records);
		}
	})
}

/** 
 * update the shard state of the stream
 * this will emit new shards / remove shards events
 */
DynamoDBStream.prototype.fetchStreamShards = function(callback) {
	debug('fetchStreamShards')

	if (callback && typeof callback !== 'function') {
		throw new Error('invalid argument, can be a callback function or nothing')
	}

	this._trimShards()

	var params = {
		StreamArn: this._streamArn
	}

	var self = this

	this._ddbStreams.describeStream(params, function(err, data) {
		if (err) {
			return callback ? callback(err) : self._emitErrorEvent(err)
		}
		
		debug('describeStream data')
		
		var shards = data.StreamDescription.Shards
		var newShardIds = []

		// collect all the new shards of this stream
		for (var i = 0; i < shards.length; i++) {
			var newShardEntry = shards[i]
			var shardEntry = self._shards[newShardEntry.ShardId]

			if (!shardEntry) {
				self._shards[newShardEntry.ShardId] = {
					shardId: newShardEntry.ShardId
				}
				newShardIds.push(newShardEntry.ShardId)
			}
		}

		if (newShardIds.length > 0) {
			self._shardsCount += newShardIds.length
			debug('Added %d new shards', newShardIds.length)
			self._emitNewShardsEvent(newShardIds)
		}

		if (callback) {
			callback()
		}
	})
}

DynamoDBStream.prototype._getShardIterators = function (callback) {
	debug('_getShardIterators')
	
	async.eachLimit(this._shards, 10, _.bind(this._getShardIterator, this), callback)
}

DynamoDBStream.prototype._getShardIterator = function(shardData, callback) {
	debug('_getShardIterator')
	debug(shardData)

	// no need to get an iterator if this shard already has NextShardIterator
	if (shardData.nextShardIterator) {
		debug('shard %s already has an iterator, skipping', shardData.shardId)
		return callback()
	}
	
	var params = {
		ShardId: shardData.shardId,
		ShardIteratorType: 'LATEST',
		StreamArn: this._streamArn
	}

	this._ddbStreams.getShardIterator(params, function (err, result) {
		if (err) return callback(err)
		shardData.nextShardIterator = result.ShardIterator
		callback()
	})
}

DynamoDBStream.prototype._getRecords = function (callback) {
	debug('_getRecords')
	
	var records = []

	async.eachLimit(this._shards, 10, _.bind(this._getShardRecords, this, records), function (err) {
		if (err) return callback(err)
		callback(null, records)
	})
}

DynamoDBStream.prototype._getShardRecords = function (records, shardData, callback) {
	debug('_getShardRecords')
	var self = this

	this._ddbStreams.getRecords({ ShardIterator: shardData.nextShardIterator }, function (err, result) {
		if (err) return callback(err)
			
		if (result.Records) {
			records.push.apply(records, result.Records)

			if (result.NextShardIterator) {
				shardData.nextShardIterator = result.NextShardIterator
			} else {
				shardData.nextShardIterator = null
			}
		}

		callback()
	})
}

DynamoDBStream.prototype._trimShards = function () {
	debug('_trimShards')
	
	var removedShards = []
	
	for (var shardId in this._shards) {
		var shardData = this._shards[shardId]

		if (shardData.nextShardIterator === null) {
			debug('deleting shard %s', shardId)
			delete this._shards[shardId]
			this._shardsCount--
			removedShards.push(shardId)
		}
	}

	if (removedShards.length > 0) {
		this._emitRemoveShardsEvent(removedShards)
	}
}

DynamoDBStream.prototype._emitRecordEvents = function (events) {
	debug('_emitRecordEvents')
	
	for (var i = 0; i < events.length; i++) {
		var event = events[i]
	
		switch (event.eventName) {
			case 'INSERT':
				this.emit('insert record', DynamoDBValue.toJavascript(event.dynamodb.NewImage))
				break

			case 'MODIFY':
				var newRecord = DynamoDBValue.toJavascript(event.dynamodb.NewImage)
				var oldRecord = DynamoDBValue.toJavascript(event.dynamodb.OldImage)
				this.emit('modify record', newRecord, oldRecord)
				break

			case 'REMOVE':
				this.emit('remove record', DynamoDBValue.toJavascript(event.dynamodb.OldImage))
				break

			default:
				 this._emitErrorEvent(new Error('unknown dynamodb event ' + event.eventName))
		}
	}
}

DynamoDBStream.prototype._emitErrorEvent = function (err) {
	this.emit('error', err)
}

DynamoDBStream.prototype._emitNewShardsEvent = function (shardIds) {
	setImmediate(_.bind(this.emit, this, 'new shards', shardIds))
}

DynamoDBStream.prototype._emitRemoveShardsEvent = function (shardIds) {
	setImmediate(_.bind(this.emit, this, 'remove shards', shardIds))
}

DynamoDBStream.prototype.getShardState = function () {
	return _.cloneDeep(this._shards)
}

DynamoDBStream.prototype.setShardState = function (shards) {
	this._shardsCount = Object.keys(shards).length
	this._shards = shards
}