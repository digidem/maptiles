var uint64be = require('uint64be')
var assert = require('assert')
var mutexify = require('mutexify')
var raf = require('random-access-file')
var defs = require('maptiles-spec').structure
var constants = require('maptiles-spec').constants

var utils = require('./lib/utils')
var encoder = require('./lib/encode_decode')

var HEADER_SIZE = utils.getBlockSize('header')
var METADATA_SIZE = utils.getBlockSize('metadata')
var TILE_HEADER_SIZE = utils.getBlockSize('tileBlock')
var INDEX_HEADER_SIZE = utils.getBlockSize('indexBlock')
var MAX_HEADER_SIZE = Math.max(HEADER_SIZE, METADATA_SIZE, TILE_HEADER_SIZE, INDEX_HEADER_SIZE)
// KM: what is 8??
var MIN_FILE_SIZE = HEADER_SIZE + METADATA_SIZE + INDEX_HEADER_SIZE + 8

var defaultHeader = {
  magicNumber: constants.MAGIC_NUMBER,
  version: '1.0.0',
  metadataOffset: HEADER_SIZE,
  firstIndexOffset: HEADER_SIZE + METADATA_SIZE
}

var defaultMetadata = {
  type: constants.METADATA_BLOCK
}

module.exports = MapTiles

function MapTiles (storage) {
  if (!(this instanceof MapTiles)) return new MapTiles(storage)
  this.storage = typeof storage === 'string' ? raf(storage) : storage
  this.lock = mutexify()
}

MapTiles.prototype.put = function (q, tile, callback) {
  var quadkey = q.quadkey
  if (!quadkey) {
    assert(typeof q.z === 'number')
    assert(typeof q.x === 'number')
    assert(typeof q.y === 'number')
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }
  assert(Buffer.isBuffer(tile))

  this.lock(function (release) {
    this._write(quadkey, tile, callback)
  })
}

MapTiles.prototype._write = function (quadkey, tile, callback) {
  // TODO
}

MapTiles.prototype._createFile = function (file, offsetBytes, callback) {
  var header = Object.assign({}, defaultHeader, {
    offsetBytes: offsetBytes
  })
  var headerBuf = encoder.encodeBlock(header)
  var buf = Buffer.allocUnsafe(MIN_FILE_SIZE).fill(0)
  headerBuf.copy(buf)
  constants.METADATA_BLOCK.copy(buf, defaultHeader.metadataOffset)
  constants.INDEX_BLOCK.copy(buf, defaultHeader.firstIndexOffset)
  file.write(0, buf, callback)
}

MapTiles.prototype._writeMetadata = function (metadata, callback) {
  var self = this
  self._readHeader(function (err, header) {
    if (err) return callback(err)
    self._readMetadata(function (err, existingMetadata) {
      if (err) return callback(err)
      var merged = Object.assign({}, defaultMetadata, existingMetadata, metadata)
      var buf = encoder.encodeBlock(merged)
      self.storage.write(header.metadataOffset, buf, callback)
    })
  })
}

MapTiles.prototype.get = function (q, callback) {
  var quadkey = q.quadkey
  if (!quadkey) {
    assert(typeof q.z === 'number')
    assert(typeof q.x === 'number')
    assert(typeof q.y === 'number')
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }

  this._read(quadkey, callback)
}

MapTiles.prototype._read = function (quadkey, callback) {
  var self = this
  self._readFirstIndexOffset(function (err, indexOffset) {
    if (err) return callback(err)
    self._readAndParseBlock(indexOffset, MAX_HEADER_SIZE, onParseBlock)
  })

  function onParseBlock (err, block, offset) {
    if (err) return callback(err)
    if (constants.TILE_BLOCK.equals(block.type)) {
      return self.storage.read(offset + TILE_DATA_OFFSET, block.length, callback)
    }
    if (!constants.INDEX_BLOCK.equals(block.type)) {
      return callback(new Error('Unexpected block type ' + block.type))
    }
    if (quadkey.length <= block.firstQuadkey.length &&
      quadkey !== block.firstQuadkey) {
      if (block.parentOffset) {
        return self._readAndParseBlock(block.parentOffset, MAX_HEADER_SIZE, onParseBlock)
      } else {
        return callback(new Error('NotFound'))
      }
    }
    self._readTileOffsetFromIndex(quadkey, block, offset, function (err, nextOffset) {
      if (err) return callback(err)
      if (!nextOffset) return callback(new Error('NotFound'))
      self._readAndParseBlock(nextOffset, MAX_HEADER_SIZE, onParseBlock)
    })
  }
}

MapTiles.prototype._readTileOffsetFromIndex = function (quadkey, indexInfo, indexOffset, callback) {
  var indexPosition = utils.getIndexPosition(
    quadkey,
    indexInfo.firstTileQuadkey,
    indexInfo.depth
  )
  if (typeof indexPosition === 'undefined') {
    return callback(new Error('NotFound'))
  }
  // This is the offset in the file of a 4 or 8 byte buffer in the index that
  // contains the offset of the tile.
  var tileOffsetOffset = indexOffset + INDEX_HEADER_SIZE +
    (indexPosition * indexInfo.entryLength)
  this.storage.read(tileOffsetOffset, indexInfo.entryLength, function (err, buf) {
    if (err) return callback(err)
    var offset = (buf.length === 4)
      ? buf.readUInt32BE(0)
      : uint64be.decode(buf, 0)
    callback(null, offset)
  })
}

MapTiles.prototype._readFirstIndexOffset = function (callback) {
  var self = this
  self._readMetadata(function (err, metadata, metadataOffset) {
    if (err) return callback(err)
    var firstBlockOffset = metadataOffset + metadata.length
    self._readAndParseBlock(firstBlockOffset, 5, onParseBlock)
  })

  function onParseBlock (err, block, offset) {
    if (err) return callback(err)
    if (constants.INDEX_BLOCK.equals(block.type)) {
      return callback(null, offset)
    } else if (!block.length) {
      return callback(new Error('Could not find Index Block in file'))
    }
    self._readAndParseBlock(offset, 5, onParseBlock)
  }
}

MapTiles.prototype._readHeader = function (callback) {
  this._readAndParseBlock(0, HEADER_SIZE, function (err, header) {
    if (err) return callback(err)
    if (!header.magicNumber || !header.magicNumber.equals(constants.MAGIC_NUMBER)) {
      return callback(new Error('Unrecognized filetype'))
    }
    callback(null, header)
  })
}

MapTiles.prototype._readMetadata = function (callback) {
  var self = this
  self._readHeader(function (err, header) {
    if (err) return callback(err)
    self._readAndParseBlock(header.metadataOffset, METADATA_SIZE, callback)
  })
}

MapTiles.prototype._readAndParseBlock = function (offset, length, callback) {
  var self = this
  self.storage.read(offset, length, function (err, buf) {
    if (err) return callback(err)
    try {
      var parsed = encoder.parseBlock(buf)
    } catch (e) {
      return callback(e)
    }
    callback(null, parsed, offset)
  })
}

MapTiles.prototype.createWriteStream = function () {
  // TODO
}

MapTiles.prototype.createReadStream = function () {
  // TODO
}
