var bitfield = require('sparse-bitfield')
var uint64be = require('uint64be')
var assert = require('assert')
var mutexify = require('mutexify')
var raf = require('random-access-file')
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

MapTiles.prototype.put = function (q, tile, cb) {
  var quadkey = q.quadkey
  if (!quadkey) {
    assert(typeof q.z === 'number')
    assert(typeof q.x === 'number')
    assert(typeof q.y === 'number')
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }
  assert(Buffer.isBuffer(tile))

  this.lock(function (release) {
    this._write(quadkey, tile, cb)
  })
}

MapTiles.prototype.end = function (cb) {
  // hey we are done writing for now, cool. Write the index
}

MapTiles.prototype._write = function (quadkey, tile, cb) {
  // need a way to build the index and then write it?
  // first lets assume there is no index yet...
  var indexPosition = utils.getIndexPosition(quadkey)
  var offset = HEADER_SIZE + METADATA_SIZE + (indexPosition * constants.ENTRY_LENGTH)
  this.storage.write(tile, indexPosition, cb)
}

MapTiles.prototype.createFile = function (file, offsetBytes, cb) {
  var header = Object.assign({}, defaultHeader, {
    offsetBytes: offsetBytes
  })
  var headerBuf = encoder.encodeBlock(header)
  var buf = Buffer.allocUnsafe(MIN_FILE_SIZE).fill(0)
  headerBuf.copy(buf)
  constants.METADATA_BLOCK.copy(buf, defaultHeader.metadataOffset)
  constants.INDEX_BLOCK.copy(buf, defaultHeader.firstIndexOffset)
  this.storage.write(0, buf, cb)
}

MapTiles.prototype._writeMetadata = function (metadata, cb) {
  var self = this
  self._readHeader(function (err, header) {
    if (err) return cb(err)
    self._readMetadata(function (err, existingMetadata) {
      if (err) return cb(err)
      var merged = Object.assign({}, defaultMetadata, existingMetadata, metadata)
      var buf = encoder.encodeBlock(merged)
      self.storage.write(header.metadataOffset, buf, cb)
    })
  })
}

MapTiles.prototype.get = function (q, cb) {
  var quadkey = q.quadkey
  if (!quadkey) {
    assert(typeof q.z === 'number')
    assert(typeof q.x === 'number')
    assert(typeof q.y === 'number')
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }

  this._read(quadkey, cb)
}

MapTiles.prototype._read = function (quadkey, cb) {
  var self = this
  self._readFirstIndexOffset(function (err, offset) {
    if (err) return cb(err)
    self._readAndParseBlock(offset, MAX_HEADER_SIZE, onParseBlock)
  })

  function onParseBlock (err, block, offset) {
    if (err) return cb(err)
    if (constants.TILE_BLOCK.equals(block.type)) {
      return self.storage.read(offset + TILE_HEADER_SIZE, block.length, cb)
    }
    if (!constants.INDEX_BLOCK.equals(block.type)) {
      return cb(new Error('Unexpected block type ' + block.type))
    }
    // KM: why not just check if there's a parentOffset?
    if (quadkey.length <= block.firstQuadkey.length &&
      quadkey !== block.firstQuadkey) {
      if (block.parentOffset) {
        // KM: this should be INDEX_HEADER_SIZE, cause we know it's an INDEX at this point
        return self._readAndParseBlock(block.parentOffset, MAX_HEADER_SIZE, onParseBlock)
      } else {
        return cb(new Error('NotFound'))
      }
    }
    self._readTileOffsetFromIndex(quadkey, block, offset, function (err, nextOffset) {
      if (err) return cb(err)
      if (!nextOffset) return cb(new Error('NotFound'))
      // KM: this should be TILE_HEADER_SIZE, cause we know it's a TILE at this point
      self._readAndParseBlock(nextOffset, MAX_HEADER_SIZE, onParseBlock)
    })
  }
}

MapTiles.prototype._readTileOffsetFromIndex = function (quadkey, block, offset, cb) {
  var indexPosition = utils.getIndexPosition(
    quadkey,
    block.firstQuadkey,
    block.depth
  )
  if (typeof indexPosition === 'undefined') {
    return cb(new Error('NotFound'))
  }
  // This is the offset that contains the offset of the tile.

  // TODO: for sparse indexes, the index position shoudl be managed
  // to not have a bunch of empty blocks

  var tileOffsetOffset = offset + INDEX_HEADER_SIZE + (indexPosition * constants.ENTRY_LENGTH)
  this.storage.read(tileOffsetOffset, block.entryLength, function (err, buf) {
    if (err) return cb(err)
    var offset = (buf.length === 4)
      ? buf.readUInt32BE(0)
      : uint64be.decode(buf, 0)
    cb(null, offset)
  })
}

MapTiles.prototype._readFirstIndexOffset = function (cb) {
  var self = this
  self._readMetadata(function (err, metadata, metadataOffset) {
    if (err) return cb(err)
    var firstBlockOffset = metadataOffset + metadata.length
    self._readAndParseBlock(firstBlockOffset, MAX_HEADER_SIZE, onParseBlock)
  })

  function onParseBlock (err, block, offset) {
    if (err) return cb(err)
    if (constants.INDEX_BLOCK.equals(block.type)) {
      return cb(null, offset)
    } else if (!block.length) {
      return cb(new Error('Could not find Index Block in file'))
    }
    var nextOffset = block.length + offset
    self._readAndParseBlock(nextOffset, MAX_HEADER_SIZE, onParseBlock)
  }
}

MapTiles.prototype._readHeader = function (cb) {
  this._readAndParseBlock(0, HEADER_SIZE, function (err, header) {
    if (err) return cb(err)
    if (!header.magicNumber || !header.magicNumber.equals(constants.MAGIC_NUMBER)) {
      return cb(new Error('Unrecognized filetype'))
    }
    cb(null, header)
  })
}

MapTiles.prototype._readMetadata = function (cb) {
  var self = this
  self._readHeader(function (err, header) {
    if (err) return cb(err)
    self._readAndParseBlock(header.metadataOffset, METADATA_SIZE, cb)
  })
}

MapTiles.prototype._readAndParseBlock = function (offset, length, cb) {
  var self = this
  self.storage.read(offset, length, function (err, buf) {
    if (err) return cb(err)
    try {
      var parsed = encoder.parseBlock(buf)
    } catch (e) {
      return cb(e)
    }
    cb(null, parsed, offset)
  })
}

MapTiles.prototype.createWriteStream = function () {
  // TODO
}

MapTiles.prototype.createReadStream = function () {
  // TODO
}
