var fs = require('fs')
var debug = require('debug')('maptiles')
var uint64be = require('uint64be')
var assert = require('assert')
var raf = require('random-access-file')
var constants = require('maptiles-spec').constants

var utils = require('./lib/utils')
var encoder = require('./lib/encode_decode')

var HEADER_SIZE = utils.getBlockSize('header')
var METADATA_SIZE = utils.getBlockSize('metadata')
var TILE_HEADER_SIZE = utils.getBlockSize('tileBlock')
var INDEX_HEADER_SIZE = utils.getBlockSize('indexBlock')

var defaultHeader = {
  magicNumber: constants.MAGIC_NUMBER,
  version: 1
}

var defaultMetadata = {
  type: constants.METADATA_BLOCK
}

module.exports = MapTiles

function MapTiles (filename, metadata) {
  if (!(this instanceof MapTiles)) return new MapTiles(filename, metadata)
  this.filename = filename
  this.storage = raf(filename)
  this.index = [] // lol memory on writes
  this.openSpaceOffset = 0
  this.writable = true
  this.maxDepth = 1
  this.metadata = metadata
}

MapTiles.prototype._length = function () {
  return fs.statSync(this.filename).size
}

MapTiles.prototype.put = function (q, tile, cb) {
  var self = this
  var quadkey = q.quadkey
  if (!quadkey) {
    q.z = Number(q.z)
    q.x = Number(q.x)
    q.y = Number(q.y)
    assert(!Number.isNaN(q.z))
    assert(!Number.isNaN(q.y))
    assert(!Number.isNaN(q.x))
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }
  assert(Buffer.isBuffer(tile))

  self._write(quadkey, tile, cb)
}

MapTiles.prototype.end = function (cb) {
  var self = this
  self.writable = false
  // write the index header
  var indexHeader = encoder.encodeBlock({
    type: constants.INDEX_BLOCK,
    entryLength: constants.ENTRY_LENGTH,
    depth: self.maxDepth,
    firstQuadkey: self.firstQuadkey,
    count: self.index.length
  })

  function done (err) {
    if (err) return cb(err)
    var offset = self.openSpaceOffset + (self.index.length * constants.ENTRY_LENGTH)
    debug('writing index header', offset, indexHeader)
    self.storage.write(offset, indexHeader, cb)
  }

  ;(function next (i) {
    if (i >= self.index.length) return done()
    var item = self.index[i]
    var offset = self.openSpaceOffset + (item.indexPosition * constants.ENTRY_LENGTH)
    var buf = Buffer.allocUnsafe(4).fill(0)
    buf.writeUInt32BE(item.offset, 0)
    debug('writing index', item.indexPosition, offset, item.offset)
    self.storage.write(offset, buf, function (err) {
      if (err) return done(err)
      next(i + 1)
    })
  })(0)
}

MapTiles.prototype._write = function (quadkey, tile, cb) {
  var self = this
  // first lets assume there is no index yet...
  if (!self.writable) return cb(new Error('Start a new maptiles instance to write, this one has already been closed.'))
  if (!self.firstQuadkey) this.firstQuadkey = quadkey
  self.maxDepth = Math.max(this.maxDepth, utils.quadkeyToTile(quadkey)[2])
  var tileHeader = {
    type: constants.TILE_BLOCK,
    length: tile.length
  }
  debug('quadkey', quadkey)
  var indexPosition = utils.getIndexPosition(quadkey)
  debug('index', indexPosition)
  var offset = HEADER_SIZE + METADATA_SIZE + self.openSpaceOffset
  self.index.push({indexPosition, offset})
  var buf = encoder.encodeBlock(tileHeader)
  debug('writing header at offset', offset, buf.length)
  self.storage.write(offset, buf, function (err) {
    if (err) return cb(err)
    var tileOffset = offset + TILE_HEADER_SIZE
    self.openSpaceOffset = tileOffset + tile.length
    console.log('writing tile', tileOffset, tile.length)
    self.storage.write(tileOffset, tile, cb)
  })
}

MapTiles.prototype._writeHeaderAndMetadata = function (cb) {
  var self = this
  var buf = Buffer.allocUnsafe(HEADER_SIZE + METADATA_SIZE).fill(0)
  var headerBuf = encoder.encodeBlock(Object.assign({}, defaultHeader, {
    metadataOffset: HEADER_SIZE,
    indexOffset: self.openSpaceOffset
  }))
  headerBuf.copy(buf, 0)
  var metadataBuf = encoder.encodeBlock(Object.assign({}, self.metadata, {
    type: constants.METADATA_BLOCK,
    length: METADATA_SIZE
  }))
  metadataBuf.copy(buf, HEADER_SIZE)
  debug('writing header', 0, buf.length)
  self.storage.write(0, buf, cb)
}

MapTiles.prototype._writeMetadata = function (metadata, cb) {
  var self = this
  self._readMetadata(function (err, existingMetadata) {
    if (err) return cb(err)
    var merged = Object.assign({}, defaultMetadata, existingMetadata, metadata)
    var buf = encoder.encodeBlock(merged)
    self.storage.write(0 + HEADER_SIZE, buf, cb)
  })
}

MapTiles.prototype.get = function (q, cb) {
  var quadkey = q.quadkey || q.q
  if (!quadkey) {
    q.z = Number(q.z)
    q.x = Number(q.x)
    q.y = Number(q.y)
    assert(!Number.isNaN(q.z))
    assert(!Number.isNaN(q.y))
    assert(!Number.isNaN(q.x))
    quadkey = utils.tileToQuadkey([q.x, q.y, q.z])
  }

  this._read(quadkey, cb)
}

MapTiles.prototype._readIndex = function (cb) {
  var self = this
  var length = this._length()
  var indexOffset = length - INDEX_HEADER_SIZE
  console.log('reading inderx', indexOffset, INDEX_HEADER_SIZE)
  self._readAndParseBlock(indexOffset, INDEX_HEADER_SIZE, function (err, indexHeader) {
    if (err) return cb(err)
    if (!constants.INDEX_BLOCK.equals(indexHeader.type)) {
      return cb(new Error('Unexpected block type ' + indexHeader.type))
    }
    return cb(null, indexHeader)
  })
}

MapTiles.prototype._read = function (quadkey, cb) {
  var self = this
  self._getTileOffset(quadkey, function (err, tileOffset) {
    if (err) return cb(err)
    debug('reading next', tileOffset)
    self._readAndParseBlock(tileOffset, TILE_HEADER_SIZE, function (err, block) {
      if (err) return cb(err)
      if (constants.TILE_BLOCK.equals(block.type)) {
        debug('got tile block', block, tileOffset)
        return self.storage.read(tileOffset + TILE_HEADER_SIZE, block.length, cb)
      }
      return cb(new Error('Could not find tile.'))
    })
  })
}

MapTiles.prototype._getTileOffset = function (quadkey, cb) {
  var self = this
  self._readIndex(function (err, index) {
    if (err) return cb(err)
    var indexPosition = utils.getIndexPosition(quadkey)
    var indexStart = self.length - (index.count * index.entryLength)
    debug('reading tile offset', indexPosition, indexStart)
    if (typeof indexPosition === 'undefined') {
      return cb(new Error('Could not find tile.'))
    }
    // This is the offset that contains the offset of the tile.
    // TODO: for sparse indexes, the index position should be managed
    // to not have a bunch of empty blocks
    var tileOffsetOffset = indexStart + (indexPosition * index.entryLength)
    debug('reading tile offsetoffset', tileOffsetOffset)
    self.storage.read(tileOffsetOffset, index.entryLength, function (err, buf) {
      if (err) return cb(err)
      var offset = (buf.length === 4) ? buf.readUInt32BE(0) : uint64be.decode(buf, 0)
      debug('got offset', offset)
      cb(null, offset)
    })
  })
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
  this._readAndParseBlock(0 + HEADER_SIZE, METADATA_SIZE, cb)
}

MapTiles.prototype._readAndParseBlock = function (offset, length, cb) {
  var self = this
  debug('reading', offset, length)
  self.storage.read(offset, length, function (err, buf) {
    if (err) return cb(err)
    try {
      var parsed = encoder.decodeBlock(buf)
    } catch (e) {
      return cb(e)
    }
    debug('read and parsed', parsed, buf.length)
    cb(null, parsed, offset)
  })
}

MapTiles.prototype.createWriteStream = function () {
  // TODO
}

MapTiles.prototype.createReadStream = function () {
  // TODO
}
