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

var defaultHeader = {
  magicNumber: constants.MAGIC_NUMBER,
  version: 1
}

var defaultMetadata = {
  type: constants.METADATA_BLOCK
}

module.exports = MapTiles

function MapTiles (storage, metadata) {
  if (!(this instanceof MapTiles)) return new MapTiles(storage, metadata)
  this.storage = raf(storage)
  this.index = [] // lol memory on writes
  this.lock = mutexify()
  this.openSpaceOffset = 0
  this.writable = true
  this.maxDepth = 1
  this._onopen(metadata || {})
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

  self.lock(function (release) {
    self._write(quadkey, tile, release.bind(null, cb))
  })
}

MapTiles.prototype.end = function (cb) {
  var self = this
  self.writable = false
  // write the index header
  var indexHeader = {
    type: constants.INDEX_BLOCK,
    entryLength: constants.ENTRY_LENGTH,
    depth: self.maxDepth,
    firstQuadkey: self.firstQuadkey
  }
  this.storage.write(self.openSpaceOffset, encoder.encodeBlock(indexHeader), done)

  function done (err) {
    if (err) return cb(err)
    ;(function next (i) {
      if (i >= self.index.length) return cb()
      var item = self.index[i]
      var offset = self.openSpaceOffset + INDEX_HEADER_SIZE + (item.indexPosition * constants.ENTRY_LENGTH)
      var buf = Buffer.allocUnsafe(4).fill(0)
      buf.writeUInt32BE(item.offset, 0)
      self.storage.write(offset, buf, function (err) {
        if (err) return cb(err)
        next(i + 1)
      })
    })(0)
  }
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
  var indexPosition = utils.getIndexPosition(quadkey)
  var offset = HEADER_SIZE + METADATA_SIZE + (indexPosition * constants.ENTRY_LENGTH)
  self.index.push({indexPosition, offset})
  var buf = encoder.encodeBlock(tileHeader)
  self.storage.write(offset, buf, function (err) {
    if (err) return cb(err)
    var tileOffset = offset + TILE_HEADER_SIZE
    self.openSpaceOffset = tileOffset + tile.length
    self.storage.write(tileOffset, tile, cb)
  })
}

MapTiles.prototype._onopen = function (metadata) {
  var self = this
  var buf = Buffer.allocUnsafe(HEADER_SIZE + METADATA_SIZE).fill(0)
  var headerBuf = encoder.encodeBlock(Object.assign({}, defaultHeader, {
    metadataOffset: HEADER_SIZE
  }))
  headerBuf.copy(buf, 0, HEADER_SIZE)
  var metadataBuf = encoder.encodeBlock(Object.assign({}, metadata, {
    type: constants.METADATA_BLOCK,
    length: METADATA_SIZE
  }))
  metadataBuf.copy(buf, HEADER_SIZE, buf.length)
  self.lock(function (release) {
    self.storage.write(0, buf, release)
  })
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
      : uint64be.decode(buf, 32)
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
      var parsed = encoder.decodeBlock(buf)
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
