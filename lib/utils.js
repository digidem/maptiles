var assert = require('assert')

var structure = require('maptiles-spec').structure

var ZERO_CHAR = '\u0000'

// Trim zero padding from both ends of string
function zeroTrim (str) {
  str = String(str)
  var start = -1
  var len = str.length
  var end = len

  while (start < len && str[++start] === ZERO_CHAR) {}
  while (end > 0 && str[--end] === ZERO_CHAR) {}

  if (start >= len || end < 0) return ''
  return str.substring(start, end + 1)
}

// Zero pad buffer
function zeroPad (buf, len) {
  if (buf.length >= len) return buf
  var zeroBuf = Buffer.allocUnsafe(len - buf.length).fill(0)
  return Buffer.concat([zeroBuf, buf], len)
}

// Check val matches requirements for field
function assertMatch (val, match) {
  switch (typeof match) {
    case 'undefined':
      break
    case 'string':
    case 'number':
      assert(val === match, "Invalid value, expected '" + match + "', got '" + val + "'")
      break
    case 'function':
      assert(match(val), 'Invalid value: ' + val)
      break
    case 'object':
      // WARNING: assumes that array values are string or number
      // Currently the file structure does not have other options, so ok for now
      if (Array.isArray(match)) {
        assert(match.indexOf(val) > -1, "Invalid value, expected one of '" + match.join("', '") + "', got '" + val + "'")
        break
      } else if (Buffer.isBuffer(match)) {
        assert(val.equals(match), "Invalid buffer value, expected '" + match + "', got '" + val + "'")
        break
      }
    default: // eslint-disable-line no-fallthrough
      throw new Error("Invalid field match value '" + match + "'")
  }
}

/**
 * Get the quadkey for a tile [x, y, z]
 *
 * @name tileToQuadkey
 * @param {Array<number>} tile [x, y, z]
 * @returns {string} quadkey
 * @example
 * var quadkey = tileToQuadkey([1, 1, 5])
 * //=quadkey
 */
function tileToQuadkey (tile) {
  var index = ''
  for (var z = tile[2]; z > 0; z--) {
    var b = 0
    var mask = 1 << (z - 1)
    if ((tile[0] & mask) !== 0) b++
    if ((tile[1] & mask) !== 0) b += 2
    index += b.toString()
  }
  return index
}

/**
 * Get the tile [x, y, z] for a quadkey
 *
 * @name quadkeyToTile
 * @param {string} quadkey
 * @returns {Array<number>} tile [x, y, z]
 * @example
 * var tile = quadkeyToTile('00001033')
 * //=tile
 */
function quadkeyToTile (quadkey) {
  var x = 0
  var y = 0
  var z = quadkey.length

  for (var i = z; i > 0; i--) {
    var mask = 1 << (i - 1)
    var q = +quadkey[z - i]
    if (q === 1) x |= mask
    if (q === 2) y |= mask
    if (q === 3) {
      x |= mask
      y |= mask
    }
  }
  return [x, y, z]
}

/**
 * Given an index block for tile `firstTileQuadkey` and depth `indexDepth`
 * calculates the position within the index for the file offset for the tile
 * block for the tile `quadkey`
 *
 * @param {string} quadkey          Quadkey of the tile to lookup in the index
 * @param {string} firstTileQuadkey Quadkey of the first (parent) tile in the index
 * @param {number} indexDepth       Number of zoom levels in this index block
 * @return {Number} [description]
**/
function getIndexPosition (quadkey, firstTileQuadkey, indexDepth) {
  if (quadkey === firstTileQuadkey) return 0
  if (indexDepth === 1) {
    // Tile cannot be found, does not exist
    // TODO: throw error here?
    return
  }
  var prefixLen = firstTileQuadkey.length
  // Remove `firstTileQuadkey` from the start of the quadkey, and digits beyond
  // the index zoom level depth
  var subKey = quadkey.slice(prefixLen, prefixLen + indexDepth)
  var subKeyLen = subKey.length
  // Convert string as base 4 to an integer and calculate offset
  var position = (Number.parseInt(subKey, 4) + sumPowers(4, subKeyLen - 1))
  return position
}

// Sum of powers of a number
// https://math.stackexchange.com/questions/971761/calculating-sum-of-consecutive-powers-of-a-number
function sumPowers (base, expo) {
  return (Math.pow(base, expo + 1) - 1) / (base - 1)
}

var blockTypes = Object.keys(structure)
  .reduce(function (acc, block) {
    var typeDef = structure[block].type || structure[block].magicNumber
    acc[typeDef.match[0]] = block
    return acc
  }, {})

/**
 * Return the block type of a buffer
 *
 * @param {buffer} buf
 * @returns {string} name of block type
 */
function getBlockType (buf) {
  return blockTypes[buf[0]]
}

// Static export, sizes of each of the file blocks
// (only the headers of the index and tile blocks)
var blockSizes = Object.keys(structure)
  .reduce(function (sizes, block) {
    sizes[block] = Object.keys(structure[block])
      .reduce(function (acc, field) {
        return acc + (structure[block][field].size || 0)
      }, 0)
    return sizes
  }, {})

function getBlockSize (blockName) {
  return blockSizes[blockName]
}

module.exports = {
  zeroTrim: zeroTrim,
  zeroPad: zeroPad,
  assertMatch: assertMatch,
  tileToQuadkey: tileToQuadkey,
  quadkeyToTile: quadkeyToTile,
  getIndexPosition: getIndexPosition,
  getBlockType: getBlockType,
  getBlockSize: getBlockSize
}
