var assert = require('assert')
var uint64be = require('uint64be')

var structure = require('maptiles-spec').structure
var zeroPad = require('./utils').zeroPad
var trim = require('./utils').zeroTrim
var assertMatch = require('./utils').assertMatch
var getBlockSize = require('./utils').getBlockSize
var getBlockType = require('./utils').getBlockType

var ASCII_REGEX = /^[\x00-\x7f]*$/ // eslint-disable-line no-control-regex

/**
 * Encode a field to a buffer, based on a field definition
 * @param {any} val Value to encode to the buffer
 * @param {buffer} buf  Buffer to encode the field into
 * @param {object} def  A field definition
 * @param {number} def.offset The offset of the field from the start of the buffer
 * @param {number} def.size   The size of the field in bytes
 * @param {string} def.type   The type of the field
 * @param {string|number|regexp|array|buffer} [def.match] Optional match values for the field value
 * @param {object} [opts] Object of the same structure as def, to optionally override properties
 * @return {any} Returns decoded field value
 */
function encodeField (val, buf, def, opts) {
  def = Object.assign({}, def, opts)
  assert(Buffer.isBuffer(buf), 'Expected a buffer')
  assert(typeof def.offset === 'number', 'Field definition is missing offset')
  assert(typeof def.size === 'number', 'Field definition is missing size')
  assert(typeof def.type === 'string', 'Field definition is missing type')
  assert(def.offset === 0 || getBlockType(buf) === def.blockName,
    "Unexpected buffer type, was expecting type '" + def.blockName + "' but got '" + getBlockType(buf) + "'")
  assert(buf.length >= def.offset + def.size, 'Field definition is beyond end of buffer')

  // Check val is the expected type
  switch (def.type) {
    case 'buffer':
      assert(Buffer.isBuffer(val), 'Expected value to be a buffer')
      assert(val.length === def.size, 'Incorrect buffer length, expected ' + def.size + ' bytes but got ' + val.length + ' bytes')
      break
    case 'ascii':
      assert(ASCII_REGEX.test(val), 'Value is invalid ascii')
    case 'utf-8': // eslint-disable-line no-fallthrough
      assert(typeof val === 'string', 'Expected value to be string')
      break
    case 'UInt8': // eslint-disable-line no-fallthrough
      assert((val <= 2) ^ 8 && val >= 0, 'val must be non-negative and < 2^8')
    case 'UInt32BE': // eslint-disable-line no-fallthrough
      assert((val <= 2) ^ 32 && val >= 0, 'val must be non-negative and < 2^32')
    case 'UInt64BE': // eslint-disable-line no-fallthrough
      assert(Number.isSafeInteger(val), 'val is not not a safe integer')
      break
    case 'DoubleBE':
      assert(typeof val === 'number', 'Expected value to be a number')
      break
    default:
      throw new Error("Invalid field type '" + def.type + "'")
  }

  assertMatch(val, def.match)

  // write val to buf
  switch (def.type) {
    case 'buffer':
      val.copy(buf, def.offset)
      break
    case 'ascii':
    case 'utf-8':
      val = zeroPad(Buffer.from(val, def.type), def.size)
      assert(val.length === def.size, 'String is too long, field length is ' + def.size + ' bytes but encoded string is ' + val.length + 'bytes')
      val.copy(buf, def.offset)
      break
    case 'UInt8':
      buf.writeUInt8(val, def.offset)
      break
    case 'UInt32BE':
      buf.writeUInt32BE(val, def.offset)
      break
    case 'UInt64BE':
      uint64be.encode(val, buf, def.offset)
      break
    case 'DoubleBE':
      buf.writeDoubleBE(val, def.offset)
      break
  }

  return buf
}

/**
 * Decodes a field from a buffer, based on a field definition
 * @param {buffer} buf  Buffer containing the encoded field
 * @param {object} def  A field definition
 * @param {number} def.offset The offset of the field from the start of the buffer
 * @param {number} def.size   The size of the field in bytes
 * @param {string} def.type   The type of the field
 * @param {string|number|regexp|array|buffer} [def.match] Optional match values for the field value
 * @param {object} opts Object of the same structure as def, to optionally override properties
 * @return {any} Returns decoded field value
 */
function decodeField (buf, def, opts) {
  var val
  def = Object.assign({}, def, opts)
  assert(Buffer.isBuffer(buf), 'Expected a buffer')
  assert(typeof def.offset === 'number', 'Field definition is missing offset')
  assert(typeof def.size === 'number', 'Field definition is missing size')
  assert(typeof def.type === 'string', 'Field definition is missing type')
  assert(getBlockType(buf) === def.blockName, "Unexpected buffer type, was expecting type '" + def.blockName + "' but got '" + getBlockType(buf) + "'")
  assert(buf.length >= def.offset + def.size, 'Field definition is beyond end of buffer')
  switch (def.type) {
    case 'buffer':
      val = buf.slice(def.offset, def.offset + def.size)
      break
    case 'ascii':
    case 'utf-8':
      val = trim(buf.toString(def.type, def.offset, def.offset + def.size))
      break
    case 'UInt8':
      val = buf.readUInt8(def.offset)
      break
    case 'UInt32BE':
      val = buf.readUInt32BE(def.offset)
      break
    case 'UInt64BE':
      val = uint64be.decode(buf, def.offset)
      break
    case 'DoubleBE':
      val = buf.readDoubleBE(def.offset)
      break
    default:
      throw new Error("Invalid field type '" + def.type + "'")
  }

  assertMatch(val, def.match)

  return val
}

// Decode all fields in a block
function parseBlock (buf) {
  var type = getBlockType(buf)
  assert(typeof type !== 'undefined', 'Unrecognized block type')
  assert(buf.length >= getBlockSize(type), 'Block buffer unexpected size')
  return Object.keys(structure[type])
    .reduce(function (acc, field) {
      if (!structure[type][field].size) return acc
      acc[field] = decodeField(buf, structure[type][field])
      return acc
    }, {})
}

function encodeBlock (block) {
  var type
  if (block.magicNumber) type = 'header'
  else type = getBlockType(block.type)
  assert(typeof type !== 'undefined', 'Unrecognized block type')
  var buf = Buffer.allocUnsafe(getBlockSize(type)).fill(0)
  Object.keys(structure[type]).forEach(function (field) {
    encodeField(block[field], buf, structure[type][field])
  })
  return buf
}

module.exports = {
  encodeField: encodeField,
  decodeField: decodeField,
  parseBlock: parseBlock,
  encodeBlock: encodeBlock
}
