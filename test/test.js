var utils = require('../lib/utils')
var rimraf = require('rimraf')
var fs = require('fs')
var cousteau = require('cousteau')
var maptiles = require('..')
var test = require('tape')
var path = require('path')

var pathname = path.join(__dirname, 'testdb.maptiles')
var mt = maptiles(pathname)

test('write and read a simple maptiles format', function (t) {
  var sourcepath = path.join(__dirname, 'data', 'mini')
  cousteau(sourcepath, function (err, result) {
    t.notOk(err.length)
    putTiles(mt, result.files, function (err) {
      t.error(err)
      mt.end(function (err) {
        t.error(err)
        check(t, ['0', '1', '2', '3'], function () {
          t.end()
        })
      })
    })
  })
})

test('cleanup', function (t) {
  rimraf(pathname, t.end)
})

function putTiles (mt, tilepaths, done) {
  ;(function next (i) {
    if (i >= tilepaths.length) return done()
    var stat = tilepaths[i]
    if (stat.path.endsWith('metadata.json')) return next(i + 1)
    var parts = stat.path.split(path.sep)
    var q = {
      z: parts[parts.length - 3],
      x: parts[parts.length - 2],
      y: parts[parts.length - 1].split('.')[0]
    }
    console.log('put', stat.path, q)
    fs.readFile(stat.path, function (err, buf) {
      if (err) return done(err)
      mt.put(q, buf, function (err) {
        if (err) return done(err)
        next(i + 1)
      })
    })
  })(0)
}

function check (t, quadkeys, cb) {
  ;(function next (i) {
    if (i >= quadkeys.length) return cb()
    var q = quadkeys[i]
    mt.get({q}, function (err, tile) {
      t.error(err)
      var xyz = utils.quadkeyToTile(q).map(function (n) { return n.toString() })
      console.log('checking', 'q=', q, 'xyz=', xyz)
      var buf = fs.readFileSync(path.join(__dirname, 'data', 'mini', xyz[2], xyz[0], xyz[1] + '.jpeg'))
      t.ok(buf.equals(tile))
      next(i + 1)
    })
  })(0)
}
