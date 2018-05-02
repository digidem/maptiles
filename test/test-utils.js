var utils = require('../lib/utils')
var test = require('tape')

test('getIndexPosition', function (t) {
  var pos = utils.getIndexPosition('0', '0', 1)
  t.same(pos, 0)

  pos = utils.getIndexPosition('00', '0', 2)
  t.same(pos, 1)

  pos = utils.getIndexPosition('01', '0', 2)
  t.same(pos, 2)

  pos = utils.getIndexPosition('02', '0', 3)
  t.same(pos, 3)

  t.end()
})
