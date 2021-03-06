(function() {
  if (typeof getToolTest === 'undefined') {
    load('jstests/configs/plain_28.config.js');
  }

  resetDbpath('dump');
  var toolTest = getToolTest('outFlagTest');
  var commonToolArgs = getCommonToolArguments();
  var db = toolTest.db.getSiblingDB('foo');

  db.dropDatabase();
  assert.eq(0, db.bar.count());
  db.getSiblingDB('baz').dropDatabase();
  assert.eq(0, db.getSiblingDB('baz').bar.count());

  // Insert into the 'foo' database
  db.bar.insert({x: 1});
  // and into the 'baz' database
  db.getSiblingDB('baz').bar.insert({x: 2});

  // Running mongodump with `--out -` specified but no '--db' should fail
  var dumpArgs = ['dump',
    '--collection', 'bar',
    '--out', '-']
    .concat(commonToolArgs);
  assert(toolTest.runTool.apply(toolTest, dumpArgs) !== 0,
    'mongodump should exit with a non-zero status when --out is ' +
    'specified but --db isn\'t');

  // Running mongodump with `--out -` specified but no '--collection' should
  // fail
  dumpArgs = ['dump',
    '--db', 'foo',
    '--out', '-']
    .concat(commonToolArgs);
  assert(toolTest.runTool.apply(toolTest, dumpArgs) !== 0,
    'mongodump should exit with a non-zero status when --out is ' +
    'specified but --collection isn\'t');

  // Running mongodump with '--out dump2' should dump data to a different dir
  resetDbpath('dump2');
  dumpArgs = ['dump',
    '--collection', 'bar',
    '--db', 'foo',
    '--out', 'dump2']
    .concat(commonToolArgs);
  assert.eq(toolTest.runTool.apply(toolTest, dumpArgs), 0,
    'mongodump with --collection, --db, --out should succeed');
  db.dropDatabase();
  db.getSiblingDB('baz').dropDatabase();
  assert.eq(0, db.bar.count());
  assert.eq(0, db.getSiblingDB('baz').bar.count());

  var restoreArgs = ['restore',
    '--dir', 'dump2']
    .concat(commonToolArgs);
  assert.eq(toolTest.runTool.apply(toolTest, restoreArgs), 0,
    'mongorestore should succeed');
  assert.eq(1, db.bar.count());
  assert.eq(0, db.getSiblingDB('baz').bar.count());

  toolTest.stop();
}());
