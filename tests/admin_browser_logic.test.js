"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(
  path.join(__dirname, "..", "src", "protocol", "http", "admin", "logic.js"),
  "utf8",
);
const context = {};
vm.runInNewContext(source, context, { filename: "logic.js" });
const logic = context.BriskDbAdminLogic;

test("tagged signed and unsigned integers retain their exact text", () => {
  for (const [kind, value] of [
    ["int64", "-9223372036854775808"],
    ["int64", "9007199254740992"],
    ["uint64", "18446744073709551615"],
  ]) {
    const cell = { $briskdb_type: kind, value };
    assert.equal(logic.taggedInteger(cell), true);
    const presentation = logic.cellPresentation(cell);
    assert.equal(presentation.text, value);
    assert.equal(presentation.className, "integer-value");
    assert.equal(presentation.title, `${kind}: ${value}`);
  }
});

test("malformed integer tags are never interpreted as BriskDB integers", () => {
  for (const value of [
    { $briskdb_type: "decimal", value: "1" },
    { $briskdb_type: "int64", value: "1.5" },
    { $briskdb_type: "int64", value: 1 },
    { value: "1" },
    null,
    [],
  ]) {
    assert.equal(logic.taggedInteger(value), false);
  }
});

test("ordinary cells keep their existing browser presentation", () => {
  assert.equal(logic.cellPresentation(null).text, "NULL");
  assert.equal(logic.cellPresentation("hello").text, "hello");
  assert.equal(logic.cellPresentation(9007199254740991).text, "9007199254740991");
  assert.equal(logic.cellPresentation([0, 15, 255]).text, "0x000fff");
});

test("logical row counts are formatted without losing integer precision", () => {
  assert.equal(logic.exactRowCount(1536282), "1536282");
  let presentation = logic.rowCountPresentation(1536282, [0, 1]);
  assert.equal(presentation.text, "1,536,282 logical records across 2 storage files");
  assert.match(presentation.title, /visited shards: 0, 1/i);
  assert.match(presentation.title, /Global tables are read once/);

  const maximum = { $briskdb_type: "uint64", value: "18446744073709551615" };
  assert.equal(logic.exactRowCount(maximum), "18446744073709551615");
  presentation = logic.rowCountPresentation(maximum, Array.from({ length: 64 }, (_, shard) => shard));
  assert.equal(
    presentation.text,
    "18,446,744,073,709,551,615 logical records across 64 storage files",
  );

  assert.equal(logic.rowCountPresentation(1, [0]).text, "1 logical record across 1 storage file");
  for (const invalid of [-1, Number.MAX_SAFE_INTEGER + 1, { $briskdb_type: "int64", value: "-1" }]) {
    assert.equal(logic.exactRowCount(invalid), null);
  }
  assert.throws(() => logic.rowCountPresentation(-1, [0, 1]), /Invalid logical/);
  for (const invalidShards of [[], [1, 0], [0, 0], [0, 1.5], "0,1"]) {
    assert.equal(logic.validVisitedShards(invalidShards), false);
    assert.throws(() => logic.rowCountPresentation(0, invalidShards), /Invalid logical/);
  }
});

test("page summaries describe one logical row stream", () => {
  assert.equal(
    logic.pageSummary({ visited_shards: [0, 1], offset: 50, rows: [[1], [2]] }),
    "Showing logical rows 51–52",
  );
  assert.equal(
    logic.pageSummary({ visited_shards: [0], offset: 0, rows: [] }),
    "No logical rows",
  );
});

test("table response guards reject stale counts after table changes", () => {
  assert.equal(logic.acceptsTableResponse(4, 4, "orders", "orders"), true);
  assert.equal(logic.acceptsTableResponse(3, 4, "orders", "orders"), false);
  assert.equal(logic.acceptsTableResponse(4, 4, "orders", "payments"), false);
});

test("authentication epochs reject stale responses after login or logout starts", () => {
  const epochs = logic.createAuthEpoch();
  const initialSession = epochs.current();
  const login = epochs.advance();

  assert.equal(epochs.isCurrent(initialSession), false);
  assert.equal(epochs.isCurrent(login), true);
  assert.equal(logic.acceptAuthenticationFailure(epochs, initialSession), false);
  assert.equal(epochs.current(), login);
  assert.equal(logic.acceptAuthenticationFailure(epochs, login), true);
  assert.equal(epochs.isCurrent(login), false);

  const oldDataRequest = epochs.current();
  const logout = epochs.advance();
  assert.equal(epochs.isCurrent(oldDataRequest), false);
  assert.equal(epochs.isCurrent(logout), true);
});
