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
