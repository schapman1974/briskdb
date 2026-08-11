(() => {
  "use strict";

  const INTEGER_KINDS = new Set(["int64", "uint64"]);

  function taggedInteger(value) {
    return value !== null
      && typeof value === "object"
      && !Array.isArray(value)
      && INTEGER_KINDS.has(value.$briskdb_type)
      && typeof value.value === "string"
      && /^-?[0-9]+$/.test(value.value);
  }

  function cellPresentation(value) {
    if (value === null) {
      return { text: "NULL", className: "null-value", title: "NULL" };
    }
    if (Array.isArray(value)) {
      const text = value.length === 0
        ? "0x"
        : `0x${value.map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
      return { text, className: "binary-value", title: text };
    }
    if (taggedInteger(value)) {
      return {
        text: value.value,
        className: "integer-value",
        title: `${value.$briskdb_type}: ${value.value}`,
      };
    }
    if (typeof value === "string") {
      return { text: value, className: "", title: value };
    }
    const text = JSON.stringify(value);
    return { text, className: "", title: text };
  }

  function exactRowCount(value) {
    if (Number.isSafeInteger(value) && value >= 0) return String(value);
    if (taggedInteger(value) && !value.value.startsWith("-")) return value.value;
    return null;
  }

  function validVisitedShards(shards) {
    return Array.isArray(shards)
      && shards.length > 0
      && shards.every((shard, index) => Number.isInteger(shard)
        && shard >= 0
        && (index === 0 || shard > shards[index - 1]));
  }

  function rowCountPresentation(value, visitedShards) {
    const exact = exactRowCount(value);
    if (exact === null || !validVisitedShards(visitedShards)) {
      throw new Error("Invalid logical row count response.");
    }
    const formatted = exact.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    const files = visitedShards.length;
    return {
      text: `${formatted} logical record${exact === "1" ? "" : "s"} across ${files} storage file${files === 1 ? "" : "s"}`,
      title: `Exact total from visited shard${files === 1 ? "" : "s"}: ${visitedShards.join(", ")}. Global tables are read once.`,
    };
  }

  function acceptsTableResponse(requestId, currentRequestId, requestedTable, currentTable) {
    return requestId === currentRequestId && requestedTable === currentTable;
  }

  function pageSummary(page) {
    if (page.rows.length === 0) return "No logical rows";
    const first = page.offset + 1;
    const last = page.offset + page.rows.length;
    return `Showing logical rows ${first}–${last}`;
  }

  function createAuthEpoch() {
    let current = 0;
    return Object.freeze({
      current: () => current,
      advance: () => {
        current += 1;
        return current;
      },
      isCurrent: (candidate) => candidate === current,
    });
  }

  function acceptAuthenticationFailure(epochs, requestEpoch) {
    if (!epochs.isCurrent(requestEpoch)) return false;
    epochs.advance();
    return true;
  }

  globalThis.BriskDbAdminLogic = Object.freeze({
    acceptAuthenticationFailure,
    acceptsTableResponse,
    cellPresentation,
    createAuthEpoch,
    exactRowCount,
    pageSummary,
    rowCountPresentation,
    taggedInteger,
    validVisitedShards,
  });
})();
