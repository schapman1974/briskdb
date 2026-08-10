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
    cellPresentation,
    createAuthEpoch,
    taggedInteger,
  });
})();
