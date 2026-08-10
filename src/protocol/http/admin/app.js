(() => {
  "use strict";

  const logic = globalThis.BriskDbAdminLogic;
  const authEpoch = logic.createAuthEpoch();

  const state = {
    shard: 0,
    shardCount: 0,
    table: null,
    limit: 50,
    offset: 0,
    hasMore: false,
    overviewRequest: 0,
    rowsRequest: 0,
  };

  const elements = {
    loginView: document.querySelector("#login-view"),
    browserView: document.querySelector("#browser-view"),
    loginForm: document.querySelector("#login-form"),
    loginError: document.querySelector("#login-error"),
    username: document.querySelector("#username"),
    password: document.querySelector("#password"),
    logout: document.querySelector("#logout-button"),
    shard: document.querySelector("#shard-select"),
    shardKicker: document.querySelector("#shard-kicker"),
    tableList: document.querySelector("#table-list"),
    tableCount: document.querySelector("#table-count"),
    tableTitle: document.querySelector("#table-title"),
    tableSubtitle: document.querySelector("#table-subtitle"),
    pageSize: document.querySelector("#page-size"),
    status: document.querySelector("#status"),
    empty: document.querySelector("#empty-state"),
    tableWrap: document.querySelector("#table-wrap"),
    tableCaption: document.querySelector("#table-caption"),
    dataHead: document.querySelector("#data-head"),
    dataBody: document.querySelector("#data-body"),
    summary: document.querySelector("#page-summary"),
    previous: document.querySelector("#previous-page"),
    next: document.querySelector("#next-page"),
  };

  async function api(path, options = {}) {
    const requestAuthEpoch = authEpoch.current();
    const response = await fetch(path, {
      credentials: "same-origin",
      ...options,
      headers: {
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
    });
    const contentType = response.headers.get("content-type") || "";
    const body = contentType.includes("json") ? await response.json() : null;
    if (response.status === 401) {
      if (logic.acceptAuthenticationFailure(authEpoch, requestAuthEpoch)) {
        showLogin(body && body.message ? body.message : "Log in to continue.");
      }
      throw new Error("authentication_required");
    }
    if (!response.ok) {
      throw new Error(body && (body.detail || body.message) ? (body.detail || body.message) : `Request failed (${response.status})`);
    }
    return body;
  }

  function setStatus(message, isError = false) {
    elements.status.textContent = message;
    elements.status.classList.toggle("error", isError);
  }

  function showLogin(message = "") {
    state.overviewRequest += 1;
    state.rowsRequest += 1;
    elements.browserView.classList.add("hidden");
    elements.loginView.classList.remove("hidden");
    elements.loginError.textContent = message;
    elements.password.value = "";
    elements.username.focus();
  }

  function showBrowser() {
    elements.loginView.classList.add("hidden");
    elements.browserView.classList.remove("hidden");
    elements.loginError.textContent = "";
    elements.browserView.focus();
  }

  function clearPage(title, message, summary = "No rows loaded") {
    elements.empty.classList.remove("hidden");
    elements.empty.querySelector("h2").textContent = title;
    elements.empty.querySelector("p").textContent = message;
    elements.tableWrap.classList.add("hidden");
    elements.dataHead.replaceChildren();
    elements.dataBody.replaceChildren();
    elements.summary.textContent = summary;
    elements.previous.disabled = true;
    elements.next.disabled = true;
  }

  function resetTable(message = "Select an application table to inspect its rows.") {
    state.rowsRequest += 1;
    state.table = null;
    state.offset = 0;
    state.hasMore = false;
    elements.tableTitle.textContent = "Choose a table";
    elements.tableSubtitle.textContent = message;
    clearPage("No table selected", "Choose a table from the sidebar to browse a bounded, read-only page.");
  }

  function renderShardOptions(count, selected) {
    elements.shard.replaceChildren();
    for (let shard = 0; shard < count; shard += 1) {
      const option = document.createElement("option");
      option.value = String(shard);
      option.textContent = `Shard ${shard}`;
      option.selected = shard === selected;
      elements.shard.append(option);
    }
  }

  function displayTableName(table) {
    if (table.length === 0) return "(empty table name)";
    if (table.trim().length === 0) return "(whitespace-only table name)";
    return table;
  }

  function renderTables(tables) {
    elements.tableList.replaceChildren();
    elements.tableCount.textContent = String(tables.length);
    if (tables.length === 0) {
      const note = document.createElement("p");
      note.className = "no-tables";
      note.textContent = "No application tables on this shard.";
      elements.tableList.append(note);
      resetTable("This physical shard has no browseable application tables.");
      return;
    }
    for (const table of tables) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "table-button";
      button.textContent = displayTableName(table);
      button.title = displayTableName(table);
      button.setAttribute("aria-pressed", "false");
      button.addEventListener("click", () => selectTable(table, button));
      elements.tableList.append(button);
    }
    resetTable();
  }

  async function loadOverview(shard) {
    const requestId = ++state.overviewRequest;
    state.rowsRequest += 1;
    state.shard = shard;
    state.table = null;
    state.offset = 0;
    elements.tableList.replaceChildren();
    elements.tableCount.textContent = "…";
    elements.shardKicker.textContent = `Physical shard ${shard}`;
    resetTable(`Loading application tables from physical shard ${shard}.`);
    setStatus(`Loading tables from physical shard ${shard}…`);
    try {
      const overview = await api(`/admin/api/overview?shard=${encodeURIComponent(shard)}`);
      if (requestId !== state.overviewRequest) return;
      state.shardCount = overview.shard_count;
      state.shard = overview.selected_shard;
      renderShardOptions(overview.shard_count, overview.selected_shard);
      renderTables(overview.tables);
      setStatus(`${overview.tables.length} application table${overview.tables.length === 1 ? "" : "s"} on physical shard ${overview.selected_shard}.`);
    } catch (error) {
      if (requestId !== state.overviewRequest) return;
      if (error.message !== "authentication_required") {
        resetTable("The table list could not be loaded.");
        setStatus(error.message, true);
      }
    }
  }

  async function selectTable(table, button) {
    state.table = table;
    state.offset = 0;
    for (const item of elements.tableList.querySelectorAll(".table-button")) {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    }
    elements.tableTitle.textContent = displayTableName(table);
    elements.tableSubtitle.textContent = `Read-only rows from physical shard ${state.shard}.`;
    await loadRows();
  }

  function renderCell(value) {
    const cell = document.createElement("td");
    const content = document.createElement("span");
    const presentation = logic.cellPresentation(value);
    content.className = presentation.className;
    content.textContent = presentation.text;
    content.title = presentation.title;
    cell.append(content);
    return cell;
  }

  function renderPage(page) {
    elements.dataHead.replaceChildren();
    elements.dataBody.replaceChildren();
    const headingRow = document.createElement("tr");
    for (const column of page.columns) {
      const heading = document.createElement("th");
      heading.scope = "col";
      const name = document.createElement("span");
      name.textContent = column.name || "(unnamed)";
      const type = document.createElement("span");
      type.className = "column-type";
      type.textContent = column.data_type;
      heading.append(name, type);
      headingRow.append(heading);
    }
    elements.dataHead.append(headingRow);
    for (const row of page.rows) {
      const tableRow = document.createElement("tr");
      for (const value of row) {
        tableRow.append(renderCell(value));
      }
      elements.dataBody.append(tableRow);
    }

    state.hasMore = page.has_more;
    elements.tableCaption.textContent = `${displayTableName(page.table)} rows from physical shard ${page.shard}`;
    elements.empty.classList.toggle("hidden", page.rows.length !== 0);
    elements.tableWrap.classList.toggle("hidden", page.rows.length === 0);
    if (page.rows.length === 0) {
      elements.empty.querySelector("h2").textContent = page.offset === 0 ? "This table is empty" : "No rows on this page";
      elements.empty.querySelector("p").textContent = "Try the previous page or select another table.";
    }
    const first = page.rows.length === 0 ? 0 : page.offset + 1;
    const last = page.offset + page.rows.length;
    elements.summary.textContent = page.rows.length === 0 ? "No rows" : `Showing rows ${first}–${last}`;
    elements.previous.disabled = page.offset === 0;
    elements.next.disabled = !page.has_more;
  }

  async function loadRows() {
    if (state.table === null) return;
    const requestId = ++state.rowsRequest;
    const displayTable = displayTableName(state.table);
    clearPage("Loading rows…", `Reading ${displayTable} from physical shard ${state.shard}.`, "Loading…");
    setStatus(`Loading ${displayTable} from physical shard ${state.shard}…`);
    elements.previous.disabled = true;
    elements.next.disabled = true;
    const query = new URLSearchParams({
      shard: String(state.shard),
      table: state.table,
      limit: String(state.limit),
      offset: String(state.offset),
    });
    try {
      const page = await api(`/admin/api/rows?${query.toString()}`);
      if (requestId !== state.rowsRequest) return;
      renderPage(page);
      setStatus(`${page.rows.length} row${page.rows.length === 1 ? "" : "s"} loaded from physical shard ${page.shard}.`);
    } catch (error) {
      if (requestId !== state.rowsRequest) return;
      if (error.message !== "authentication_required") {
        clearPage("Rows unavailable", error.message, "No rows loaded");
        setStatus(error.message, true);
      }
    }
  }

  elements.loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    authEpoch.advance();
    const loginAuthEpoch = authEpoch.current();
    elements.loginError.textContent = "";
    const submit = elements.loginForm.querySelector("button[type='submit']");
    submit.disabled = true;
    try {
      await api("/admin/api/login", {
        method: "POST",
        body: JSON.stringify({ username: elements.username.value, password: elements.password.value }),
      });
      if (!authEpoch.isCurrent(loginAuthEpoch)) return;
      showBrowser();
      await loadOverview(0);
    } catch (error) {
      if (authEpoch.isCurrent(loginAuthEpoch) && error.message !== "authentication_required") {
        elements.loginError.textContent = error.message;
      }
    } finally {
      submit.disabled = false;
    }
  });

  elements.logout.addEventListener("click", async () => {
    authEpoch.advance();
    const logoutAuthEpoch = authEpoch.current();
    try {
      await api("/admin/api/logout", { method: "POST" });
    } catch (_) {
      // The local page still returns to login when the server already forgot the session.
    } finally {
      if (authEpoch.isCurrent(logoutAuthEpoch)) {
        authEpoch.advance();
        showLogin();
      }
    }
  });

  elements.shard.addEventListener("change", () => loadOverview(Number(elements.shard.value)));
  elements.pageSize.addEventListener("change", () => {
    state.limit = Number(elements.pageSize.value);
    state.offset = 0;
    loadRows();
  });
  elements.previous.addEventListener("click", () => {
    state.offset = Math.max(0, state.offset - state.limit);
    loadRows();
  });
  elements.next.addEventListener("click", () => {
    if (state.hasMore) {
      state.offset += state.limit;
      loadRows();
    }
  });

  const sessionAuthEpoch = authEpoch.current();
  api("/admin/api/session")
    .then(() => {
      if (!authEpoch.isCurrent(sessionAuthEpoch)) return undefined;
      showBrowser();
      return loadOverview(0);
    })
    .catch((error) => {
      if (!authEpoch.isCurrent(sessionAuthEpoch)) return;
      if (error.message !== "authentication_required") {
        authEpoch.advance();
        showLogin(error.message);
      }
    });
})();
