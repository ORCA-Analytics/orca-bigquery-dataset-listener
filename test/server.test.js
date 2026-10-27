const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const util = require("node:util");
const {
  CATALOG,
  buildPlan,
  createApp,
  createCompanyFlagSetter,
  createCompanySettingsReader,
  createSourceDatasetNotifier,
  scaffoldSettings,
  timezoneTag,
} = require("../server.js");

const HOUR = 3600000;
const NOW = Date.parse("2026-10-07T12:00:00Z");

// Fake BigQuery client: `datasets` maps dataset id -> table ids; a dataset not
// listed raises 404 like the real client. `error` makes every call fail.
function fakeBigQuery({ datasets = {}, error } = {}) {
  return {
    dataset: (id) => ({
      getTables: async () => {
        if (error) throw error;
        if (!(id in datasets)) throw Object.assign(new Error(`Not found: ${id}`), { code: 404 });
        return [datasets[id].map((t) => ({ id: t }))];
      },
    }),
  };
}

function pubsubBody(datasetId, createdAt) {
  const logEntry = {
    timestamp: new Date(createdAt).toISOString(),
    protoPayload: { resourceName: `projects/orcaanalytics/datasets/${datasetId}` },
  };
  return { message: { data: Buffer.from(JSON.stringify(logEntry)).toString("base64") } };
}

async function deliver(app, body) {
  const server = app.listen(0);
  try {
    const { port } = server.address();
    const res = await fetch(`http://127.0.0.1:${port}/`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    return res.status;
  } finally {
    server.close();
  }
}

function setup(bigqueryOptions, { dispatchError, setCompanyFlag, notifySourceDataset, readCompanySettings } = {}) {
  const dispatched = [];
  const app = createApp({
    setCompanyFlag,
    notifySourceDataset,
    readCompanySettings,
    bigquery: fakeBigQuery(bigqueryOptions),
    dispatch: async (payload) => {
      if (dispatchError) throw dispatchError;
      dispatched.push(payload);
    },
    now: () => NOW,
    maxWaitHours: 72,
  });
  return { app, dispatched };
}

const SHOPIFY_TABLES = ["orders", "order_refunds", "products", "product_variants", "customer_journey_summary"];

test("ignores datasets that aren't in the catalog", async () => {
  const { app, dispatched } = setup();
  assert.equal(await deliver(app, pubsubBody("unknown__acme", NOW)), 204);
  assert.equal(dispatched.length, 0);
});

test("waits (non-2xx so Pub/Sub redelivers) while tables are missing", async () => {
  const { app, dispatched } = setup({ datasets: { shopify__acme: ["orders"] } });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - 2 * HOUR)), 429);
  assert.equal(dispatched.length, 0);
});

test("waits when the dataset itself isn't visible yet", async () => {
  const { app, dispatched } = setup({ datasets: {} });
  assert.equal(await deliver(app, pubsubBody("facebook_ads__acme", NOW)), 429);
  assert.equal(dispatched.length, 0);
});

test("dispatches once every table exists", async () => {
  const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - HOUR)), 204);
  assert.equal(dispatched.length, 1);
  const payload = dispatched[0];
  assert.equal(payload.datasetId, "shopify__acme");
  assert.deepEqual(payload.missingTables, []);
  assert.deepEqual(payload.files, buildPlan("shopify__acme", "orcaanalytics").files);
  assert.equal(payload.vars.client, "acme");
});

test("dispatches anyway after the max wait, listing what's missing", async () => {
  const { app, dispatched } = setup({ datasets: { shopify__acme: ["orders", "products"] } });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - 73 * HOUR)), 204);
  assert.equal(dispatched.length, 1);
  assert.deepEqual(dispatched[0].missingTables.sort(), [
    "shopify__acme.customer_journey_summary",
    "shopify__acme.order_refunds",
    "shopify__acme.product_variants",
  ]);
});

test("dispatches immediately if the table check itself fails", async () => {
  const denied = Object.assign(new Error("Access Denied"), { code: 403 });
  const { app, dispatched } = setup({ error: denied });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.deepEqual(dispatched[0].missingTables, []);
});

test("checks the other datasets a platform reads", async () => {
  const { app, dispatched } = setup({
    datasets: { klaviyo__acme: ["profiles"], shopify__acme: [] },
  });
  assert.equal(await deliver(app, pubsubBody("klaviyo__acme", NOW)), 429);
  assert.equal(dispatched.length, 0);
});

test("ignores pacing__ datasets: no connector makes one, and pacing rides on source scaffolds", async () => {
  const { app, dispatched } = setup({ datasets: {} });
  assert.equal(await deliver(app, pubsubBody("pacing__acme", NOW)), 204);
  assert.equal(dispatched.length, 0);
});

test("returns 500 when the GitHub dispatch fails, so Pub/Sub retries", async () => {
  const { app } = setup({ datasets: { rakuten__acme: ["report"] } }, { dispatchError: new Error("GitHub down") });
  assert.equal(await deliver(app, pubsubBody("rakuten__acme", NOW)), 500);
});

// --- Amazon ---

function recordFlags() {
  const calls = [];
  return { calls, setCompanyFlag: async (args) => (calls.push(args), { status: "set" }) };
}

test("amazon_sellercentral scaffolds the Seller Central models, suffixes included", () => {
  const plan = buildPlan("amazon_sellercentral__acme_airbyte", "orcaanalytics");
  assert.equal(plan.vars.parent, "amazon_sellercentral");
  assert.equal(plan.vars.client, "acme_airbyte");
  assert.deepEqual(plan.files, [
    {
      template: "templates/amazon/amazon_sellercentral/amazon_sellercentral.sql",
      path: "models/amazon/amazon_sellercentral/amazon_sellercentral__acme_airbyte.sql",
    },
    {
      template: "templates/amazon/amazon_sellercentral_cohorts/amazon_sellercentral_cohorts.sql",
      path: "models/amazon/amazon_sellercentral_cohorts/amazon_sellercentral_cohorts__acme_airbyte.sql",
    },
    {
      template: "templates/amazon/amazon_sellercentral_newreturn/amazon_sellercentral_newreturn.sql",
      path: "models/amazon/amazon_sellercentral_newreturn/amazon_sellercentral_newreturn__acme_airbyte.sql",
    },
  ]);
});

test("amazon_ads scaffolds only the Amazon Ads model", () => {
  const plan = buildPlan("amazon_ads__acme", "orcaanalytics");
  assert.equal(plan.vars.parent, "amazon_ads");
  assert.deepEqual(plan.files, [
    { template: "templates/amazon/amazon_ads/amazon_ads.sql", path: "models/amazon/amazon_ads/amazon_ads__acme.sql" },
  ]);
  assert.ok(plan.tables.every((t) => t.startsWith("amazon_ads__acme.")));
});

test("Seller Central is ready with either loader's table names", async () => {
  for (const tables of [
    ["GET_SALES_AND_TRAFFIC_REPORT", "Orders"], // Airbyte
    ["sales_and_traffic_business_report_daily", "orders"], // Fivetran
  ]) {
    const flags = recordFlags();
    const { app, dispatched } = setup({ datasets: { amazon_sellercentral__acme: tables } }, flags);
    assert.equal(await deliver(app, pubsubBody("amazon_sellercentral__acme", NOW)), 204);
    assert.equal(dispatched.length, 1);
    assert.deepEqual(dispatched[0].missingTables, []);
    assert.deepEqual(flags.calls, [{ parent: "amazon_sellercentral", client: "acme" }]);
  }
});

test("doesn't scaffold an Amazon dataset that never gets a template's tables, but still turns Amazon on", async () => {
  const flags = recordFlags();
  const id = "amazon_sellercentral__acme_airbyte_fulfillments";
  const { app, dispatched } = setup({ datasets: { [id]: ["GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL"] } }, flags);
  assert.equal(await deliver(app, pubsubBody(id, NOW)), 429);
  assert.equal(await deliver(app, pubsubBody(id, NOW - 73 * HOUR)), 204);
  assert.equal(dispatched.length, 0);
  assert.deepEqual(flags.calls, [{ parent: "amazon_sellercentral", client: "acme_airbyte_fulfillments" }]);
});

test("turns Amazon on even when a template's tables are still partly missing after the max wait", async () => {
  const flags = recordFlags();
  const { app, dispatched } = setup(
    { datasets: { amazon_sellercentral__acme: ["GET_SALES_AND_TRAFFIC_REPORT"] } },
    flags
  );
  assert.equal(await deliver(app, pubsubBody("amazon_sellercentral__acme", NOW - 73 * HOUR)), 204);
  assert.deepEqual(dispatched[0].missingTables, ["amazon_sellercentral__acme.Orders|orders"]);
  assert.equal(flags.calls.length, 1);
});

test("any other amazon* dataset only turns Amazon on, once it holds a table", async () => {
  const flags = recordFlags();
  const { app, dispatched } = setup({ datasets: { amazong_ads__acme: ["campaign_history"], amazon__empty: [] } }, flags);
  assert.equal(await deliver(app, pubsubBody("amazong_ads__acme", NOW)), 204);
  assert.equal(await deliver(app, pubsubBody("amazon__empty", NOW)), 429);
  assert.equal(await deliver(app, pubsubBody("amazon__empty", NOW - 73 * HOUR)), 204);
  assert.equal(dispatched.length, 0);
  assert.deepEqual(flags.calls, [{ parent: "amazong_ads", client: "acme" }]);
});

test("sets the company flag after dispatching a dataset whose tables exist", async () => {
  const calls = [];
  const { app, dispatched } = setup(
    { datasets: { fairing__acme: ["responses"] } },
    { setCompanyFlag: async (args) => (calls.push(args), { status: "set" }) }
  );
  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.deepEqual(calls, [{ parent: "fairing", client: "acme" }]);
});

test("leaves company settings alone when dispatching with missing tables", async () => {
  const calls = [];
  const { app, dispatched } = setup(
    { datasets: {} },
    { setCompanyFlag: async (args) => (calls.push(args), { status: "set" }) }
  );
  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW - 73 * HOUR)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(calls.length, 0);
});

test("leaves company settings alone when the table check itself failed", async () => {
  const calls = [];
  const denied = Object.assign(new Error("Access Denied"), { code: 403 });
  const { app, dispatched } = setup(
    { error: denied },
    { setCompanyFlag: async (args) => (calls.push(args), { status: "set" }) }
  );
  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(calls.length, 0);
});

test("a failed company update doesn't fail the message (a retry would re-dispatch)", async () => {
  const { app, dispatched } = setup(
    { datasets: { fairing__acme: ["responses"] } },
    { setCompanyFlag: async () => { throw new Error("Supabase down"); } }
  );
  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW)), 204);
  assert.equal(dispatched.length, 1);
});

// Fake Supabase REST API: `companies` are the rows a GET returns; PATCHes
// return `patched` (defaults to one updated row).
function fakeSupabase({ companies = [], patched } = {}) {
  const requests = [];
  const fetchImpl = async (url, init = {}) => {
    requests.push({ url, method: init.method || "GET", body: init.body && JSON.parse(init.body), headers: init.headers });
    const rows = init.method === "PATCH" ? patched ?? [{}] : companies;
    return { ok: true, json: async () => rows, text: async () => "" };
  };
  const setCompanyFlag = createCompanyFlagSetter({ url: "https://sb.test", serviceRoleKey: "key", fetchImpl });
  return { setCompanyFlag, requests };
}

test("sets hdyhau to Fairing when it's still 'none'", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, company_name: "Acme", hdyhau: "none" }] });
  const result = await setCompanyFlag({ parent: "fairing", client: "acme" });
  assert.deepEqual(result, { status: "set", column: "hdyhau", company: "Acme", from: "none", to: "Fairing" });
  assert.equal(requests[0].url, "https://sb.test/rest/v1/companies?table_identifier=eq.acme&select=id,company_name,table_identifier,hdyhau");
  assert.equal(requests[1].method, "PATCH");
  assert.equal(requests[1].url, "https://sb.test/rest/v1/companies?id=eq.7&or=(hdyhau.is.null,hdyhau.eq.none)");
  assert.deepEqual(requests[1].body, { hdyhau: "Fairing" });
  assert.equal(requests[1].headers.Authorization, "Bearer key");
});

test("turns on a boolean ad platform flag", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, company_name: "Acme", applovin_ads: null }] });
  const result = await setCompanyFlag({ parent: "applovin", client: "acme" });
  assert.equal(result.status, "set");
  assert.equal(requests[1].url, "https://sb.test/rest/v1/companies?id=eq.7&or=(applovin_ads.is.null,applovin_ads.eq.false)");
  assert.deepEqual(requests[1].body, { applovin_ads: true });
});

test("never overwrites a value that was set by hand", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, company_name: "Acme", hdyhau: "Custom" }] });
  const result = await setCompanyFlag({ parent: "fairing", client: "acme" });
  assert.equal(result.status, "already_set");
  assert.equal(result.current, "Custom");
  assert.equal(requests.length, 1);
});

test("reports already_set when the conditional update matches no row", async () => {
  const { setCompanyFlag } = fakeSupabase({ companies: [{ id: 7, company_name: "Acme", hdyhau: "none" }], patched: [] });
  assert.equal((await setCompanyFlag({ parent: "fairing", client: "acme" })).status, "already_set");
});

test("skips when no company or several companies match the client", async () => {
  for (const companies of [[], [{ id: 1 }, { id: 2 }]]) {
    const { setCompanyFlag, requests } = fakeSupabase({ companies });
    const result = await setCompanyFlag({ parent: "google_ads", client: "acme" });
    assert.equal(result.status, "company_not_unique");
    assert.equal(result.matches, companies.length);
    assert.equal(requests.length, 1);
  }
});

test("matches Amazon datasets on the longest table_identifier the client starts with", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({
    companies: [
      { id: 1, company_name: "Acme", table_identifier: "acme", amazon: false },
      { id: 2, company_name: "Acme US", table_identifier: "acme_us", amazon: null },
    ],
  });
  const result = await setCompanyFlag({ parent: "amazon_sellercentral", client: "acme_us_airbyte" });
  assert.deepEqual(result, { status: "set", column: "amazon", company: "Acme US", from: null, to: true });
  assert.equal(
    requests[0].url,
    "https://sb.test/rest/v1/companies?table_identifier=in.(acme,acme_us,acme_us_airbyte)&select=id,company_name,table_identifier,amazon"
  );
  assert.equal(requests[1].url, "https://sb.test/rest/v1/companies?id=eq.2&or=(amazon.is.null,amazon.eq.false)");
  assert.deepEqual(requests[1].body, { amazon: true });
});

test("skips an Amazon dataset whose client matches no company", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [] });
  const result = await setCompanyFlag({ parent: "amazon_ads", client: "rally_amazon_ads_source" });
  assert.equal(result.status, "company_not_unique");
  assert.equal(requests.length, 1);
});

test("does nothing for sources without a company setting", async () => {
  const { setCompanyFlag, requests } = fakeSupabase();
  assert.deepEqual(await setCompanyFlag({ parent: "shopify", client: "acme" }), { status: "no_flag" });
  assert.equal(requests.length, 0);
});

test("gives up on a Supabase call that hangs", async () => {
  const setCompanyFlag = createCompanyFlagSetter({
    url: "https://sb.test",
    serviceRoleKey: "key",
    timeoutMs: 20,
    // Never answers; the long timer keeps the event loop alive like an open socket.
    fetchImpl: (_url, init) =>
      new Promise((_resolve, reject) => {
        const socket = setTimeout(() => {}, 10000);
        init.signal.addEventListener("abort", () => (clearTimeout(socket), reject(init.signal.reason)));
      }),
  });
  await assert.rejects(setCompanyFlag({ parent: "fairing", client: "acme" }), { name: "TimeoutError" });
});

test("throws on a Supabase error so the caller logs it", async () => {
  const setCompanyFlag = createCompanyFlagSetter({
    url: "https://sb.test",
    serviceRoleKey: "key",
    fetchImpl: async () => ({ ok: false, status: 401, text: async () => "bad key" }),
  });
  await assert.rejects(setCompanyFlag({ parent: "fairing", client: "acme" }), /401 bad key/);
});

// --- Source dataset ping (Supabase edge function source-dataset-created) ---

const SECRET = "test-source-secret-must-not-be-logged";
const FACEBOOK_TABLES = ["ad_sets", "ads", "ads_insights", "campaigns"];

// The push body as the Pub/Sub subscription sends it: BigQuery's InsertDataset
// audit log entry, base64-encoded in message.data.
function auditLogPush(datasetId, createdAt) {
  const logEntry = {
    protoPayload: {
      "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
      serviceName: "bigquery.googleapis.com",
      methodName: "google.cloud.bigquery.v2.DatasetService.InsertDataset",
      resourceName: `projects/orcaanalytics/datasets/${datasetId}`,
      authenticationInfo: { principalEmail: "connector@example.com" },
      resourceLocation: { currentLocations: ["US"] },
    },
    resource: { type: "bigquery_dataset", labels: { project_id: "orcaanalytics", dataset_id: datasetId } },
    timestamp: new Date(createdAt).toISOString(),
    severity: "NOTICE",
    logName: "projects/orcaanalytics/logs/cloudaudit.googleapis.com%2Factivity",
  };
  return {
    message: {
      data: Buffer.from(JSON.stringify(logEntry)).toString("base64"),
      messageId: "1234567890",
      publishTime: new Date(createdAt + 5000).toISOString(),
    },
    subscription: "projects/orcaanalytics/subscriptions/test",
  };
}

// Local stand-in for the edge function: records each request and answers with
// `status`, or never with `hang`.
async function fakeEdgeFunction(t, { status = 200, hang = false } = {}) {
  const requests = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      requests.push({ method: req.method, path: req.url, headers: req.headers, body: JSON.parse(body) });
      if (!hang) res.writeHead(status, { "Content-Type": "application/json" }).end(JSON.stringify({ status }));
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => (server.closeAllConnections(), server.close()));
  return { url: `http://127.0.0.1:${server.address().port}/functions/v1/source-dataset-created`, requests };
}

// Everything the handler logs, one string per call.
function captureLogs(t) {
  const lines = [];
  for (const level of ["log", "warn", "error"]) {
    t.mock.method(console, level, (...args) =>
      lines.push(args.map((a) => (typeof a === "string" ? a : util.inspect(a, { depth: null }))).join(" "))
    );
  }
  return lines;
}

test("pings source-dataset-created with the dataset id and shared secret after dispatching", async (t) => {
  const logs = captureLogs(t);
  const edge = await fakeEdgeFunction(t);
  const { app, dispatched } = setup(
    { datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } },
    { notifySourceDataset: createSourceDatasetNotifier({ url: edge.url, secret: SECRET }) }
  );
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW - HOUR)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(edge.requests.length, 1);
  const [ping] = edge.requests;
  assert.equal(ping.method, "POST");
  assert.equal(ping.path, "/functions/v1/source-dataset-created");
  assert.deepEqual(ping.body, { dataset_id: "facebook_ads__testbrand" });
  assert.equal(ping.headers["x-orca-source-secret"], SECRET);
  assert.equal(ping.headers["content-type"], "application/json");
  assert.equal(ping.headers.authorization, undefined);
  assert.ok(logs.some((l) => l.startsWith("SOURCE_DATASET_PINGED") && l.includes("status: 200")));
  assert.ok(!logs.some((l) => l.includes(SECRET)));
});

test("only pings for datasets it dispatches", async (t) => {
  captureLogs(t);
  const pinged = [];
  const { app, dispatched } = setup(
    {
      datasets: {
        shopify__acme: ["orders"],
        amazong_ads__acme: ["campaign_history"],
        amazon_sellercentral__acme_airbyte_fulfillments: ["GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL"],
      },
    },
    { notifySourceDataset: async (id) => (pinged.push(id), 200) }
  );
  assert.equal(await deliver(app, auditLogPush("unknown__acme", NOW)), 204); // no template
  assert.equal(await deliver(app, auditLogPush("shopify__acme", NOW)), 429); // still waiting for tables
  assert.equal(await deliver(app, auditLogPush("amazong_ads__acme", NOW)), 204); // company setting only
  // NOT_DISPATCHING: none of the template's tables after the max wait
  assert.equal(await deliver(app, auditLogPush("amazon_sellercentral__acme_airbyte_fulfillments", NOW - 73 * HOUR)), 204);
  assert.equal(dispatched.length, 0);
  assert.deepEqual(pinged, []);
});

test("doesn't ping when the GitHub dispatch fails", async (t) => {
  captureLogs(t);
  const pinged = [];
  const { app } = setup(
    { datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } },
    { dispatchError: new Error("GitHub down"), notifySourceDataset: async (id) => (pinged.push(id), 200) }
  );
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW)), 500);
  assert.deepEqual(pinged, []);
});

test("an edge function error never fails the message (a retry would re-dispatch)", async (t) => {
  const logs = captureLogs(t);
  const edge = await fakeEdgeFunction(t, { status: 401 });
  const { app, dispatched } = setup(
    { datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } },
    { notifySourceDataset: createSourceDatasetNotifier({ url: edge.url, secret: SECRET }) }
  );
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(edge.requests.length, 1);
  assert.ok(logs.some((l) => l.startsWith("SOURCE_DATASET_PING_FAILED") && l.includes("failed: 401")));
  assert.ok(!logs.some((l) => l.includes(SECRET)));
});

test("an unreachable edge function never fails the message", async (t) => {
  const logs = captureLogs(t);
  // A port nothing listens on any more.
  const closed = http.createServer();
  await new Promise((resolve) => closed.listen(0, "127.0.0.1", resolve));
  const { port } = closed.address();
  await new Promise((resolve) => closed.close(resolve));
  const { app, dispatched } = setup(
    { datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } },
    { notifySourceDataset: createSourceDatasetNotifier({ url: `http://127.0.0.1:${port}/`, secret: SECRET }) }
  );
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.ok(logs.some((l) => l.startsWith("SOURCE_DATASET_PING_FAILED") && l.includes("ECONNREFUSED")));
  assert.ok(!logs.some((l) => l.includes(SECRET)));
});

test("gives up on an edge function that hangs, and still acks", async (t) => {
  const logs = captureLogs(t);
  const edge = await fakeEdgeFunction(t, { hang: true });
  const { app, dispatched } = setup(
    { datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } },
    { notifySourceDataset: createSourceDatasetNotifier({ url: edge.url, secret: SECRET, timeoutMs: 50 }) }
  );
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(edge.requests.length, 1);
  assert.ok(logs.some((l) => l.startsWith("SOURCE_DATASET_PING_FAILED") && l.includes("timeout")));
});

test("skips the ping with one log line when it isn't configured", async (t) => {
  const logs = captureLogs(t);
  const { app, dispatched } = setup({ datasets: { facebook_ads__testbrand: FACEBOOK_TABLES } });
  assert.equal(await deliver(app, auditLogPush("facebook_ads__testbrand", NOW)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(logs.filter((l) => l.startsWith("SOURCE_DATASET_PING")).length, 1);
  assert.ok(logs.some((l) => l.startsWith("SOURCE_DATASET_PING_SKIPPED")));
});

// --- Optional entries (Google Ads search terms) ---

const SEARCHTERMS = {
  template: "templates/google_ads/searchterms/google_ads_searchterms.sql",
  path: "models/google_ads/searchterms/google_ads_searchterms__acme.sql",
};
const GOOGLE_ADS_TABLES = ["custom_campaign", "custom_keyword", "custom_shopping"];

test("google_ads plan scaffolds the searchterms model as an optional file it waits a day for", () => {
  const plan = buildPlan("google_ads__acme", "orcaanalytics");
  assert.deepEqual(plan.optional, [{ file: SEARCHTERMS, tables: ["google_ads__acme.custom_searchterms", "google_ads__acme.custom_pmax_searchterms"], waitHours: 24 }]);
  assert.ok(!plan.files.some((f) => f.path === SEARCHTERMS.path));
  assert.ok(!plan.tables.includes("google_ads__acme.custom_searchterms"));
});

test("waits for custom_searchterms during its first day", async () => {
  const { app, dispatched } = setup({ datasets: { google_ads__acme: GOOGLE_ADS_TABLES } });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - 23 * HOUR)), 429);
  assert.equal(dispatched.length, 0);
});

test("after a day without custom_searchterms, dispatches without its model", async () => {
  const { app, dispatched } = setup({ datasets: { google_ads__acme: GOOGLE_ADS_TABLES } });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - 25 * HOUR)), 204);
  assert.equal(dispatched.length, 1);
  assert.deepEqual(dispatched[0].missingTables, []);
  assert.deepEqual(dispatched[0].files, buildPlan("google_ads__acme", "orcaanalytics").files);
});

test("empty search-term tables (e.g. no Search/Shopping or no PMax campaigns) still count as landed", async () => {
  const { app, dispatched } = setup({
    datasets: { google_ads__acme: [...GOOGLE_ADS_TABLES, "custom_searchterms", "custom_pmax_searchterms"] },
  });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - HOUR)), 204);
  assert.deepEqual(dispatched[0].files, [...buildPlan("google_ads__acme", "orcaanalytics").files, SEARCHTERMS]);
});

test("an older connection with only custom_searchterms skips the model after a day (template needs both)", async () => {
  const { app, dispatched } = setup({ datasets: { google_ads__acme: [...GOOGLE_ADS_TABLES, "custom_searchterms"] } });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - HOUR)), 429);
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - 25 * HOUR)), 204);
  assert.deepEqual(dispatched[0].files, buildPlan("google_ads__acme", "orcaanalytics").files);
});

test("still waits for the required Google Ads tables", async () => {
  const { app, dispatched } = setup({ datasets: { google_ads__acme: ["custom_campaign", "custom_searchterms", "custom_pmax_searchterms"] } });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - HOUR)), 429);
  assert.equal(dispatched.length, 0);
});

test("keeps waiting for the required Google Ads tables after the searchterms day", async () => {
  const { app, dispatched } = setup({ datasets: { google_ads__acme: ["custom_campaign", "custom_searchterms", "custom_pmax_searchterms"] } });
  assert.equal(await deliver(app, pubsubBody("google_ads__acme", NOW - 30 * HOUR)), 429);
  assert.equal(dispatched.length, 0);
});

// --- Pacing model (ENG-299) ---

// Stands in for createCompanySettingsReader: `company` is the one companies row
// for the client (null for none), or `error` makes every read throw.
function companySettings({ company = { timezone: "America/Los_Angeles", revenue_measured_by: "Gross - Discounts" }, error } = {}) {
  const reads = [];
  const readCompanySettings = async (client, options) => {
    reads.push(options?.matchClientPrefix ? `${client} (prefix)` : client);
    if (error) throw error;
    if (!company) return { status: "company_not_unique", matches: 0 };
    return { status: "ok", timezone: company.timezone, revenueMeasuredBy: company.revenue_measured_by };
  };
  return { reads, readCompanySettings };
}

const PACING_FILE = { template: "templates/pacing/pacing.sql", path: "models/pacing/pacing__acme.sql" };

test("first render: Shopify's scaffold sends the pacing model with the company's settings", async (t) => {
  const logs = captureLogs(t);
  const { reads, readCompanySettings } = companySettings();
  const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - HOUR)), 204);
  assert.deepEqual(dispatched[0].generated, [
    { ...PACING_FILE, vars: { sources: ["shopify"], timezone_tag: "pst", revenue: "gross_discounts" } },
  ]);
  assert.deepEqual(dispatched[0].files, buildPlan("shopify__acme", "orcaanalytics").files);
  assert.deepEqual(reads, ["acme"]);
  assert.ok(logs.some((l) => l.startsWith("PACING_INCLUDED")));
});

test("re-render: a new source's scaffold sends the model with every source that has landed", async () => {
  const { readCompanySettings } = companySettings();
  const { app, dispatched } = setup(
    {
      datasets: {
        facebook_ads__acme: FACEBOOK_TABLES,
        shopify__acme: ["orders"],
        google_ads__acme: ["custom_campaign"],
        tiktok_ads__acme: [], // dataset made, first sync not landed: not a source yet
        klaviyo__acme: ["profiles"], // not a pacing source
      },
    },
    { readCompanySettings }
  );
  assert.equal(await deliver(app, pubsubBody("facebook_ads__acme", NOW - HOUR)), 204);
  assert.deepEqual(dispatched[0].generated[0].vars.sources, ["shopify", "facebook_ads", "google_ads"]);
});

test("a spend source that lands before Shopify sends no pacing model yet", async (t) => {
  const logs = captureLogs(t);
  const { reads, readCompanySettings } = companySettings();
  const { app, dispatched } = setup({ datasets: { facebook_ads__acme: FACEBOOK_TABLES } }, { readCompanySettings });
  assert.equal(await deliver(app, pubsubBody("facebook_ads__acme", NOW - HOUR)), 204);
  assert.equal(dispatched.length, 1);
  assert.equal(dispatched[0].generated, undefined);
  assert.deepEqual(reads, ["acme"]); // for the scaffold's own settings
  assert.ok(logs.some((l) => l.startsWith("PACING_SKIPPED") && l.includes("shopify__acme hasn't landed")));
});

test("can't overwrite a hand-built model: pacing only ever goes in `generated`, which orca-dbt writes only while the file is generated", async () => {
  for (const datasetId of Object.keys(CATALOG).map((parent) => `${parent}__acme`)) {
    assert.ok(!buildPlan(datasetId, "orcaanalytics").files.some((f) => f.path.startsWith("models/pacing/")), datasetId);
  }
  const { readCompanySettings } = companySettings();
  const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
  await deliver(app, pubsubBody("shopify__acme", NOW - HOUR));
  assert.ok(!dispatched[0].files.some((f) => f.path.startsWith("models/pacing/")));
  assert.equal(dispatched[0].generated[0].path, PACING_FILE.path);
});

test("unset settings render as est and Gross Revenue", async () => {
  const { readCompanySettings } = companySettings({ company: { timezone: null, revenue_measured_by: null } });
  const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
  await deliver(app, pubsubBody("shopify__acme", NOW - HOUR));
  assert.deepEqual(dispatched[0].generated[0].vars, { sources: ["shopify"], timezone_tag: "est", revenue: "gross" });
});

test("no pacing model for an unmatched company or a custom revenue definition; the scaffold still goes", async (t) => {
  for (const company of [null, { timezone: "America/New_York", revenue_measured_by: "Gross - Grin orders" }]) {
    const logs = captureLogs(t);
    const { readCompanySettings } = companySettings({ company });
    const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
    assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - HOUR)), 204);
    assert.equal(dispatched.length, 1);
    assert.equal(dispatched[0].generated, undefined);
    assert.ok(logs.some((l) => l.startsWith("PACING_SKIPPED")));
  }
});

test("redelivers (503) when Supabase is unreachable, since nothing is dispatched yet", async (t) => {
  captureLogs(t);
  const { readCompanySettings } = companySettings({ error: new TypeError("fetch failed") });
  const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - HOUR)), 503);
  assert.equal(dispatched.length, 0);
});

test("after the max wait, or on a 4xx that won't clear, dispatches without the pacing model", async (t) => {
  captureLogs(t);
  const cases = [
    [new TypeError("fetch failed"), 73 * HOUR],
    [Object.assign(new Error("Supabase GET failed: 401 bad key"), { status: 401 }), HOUR],
  ];
  for (const [error, age] of cases) {
    const { readCompanySettings } = companySettings({ error });
    const { app, dispatched } = setup({ datasets: { shopify__acme: SHOPIFY_TABLES } }, { readCompanySettings });
    assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW - age)), 204);
    assert.equal(dispatched.length, 1);
    assert.equal(dispatched[0].generated, undefined);
  }
});

test("no pacing model when the table check failed (can't tell what landed); the scaffold keeps its settings", async (t) => {
  captureLogs(t);
  const { readCompanySettings } = companySettings();
  const denied = Object.assign(new Error("Access Denied"), { code: 403 });
  const { app, dispatched } = setup({ error: denied }, { readCompanySettings });
  assert.equal(await deliver(app, pubsubBody("shopify__acme", NOW)), 204);
  assert.equal(dispatched[0].generated, undefined);
  assert.equal(dispatched[0].vars.timezone, "America/Los_Angeles");
});

test("sources the pacing model doesn't read never send it", async () => {
  const { reads, readCompanySettings } = companySettings();
  const { app, dispatched } = setup(
    { datasets: { klaviyo__acme: ["profiles"], shopify__acme: SHOPIFY_TABLES } },
    { readCompanySettings }
  );
  assert.equal(await deliver(app, pubsubBody("klaviyo__acme", NOW - HOUR)), 204);
  assert.equal(dispatched[0].generated, undefined);
  assert.deepEqual(reads, ["acme"]); // for the scaffold's own settings
});

test("timezone tags follow the nearest dbt Cloud schedule", () => {
  const expected = {
    "America/New_York": "est",
    "America/Toronto": "est",
    "America/Chicago": "cst",
    "America/Denver": "mst",
    "America/Phoenix": "mst",
    "America/Los_Angeles": "pst",
    "Europe/London": "gmt",
    "Europe/Berlin": "gmt",
    "Australia/Sydney": "aest",
    "Australia/Brisbane": "aest",
    UTC: "gmt",
  };
  for (const [zone, tag] of Object.entries(expected)) assert.equal(timezoneTag(zone), tag, zone);
  for (const unset of [null, undefined, "", "Not/AZone"]) assert.equal(timezoneTag(unset), "est");
});

test("reads timezone and revenue basis for the one company with that table_identifier", async () => {
  const requests = [];
  const reply = (rows) => async (url, init) => (requests.push({ url, headers: init.headers }), { ok: true, json: async () => rows });
  const one = createCompanySettingsReader({
    url: "https://sb.test",
    serviceRoleKey: "key",
    fetchImpl: reply([{ table_identifier: "acme", timezone: "America/Chicago", revenue_measured_by: "Gross Revenue" }]),
  });
  assert.deepEqual(await one("acme"), { status: "ok", timezone: "America/Chicago", revenueMeasuredBy: "Gross Revenue" });
  assert.equal(
    requests[0].url,
    "https://sb.test/rest/v1/companies?table_identifier=eq.acme&select=table_identifier,timezone,revenue_measured_by"
  );
  assert.equal(requests[0].headers.Authorization, "Bearer key");

  const two = createCompanySettingsReader({ url: "https://sb.test", serviceRoleKey: "key", fetchImpl: reply([{}, {}]) });
  assert.deepEqual(await two("acme"), { status: "company_not_unique", matches: 2 });

  // Amazon: the longest table_identifier the client starts with.
  const amazon = createCompanySettingsReader({
    url: "https://sb.test",
    serviceRoleKey: "key",
    fetchImpl: reply([
      { table_identifier: "acme", timezone: "America/New_York", revenue_measured_by: null },
      { table_identifier: "acme_us", timezone: "America/Denver", revenue_measured_by: "Net (Gross - Discounts - Returns)" },
    ]),
  });
  assert.deepEqual(await amazon("acme_us_airbyte", { matchClientPrefix: true }), {
    status: "ok",
    timezone: "America/Denver",
    revenueMeasuredBy: "Net (Gross - Discounts - Returns)",
  });
  assert.equal(
    requests.at(-1).url,
    "https://sb.test/rest/v1/companies?table_identifier=in.(acme,acme_us,acme_us_airbyte)&select=table_identifier,timezone,revenue_measured_by"
  );

  const denied = createCompanySettingsReader({
    url: "https://sb.test",
    serviceRoleKey: "key",
    fetchImpl: async () => ({ ok: false, status: 401, text: async () => "bad key" }),
  });
  await assert.rejects(denied("acme"), { status: 401 });
});

// --- Scaffold settings (ENG-306) ---

test("every scaffold carries the company's timezone, schedule tag and revenue basis", async (t) => {
  const logs = captureLogs(t);
  for (const [datasetId, datasets] of [
    ["shopify__acme", { shopify__acme: SHOPIFY_TABLES }],
    ["facebook_ads__acme", { facebook_ads__acme: FACEBOOK_TABLES }],
    ["fairing__acme", { fairing__acme: ["responses"] }],
  ]) {
    const { readCompanySettings } = companySettings();
    const { app, dispatched } = setup({ datasets }, { readCompanySettings });
    assert.equal(await deliver(app, pubsubBody(datasetId, NOW - HOUR)), 204);
    assert.deepEqual(dispatched[0].vars, {
      ...buildPlan(datasetId, "orcaanalytics").vars,
      timezone: "America/Los_Angeles",
      timezone_tag: "pst",
      revenue: "gross_discounts",
    });
  }
  assert.ok(logs.some((l) => l.startsWith("SCAFFOLD_SETTINGS") && l.includes("'America/Los_Angeles'")));
});

test("settings that are unknown or can't be used keep New York, est and Gross Revenue", async (t) => {
  const logs = captureLogs(t);
  const NEW_YORK = { timezone: "America/New_York", timezone_tag: "est", revenue: "gross" };
  const cases = [
    [undefined, NEW_YORK], // Supabase variables not set
    [companySettings({ company: null }).readCompanySettings, NEW_YORK],
    [companySettings({ company: { timezone: null, revenue_measured_by: null } }).readCompanySettings, NEW_YORK],
    [
      companySettings({ company: { timezone: "America/Chicago'); drop table x; --", revenue_measured_by: "Gross - Grin orders" } })
        .readCompanySettings,
      NEW_YORK,
    ],
    [
      companySettings({ company: { timezone: "Not/AZone", revenue_measured_by: "Net (Gross - Discounts - Returns)" } }).readCompanySettings,
      { ...NEW_YORK, revenue: "net" },
    ],
  ];
  for (const [readCompanySettings, expected] of cases) {
    const { app, dispatched } = setup({ datasets: { facebook_ads__acme: FACEBOOK_TABLES } }, { readCompanySettings });
    assert.equal(await deliver(app, pubsubBody("facebook_ads__acme", NOW - HOUR)), 204);
    const { timezone, timezone_tag, revenue } = dispatched[0].vars;
    assert.deepEqual({ timezone, timezone_tag, revenue }, expected);
  }
  assert.ok(logs.some((l) => l.startsWith("SCAFFOLD_SETTINGS") && l.includes("revenue_measured_by: 'Gross - Grin orders'")));
});

test("Amazon scaffolds find the company the way the Amazon setting does, by table_identifier prefix", async () => {
  const { reads, readCompanySettings } = companySettings();
  const { app, dispatched } = setup(
    { datasets: { amazon_sellercentral__acme_airbyte: ["GET_SALES_AND_TRAFFIC_REPORT", "Orders"] } },
    { readCompanySettings }
  );
  assert.equal(await deliver(app, pubsubBody("amazon_sellercentral__acme_airbyte", NOW - HOUR)), 204);
  assert.deepEqual(reads, ["acme_airbyte (prefix)"]);
  assert.equal(dispatched[0].vars.timezone, "America/Los_Angeles");
});

test("any scaffold redelivers (503) while Supabase is unreachable, then goes with the defaults", async (t) => {
  captureLogs(t);
  const { readCompanySettings } = companySettings({ error: new TypeError("fetch failed") });
  const { app, dispatched } = setup({ datasets: { fairing__acme: ["responses"] } }, { readCompanySettings });
  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW - HOUR)), 503);
  assert.equal(dispatched.length, 0);

  assert.equal(await deliver(app, pubsubBody("fairing__acme", NOW - 73 * HOUR)), 204);
  assert.equal(dispatched[0].vars.timezone, "America/New_York");
});

test("scaffoldSettings maps a company's settings to the dispatch vars", () => {
  const ok = (timezone, revenueMeasuredBy) => scaffoldSettings({ status: "ok", timezone, revenueMeasuredBy });
  assert.deepEqual(ok("America/Phoenix", "Total (Gross - Discounts - Returns + Shipping + Tax)"), {
    vars: { timezone: "America/Phoenix", timezone_tag: "mst", revenue: "total" },
    defaulted: {},
  });
  assert.deepEqual(ok("Europe/Berlin", " Gross + Shipping ").vars, { timezone: "Europe/Berlin", timezone_tag: "gmt", revenue: "gross_shipping" });
  // Only an IANA zone reaches the SQL.
  for (const zone of ["+05:00", "America/New_York'", "America/New York", "Bogus/Zone", 42]) {
    assert.deepEqual(ok(zone, null), {
      vars: { timezone: "America/New_York", timezone_tag: "est", revenue: "gross" },
      defaulted: { timezone: zone },
    });
  }
  assert.deepEqual(scaffoldSettings({ status: "company_not_unique", matches: 2 }), {
    vars: { timezone: "America/New_York", timezone_tag: "est", revenue: "gross" },
    defaulted: { company: "company_not_unique" },
  });
});
