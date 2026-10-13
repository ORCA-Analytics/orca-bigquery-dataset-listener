const test = require("node:test");
const assert = require("node:assert/strict");
const { buildPlan, createApp, createCompanyFlagSetter } = require("../server.js");

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

function setup(bigqueryOptions, { dispatchError, setCompanyFlag } = {}) {
  const dispatched = [];
  const app = createApp({
    setCompanyFlag,
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
  assert.deepEqual(buildPlan("amazon__acme", "orcaanalytics").tables.map((t) => t.split(".")[0]).sort()[0], "amazon_ads__acme");
});

test("dispatches right away for platforms that read no raw tables", async () => {
  const { app, dispatched } = setup({ datasets: {} });
  assert.equal(await deliver(app, pubsubBody("pacing__acme", NOW)), 204);
  assert.equal(dispatched.length, 1);
});

test("returns 500 when the GitHub dispatch fails, so Pub/Sub retries", async () => {
  const { app } = setup({ datasets: { rakuten__acme: ["report"] } }, { dispatchError: new Error("GitHub down") });
  assert.equal(await deliver(app, pubsubBody("rakuten__acme", NOW)), 500);
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
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, name: "Acme", hdyhau: "none" }] });
  const result = await setCompanyFlag({ parent: "fairing", client: "acme" });
  assert.deepEqual(result, { status: "set", column: "hdyhau", company: "Acme", from: "none", to: "Fairing" });
  assert.equal(requests[0].url, "https://sb.test/rest/v1/companies?table_identifier=eq.acme&select=id,name,hdyhau");
  assert.equal(requests[1].method, "PATCH");
  assert.equal(requests[1].url, "https://sb.test/rest/v1/companies?id=eq.7&or=(hdyhau.is.null,hdyhau.eq.none)");
  assert.deepEqual(requests[1].body, { hdyhau: "Fairing" });
  assert.equal(requests[1].headers.Authorization, "Bearer key");
});

test("turns on a boolean ad platform flag", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, name: "Acme", applovin_ads: null }] });
  const result = await setCompanyFlag({ parent: "applovin", client: "acme" });
  assert.equal(result.status, "set");
  assert.equal(requests[1].url, "https://sb.test/rest/v1/companies?id=eq.7&or=(applovin_ads.is.null,applovin_ads.eq.false)");
  assert.deepEqual(requests[1].body, { applovin_ads: true });
});

test("never overwrites a value that was set by hand", async () => {
  const { setCompanyFlag, requests } = fakeSupabase({ companies: [{ id: 7, name: "Acme", hdyhau: "Custom" }] });
  const result = await setCompanyFlag({ parent: "fairing", client: "acme" });
  assert.equal(result.status, "already_set");
  assert.equal(result.current, "Custom");
  assert.equal(requests.length, 1);
});

test("reports already_set when the conditional update matches no row", async () => {
  const { setCompanyFlag } = fakeSupabase({ companies: [{ id: 7, name: "Acme", hdyhau: "none" }], patched: [] });
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
