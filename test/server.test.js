const test = require("node:test");
const assert = require("node:assert/strict");
const { buildPlan, createApp } = require("../server.js");

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

function setup(bigqueryOptions, { dispatchError } = {}) {
  const dispatched = [];
  const app = createApp({
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
