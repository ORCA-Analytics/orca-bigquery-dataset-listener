// server.js
const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

// --- Environment variables ---
const GITHUB_TOKEN = process.env.GITHUB_TOKEN; // stored in Secret Manager
const GH_OWNER = "ORCA-Analytics";
const GH_REPO = "orca-dbt";
const PROJECT = "orcaanalytics";
// How long to keep waiting for a new dataset's tables before dispatching anyway.
const MAX_WAIT_HOURS = Number(process.env.MAX_WAIT_HOURS || 72);
// Supabase project with the `companies` table the ORCA app reads its settings from.
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // stored in Secret Manager

// --- 1️⃣ buildPlan logic ---
function splitDataset(datasetId) {
  const m = datasetId.match(/^([a-z0-9_]+)__([a-z0-9_]+)$/);
  return m ? { parent: m[1], client: m[2] } : null;
}

function tPath(baseDir, rel, tpl) {
  return `templates/${baseDir}/${rel ? rel + "/" : ""}${tpl}.sql`;
}
function oPath(baseDir, rel, name) {
  return `models/${baseDir}/${rel ? rel + "/" : ""}${name}`;
}

// `tables` lists the raw tables each template reads, as "dataset.table" with
// {client} filled in at dispatch time. The listener waits for all of them to
// exist before dispatching, so keep them in sync with the templates in orca-dbt.
// "dataset.a|b" is satisfied by either table (loaders name some tables
// differently, e.g. Airbyte `Orders` vs Fivetran `orders`).
//
// `requireSomeTables` drops a dataset that still has none of its tables after
// the max wait instead of dispatching anyway. Amazon datasets come with many
// suffixes after the client (amazon_sellercentral__acme_airbyte_fulfillments,
// Fivetran dbt-package outputs like amazon_ads__acme_amazon_ads_source) and
// only some of them are the raw source a template reads.
const CATALOG = {
  facebook_ads: {
    entries: [
      {
        rel: "campaigns",
        tpl: "facebook_ads",
        tables: [
          "facebook_ads__{client}.ad_sets",
          "facebook_ads__{client}.ads",
          "facebook_ads__{client}.ads_insights",
          "facebook_ads__{client}.campaigns",
        ],
      },
      { rel: "spendcohorts", tpl: "facebook_ads_spendcohorts" },
    ],
  },
  google_ads: {
    entries: [
      { rel: "campaigns", tpl: "google_ads", tables: ["google_ads__{client}.custom_campaign"] },
      { rel: "keywords", tpl: "google_ads_keywords", tables: ["google_ads__{client}.custom_keyword"] },
      { rel: "products", tpl: "google_ads_products", tables: ["google_ads__{client}.custom_shopping"] },
    ],
  },
  google_analytics_4: {
    entries: [
      {
        rel: "sessionscvr",
        tpl: "google_analytics_4_sessionscvr",
        out: (_tpl, client) =>
          `google_analytics_4__${client}_sessionscvr.sql`,
        tables: ["google_analytics_4__{client}.sessionscvr"],
      },
    ],
  },
  shareasale: {
    entries: [
      {
        rel: "shareasale_weeklyprogress",
        tpl: "shareasale_weeklyprogressreport",
        out: (_tpl, client) =>
          `shareasale__${client}_weeklyprogressreport.sql`,
        tables: ["shareasale__{client}_fivetran.weekly_progress"],
      },
    ],
  },
  shopify: {
    entries: [
      // Non-standard
      {
        rel: "cohort_subscription",
        tpl: "shopify_cohort_otptosub",
        out: (_tpl, client) => `shopify_cohort__${client}_otptosub.sql`,
        tables: ["shopify__{client}.orders"],
      },
      {
        rel: "cohort_subscription",
        tpl: "shopify_cohort_subfirstpurchase",
        out: (_tpl, client) =>
          `shopify_cohort__${client}_subfirstpurchase.sql`,
      },
      // Standard
      { rel: "cohort", tpl: "shopify_cohort" },
      { rel: "newreturn", tpl: "shopify_newreturn" },
      {
        rel: "orderlines",
        tpl: "shopify_orderlines",
        tables: ["shopify__{client}.orders", "shopify__{client}.product_variants", "shopify__{client}.products"],
      },
      { rel: "orders", tpl: "shopify_orders", tables: ["shopify__{client}.orders"] },
      { rel: "product_firstbasket", tpl: "shopify_product_firstbasket", tables: ["shopify__{client}.orders"] },
      {
        rel: "product_firstsecondorder",
        tpl: "shopify_product_firstsecondpurchase",
        tables: ["shopify__{client}.orders"],
      },
      {
        rel: "product_ltr_journey",
        tpl: "shopify_product_ltrjourney",
        tables: ["shopify__{client}.orders", "shopify__{client}.products"],
      },
      {
        rel: "product_ltr",
        tpl: "shopify_product_ltr",
        tables: ["shopify__{client}.orders", "shopify__{client}.products"],
      },
      {
        rel: "product",
        tpl: "shopify_products",
        tables: ["shopify__{client}.orders", "shopify__{client}.product_variants", "shopify__{client}.products"],
      },
      { rel: "refunds", tpl: "shopify_refunds", tables: ["shopify__{client}.order_refunds"] },
      {
        rel: "shopify_pixel/base_customervisits",
        tpl: "shopify_customervisits",
        tables: ["shopify__{client}.customer_journey_summary"],
      },
      { rel: "shopify_pixel/daily_channel", tpl: "shopify_pixel_dailychannel" },
      { rel: "shopify_pixel/extrapolated", tpl: "shopify_pixel_extrapolated" },
      { rel: "shopify_pixel/modeled", tpl: "shopify_pixel_modeled" },
      { rel: "shopify_pixel/percent_of_orders", tpl: "shopify_pixel_percentoforders" },
    ],
  },
  amazon_ads: {
    dir: "amazon",
    requireSomeTables: true,
    entries: [
      {
        rel: "amazon_ads",
        tpl: "amazon_ads",
        tables: [
          "amazon_ads__{client}.campaign_history",
          "amazon_ads__{client}.campaign_level_report",
          "amazon_ads__{client}.sb_campaign_history",
          "amazon_ads__{client}.sb_campaign_report",
        ],
      },
    ],
  },
  amazon_sellercentral: {
    dir: "amazon",
    requireSomeTables: true,
    entries: [
      {
        rel: "amazon_sellercentral",
        tpl: "amazon_sellercentral",
        tables: ["amazon_sellercentral__{client}.GET_SALES_AND_TRAFFIC_REPORT|sales_and_traffic_business_report_daily"],
      },
      {
        rel: "amazon_sellercentral_cohorts",
        tpl: "amazon_sellercentral_cohorts",
        tables: ["amazon_sellercentral__{client}.Orders|orders"],
      },
      {
        rel: "amazon_sellercentral_newreturn",
        tpl: "amazon_sellercentral_newreturn",
        tables: ["amazon_sellercentral__{client}.Orders|orders"],
      },
    ],
  },
  applovin: { entries: [{ rel: "", tpl: "applovin", tables: ["applovin__{client}.ad_report_daily"] }] },
  bing_ads: {
    entries: [{ rel: "", tpl: "bing_ads", tables: ["bing_ads__{client}.campaign_performance_report_daily"] }],
  },
  pinterest_ads: {
    entries: [{ rel: "", tpl: "pinterest_ads", tables: ["pinterest_ads__{client}.custom_custom_campaign2"] }],
  },
  snapchat_ads: {
    entries: [
      {
        rel: "",
        tpl: "snapchat_ads",
        tables: ["snapchat_ads__{client}.campaign_daily_report", "snapchat_ads__{client}.campaign_history"],
      },
    ],
  },
  tiktok_ads: {
    entries: [
      {
        rel: "campaigns",
        tpl: "tiktok_ads",
        tables: ["tiktok_ads__{client}.ads_reports_daily", "tiktok_ads__{client}.campaigns"],
      },
      {
        rel: "creative",
        tpl: "tiktok_ads_creative",
        tables: [
          "tiktok_ads__{client}.ad_groups",
          "tiktok_ads__{client}.ads",
          "tiktok_ads__{client}.ads_reports_daily",
          "tiktok_ads__{client}.campaigns",
          "tiktok_ads__{client}.creative_assets_images",
          "tiktok_ads__{client}.creative_assets_videos",
          "tiktok_ads__{client}.spark_ads",
        ],
      },
    ],
  },
  fairing: {
    dir: "hdyhau_fairing",
    entries: [{ rel: "", tpl: "fairing_hdyhau", tables: ["fairing__{client}.responses"] }],
  },
  knocommerce: {
    dir: "hdyhau_knocommerce",
    entries: [
      { rel: "all_responses", tpl: "knocommerce_allresponses", tables: ["knocommerce__{client}.responses"] },
      { rel: "hdyhau", tpl: "knocommerce_hdyhau" },
    ],
  },
  klaviyo: {
    entries: [
      {
        rel: "leadgen_sms",
        tpl: "klaviyo_leadgen_sms",
        tables: ["klaviyo__{client}.profiles", "shopify__{client}.orders"],
      },
      {
        rel: "leadgen",
        tpl: "klaviyo_leadgen",
        tables: ["klaviyo__{client}.profiles", "shopify__{client}.orders"],
      },
    ],
  },
  liveintent: {
    entries: [
      {
        rel: "",
        tpl: "liveintent",
        tables: ["liveintent__{client}.advertiser_dynamic", "liveintent__{client}.advertiser_yesterday"],
      },
    ],
  },
  pacing: { entries: [{ rel: "", tpl: "pacing" }] },
  rakuten: { entries: [{ rel: "", tpl: "rakuten", tables: ["rakuten__{client}.report"] }] },
  twitter_ads: { entries: [{ rel: "", tpl: "twitter_ads", tables: ["twitter_ads__{client}.campaign_report"] }] },
};

function buildPlan(datasetId, projectId) {
  const parts = splitDataset(datasetId);
  if (!parts) return null;
  const { parent, client } = parts;
  const cfg = CATALOG[parent];
  if (!cfg) return null;

  const baseDir = cfg.dir || parent; // <— use alias if provided
  const files = [];
  const tables = new Set();
  for (const e of cfg.entries) {
    const template = tPath(baseDir, e.rel, e.tpl);
    const outName = e.out ? e.out(e.tpl, client) : `${e.tpl}__${client}.sql`;
    files.push({ template, path: oPath(baseDir, e.rel, outName) });
    for (const t of e.tables || []) tables.add(t.replace("{client}", client));
  }
  return {
    files,
    tables: [...tables],
    requireSomeTables: Boolean(cfg.requireSomeTables),
    vars: { datasetId, parent, client, project: projectId },
  };
}

// --- 2️⃣ Table readiness ---
// Returns the "dataset.table" entries that don't exist yet ("dataset.a|b" is
// there when either table is). A dataset that doesn't exist yet counts as all
// of its tables missing.
async function findMissingTables(bigquery, tables) {
  const byDataset = new Map();
  for (const t of tables) {
    const [dataset, table] = t.split(".");
    if (!byDataset.has(dataset)) byDataset.set(dataset, []);
    byDataset.get(dataset).push(table);
  }

  const missing = [];
  for (const [dataset, wanted] of byDataset) {
    const existing = await listTables(bigquery, dataset);
    for (const table of wanted) {
      if (!table.split("|").some((name) => existing.has(name))) missing.push(`${dataset}.${table}`);
    }
  }
  return missing;
}

// The dataset's table names; empty when the dataset doesn't exist (yet).
async function listTables(bigquery, dataset) {
  try {
    const [found] = await bigquery.dataset(dataset).getTables();
    return new Set(found.map((t) => t.id));
  } catch (e) {
    if (e.code !== 404) throw e;
    return new Set();
  }
}

// --- 3️⃣ Company settings ---
// The `companies` column a new source turns on in the ORCA app, and the
// "not set up" value it may replace. A column that already holds anything
// else (e.g. hdyhau = 'Custom') was set by hand and is left alone.
const COMPANY_FLAGS = {
  facebook_ads: { column: "facebook_ads", value: true, unset: false },
  google_ads: { column: "google_ads", value: true, unset: false },
  tiktok_ads: { column: "tiktok_ads", value: true, unset: false },
  bing_ads: { column: "bing_ads", value: true, unset: false },
  pinterest_ads: { column: "pinterest_ads", value: true, unset: false },
  snapchat_ads: { column: "snapchat_ads", value: true, unset: false },
  twitter_ads: { column: "twitter_ads", value: true, unset: false },
  applovin: { column: "applovin_ads", value: true, unset: false },
  fairing: { column: "hdyhau", value: "Fairing", unset: "none" },
  knocommerce: { column: "hdyhau", value: "KnoCommerce", unset: "none" },
};

// Every amazon* dataset turns on `amazon`: amazon_ads, amazon_sellercentral,
// and names without a template (e.g. amazong_ads__acme). The client part often
// carries a suffix (acme_airbyte, acme_us_fivetran), so the company is the one
// whose table_identifier the client starts with, and the flag waits only for
// the dataset to hold any table rather than for a template's tables.
const AMAZON_FLAG = { column: "amazon", value: true, unset: false, matchClientPrefix: true };

function companyFlagFor(parent) {
  return COMPANY_FLAGS[parent] || (parent.startsWith("amazon") ? AMAZON_FLAG : null);
}

// "acme_us_airbyte" -> ["acme", "acme_us", "acme_us_airbyte"]
function clientPrefixes(client) {
  const parts = client.split("_");
  return parts.map((_, i) => parts.slice(0, i + 1).join("_")).filter(Boolean);
}

// Turns on the company setting for a new source, via Supabase's REST API.
// Matches the company on `table_identifier` (the dataset's client part, or for
// Amazon the longest table_identifier it starts with) and only writes when
// exactly one company matches and the column is still unset.
function createCompanyFlagSetter({ url, serviceRoleKey, fetchImpl = fetch, timeoutMs = 5000 }) {
  const headers = {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Content-Type": "application/json",
  };

  // Each call is capped: this runs inside the Pub/Sub push request, and a hang
  // past the ack deadline would make Pub/Sub redeliver and re-dispatch the scaffold.
  async function request(path, init = {}) {
    const resp = await fetchImpl(`${url}/rest/v1/${path}`, {
      ...init,
      headers: { ...headers, ...init.headers },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!resp.ok) throw new Error(`Supabase ${init.method || "GET"} failed: ${resp.status} ${await resp.text()}`);
    return resp.json();
  }

  return async function setCompanyFlag({ parent, client }) {
    const flag = companyFlagFor(parent);
    if (!flag) return { status: "no_flag" };
    const { column, value, unset } = flag;

    const match = flag.matchClientPrefix
      ? `in.(${clientPrefixes(client).map(encodeURIComponent).join(",")})`
      : `eq.${encodeURIComponent(client)}`;
    const rows = await request(`companies?table_identifier=${match}&select=id,name,table_identifier,${column}`);
    // Longest match wins, so acme_us_airbyte picks acme_us over acme.
    const idOf = (r) => String(r.table_identifier ?? "");
    const tableIdentifier = rows.reduce((best, r) => (idOf(r).length > best.length ? idOf(r) : best), "");
    const companies = rows.filter((r) => idOf(r) === tableIdentifier);
    if (companies.length !== 1) return { status: "company_not_unique", column, matches: companies.length };

    const company = companies[0];
    const current = company[column];
    if (current !== null && current !== unset) return { status: "already_set", column, company: company.name, current };

    // The unset filter is repeated in the update so a value someone set in the
    // meantime is never overwritten.
    const updated = await request(
      `companies?id=eq.${company.id}&or=(${column}.is.null,${column}.eq.${unset})`,
      { method: "PATCH", headers: { Prefer: "return=representation" }, body: JSON.stringify({ [column]: value }) }
    );
    if (!updated.length) return { status: "already_set", column, company: company.name };
    return { status: "set", column, company: company.name, from: current, to: value };
  };
}

// --- 4️⃣ GitHub dispatch helper ---
async function dispatchToGitHub(payload) {
  const resp = await fetch(
    `https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/dispatches`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GITHUB_TOKEN}`,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        event_type: "bq_dataset_created",
        client_payload: payload,
      }),
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`GitHub dispatch failed: ${resp.status} ${text}`);
  }
}

// --- 5️⃣ Express routes ---
function createApp({
  bigquery,
  dispatch,
  setCompanyFlag = null,
  now = Date.now,
  maxWaitHours = MAX_WAIT_HOURS,
}) {
  const app = express();
  app.use(express.json());

  app.get("/healthz", (_, res) => res.status(200).send("ok"));

  app.post("/", async (req, res) => {
    try {
      const msg = req.body?.message;
      const data = msg?.data
        ? JSON.parse(Buffer.from(msg.data, "base64").toString())
        : {};
      const entry = data?.protoPayload || {};
      const resourceName = entry.resourceName || "";
      const datasetId = resourceName.split("/").pop() || "";

      console.log("NEW_DATASET_EVENT", {
        datasetId,
        resourceName,
        who: entry?.authenticationInfo?.principalEmail,
        locations: entry?.resourceLocation?.currentLocations,
      });

      const plan = buildPlan(datasetId, PROJECT);
      const parts = splitDataset(datasetId);
      const flag = parts && companyFlagFor(parts.parent);
      // amazon* datasets without a template still turn on the company setting.
      if (!plan && !flag) {
        console.log("No matching template plan for:", datasetId);
        return res.status(204).end();
      }

      // The log entry's timestamp is when the dataset was created, and it stays
      // the same on every Pub/Sub redelivery.
      const createdAt = Date.parse(data?.timestamp || msg?.publishTime || "") || now();
      const waitedHours = Math.round(((now() - createdAt) / 3600000) * 10) / 10;

      // A plan waits for its template's tables; the Amazon setting only for the
      // dataset to hold any table.
      let missing = [];
      let landed = false;
      let tablesChecked = false;
      try {
        if (plan) missing = await findMissingTables(bigquery, plan.tables);
        if (flag?.matchClientPrefix) landed = (await listTables(bigquery, datasetId)).size > 0;
        tablesChecked = true;
      } catch (e) {
        // Can't check (e.g. no BigQuery permission): dispatch now, as before.
        console.error("TABLE_CHECK_FAILED", { datasetId, error: e.message });
      }

      const waiting = plan ? missing.length > 0 : tablesChecked && !landed;
      if (waiting && waitedHours < maxWaitHours) {
        console.log("WAITING_FOR_TABLES", { datasetId, missing, waitedHours });
        // Any non-2xx response makes Pub/Sub redeliver the message later.
        return res.status(429).end();
      }

      if (plan) {
        const nothingLanded = tablesChecked && plan.tables.length > 0 && missing.length === plan.tables.length;
        if (plan.requireSomeTables && nothingLanded) {
          console.warn("NOT_DISPATCHING", { datasetId, reason: "none of the template's tables landed", missing, waitedHours });
        } else {
          if (missing.length) {
            console.warn("DISPATCHING_WITH_MISSING_TABLES", { datasetId, missing, waitedHours });
          }
          await dispatch({
            datasetId,
            files: plan.files,
            vars: plan.vars,
            missingTables: missing,
          });
          console.log("✅ Dispatched to GitHub for", datasetId);
        }
      }

      // Only once the first sync has landed: a dataset still missing tables
      // after the max wait points at a broken connector, not live data, and a
      // failed table check proves nothing either way.
      // Errors are logged, not returned, because a retry would re-dispatch.
      if (setCompanyFlag && flag && !tablesChecked) {
        console.log("COMPANY_FLAG_SKIPPED", { datasetId, status: "tables_unchecked" });
      } else if (setCompanyFlag && flag && (flag.matchClientPrefix ? landed : !missing.length)) {
        try {
          const result = await setCompanyFlag({ parent: parts.parent, client: parts.client });
          console.log(result.status === "set" ? "COMPANY_FLAG_SET" : "COMPANY_FLAG_SKIPPED", { datasetId, ...result });
        } catch (e) {
          console.error("COMPANY_FLAG_FAILED", { datasetId, error: e.message });
        }
      }

      res.status(204).end();
    } catch (e) {
      console.error("Handler error:", e);
      res.status(500).end();
    }
  });

  return app;
}

if (require.main === module) {
  const setCompanyFlag =
    SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
      ? createCompanyFlagSetter({ url: SUPABASE_URL, serviceRoleKey: SUPABASE_SERVICE_ROLE_KEY })
      : null;
  if (!setCompanyFlag) console.warn("COMPANY_FLAGS_DISABLED: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set");
  const app = createApp({
    bigquery: new BigQuery({ projectId: PROJECT }),
    dispatch: dispatchToGitHub,
    setCompanyFlag,
  });
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`listener on :${PORT}`));
}

module.exports = { CATALOG, COMPANY_FLAGS, buildPlan, findMissingTables, createCompanyFlagSetter, createApp };
