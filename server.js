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
  amazon: {
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
      {
        rel: "amazon_sellercentral",
        tpl: "amazon_sellercentral",
        tables: ["amazon_sellercentral__{client}.sales_and_traffic_business_report_daily"],
      },
      {
        rel: "amazon_sellercentral_cohorts",
        tpl: "amazon_sellercentral_cohorts",
        tables: ["amazon_sellercentral__{client}.orders"],
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
  return { files, tables: [...tables], vars: { datasetId, parent, client, project: projectId } };
}

// --- 2️⃣ Table readiness ---
// Returns the "dataset.table" entries that don't exist yet. A dataset that
// doesn't exist yet counts as all of its tables missing.
async function findMissingTables(bigquery, tables) {
  const byDataset = new Map();
  for (const t of tables) {
    const [dataset, table] = t.split(".");
    if (!byDataset.has(dataset)) byDataset.set(dataset, []);
    byDataset.get(dataset).push(table);
  }

  const missing = [];
  for (const [dataset, wanted] of byDataset) {
    let existing = new Set();
    try {
      const [found] = await bigquery.dataset(dataset).getTables();
      existing = new Set(found.map((t) => t.id));
    } catch (e) {
      if (e.code !== 404) throw e;
    }
    for (const table of wanted) {
      if (!existing.has(table)) missing.push(`${dataset}.${table}`);
    }
  }
  return missing;
}

// --- 3️⃣ GitHub dispatch helper ---
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

// --- 4️⃣ Express routes ---
function createApp({ bigquery, dispatch, now = Date.now, maxWaitHours = MAX_WAIT_HOURS }) {
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
      if (!plan) {
        console.log("No matching template plan for:", datasetId);
        return res.status(204).end();
      }

      // The log entry's timestamp is when the dataset was created, and it stays
      // the same on every Pub/Sub redelivery.
      const createdAt = Date.parse(data?.timestamp || msg?.publishTime || "") || now();
      const waitedHours = Math.round(((now() - createdAt) / 3600000) * 10) / 10;

      let missing = [];
      try {
        missing = await findMissingTables(bigquery, plan.tables);
      } catch (e) {
        // Can't check (e.g. no BigQuery permission): dispatch now, as before.
        console.error("TABLE_CHECK_FAILED", { datasetId, error: e.message });
      }

      if (missing.length && waitedHours < maxWaitHours) {
        console.log("WAITING_FOR_TABLES", { datasetId, missing, waitedHours });
        // Any non-2xx response makes Pub/Sub redeliver the message later.
        return res.status(429).end();
      }
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
      res.status(204).end();
    } catch (e) {
      console.error("Handler error:", e);
      res.status(500).end();
    }
  });

  return app;
}

if (require.main === module) {
  const app = createApp({
    bigquery: new BigQuery({ projectId: PROJECT }),
    dispatch: dispatchToGitHub,
  });
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`listener on :${PORT}`));
}

module.exports = { CATALOG, buildPlan, findMissingTables, createApp };
