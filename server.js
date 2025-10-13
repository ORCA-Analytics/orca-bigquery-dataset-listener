// server.js
const express = require("express");
const fetch = require("node-fetch");
const app = express();
app.use(express.json());

// --- Environment variables ---
const GITHUB_TOKEN = process.env.GITHUB_TOKEN; // stored in Secret Manager
const GH_OWNER = "ORCA-Analytics";
const GH_REPO = "orca-dbt";
const PROJECT = "orcaanalytics";

// --- 1️⃣ buildPlan logic ---
function splitDataset(datasetId) {
  const m = datasetId.match(/^([a-z0-9_]+)__([a-z0-9_]+)$/);
  return m ? { parent: m[1], client: m[2] } : null;
}

function tPath(parent, rel, tpl) {
  return `templates/${parent}/${rel ? rel + "/" : ""}${tpl}.sql`;
}
function oPath(parent, rel, name) {
  return `models/${parent}/${rel ? rel + "/" : ""}${name}`;
}

const CATALOG = {
  facebook_ads: {
    entries: [
      { rel: "campaigns", tpl: "facebook_ads" },
      { rel: "creative", tpl: "facebook_ads_creative" },
    ],
  },
  google_ads: {
    entries: [
      { rel: "campaigns", tpl: "google_ads" },
      { rel: "keywords", tpl: "google_ads_keywords" },
      { rel: "products", tpl: "google_ads_products" },
    ],
  },
  google_analytics_4: {
    entries: [
      {
        rel: "sessionscvr",
        tpl: "google_analytics_4_sessionscvr",
        out: (_tpl, client) =>
          `google_analytics_4__${client}_sessionscvr.sql`,
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
      { rel: "orders", tpl: "shopify_orders" },
      { rel: "product_firstbasket", tpl: "shopify_product_firstbasket" },
      {
        rel: "product_firstsecondorder",
        tpl: "shopify_product_firstsecondorder",
      },
      { rel: "product_ltr_journey", tpl: "shopify_product_ltrjourney" },
      { rel: "product_ltr", tpl: "shopify_product_ltr" },
      { rel: "product", tpl: "shopify_products" },
      { rel: "refunds", tpl: "shopify_refunds" },
      { rel: "shopify_pixel/base_customervisits", tpl: "shopify_customervisits" },
      { rel: "shopify_pixel/daily_channel", tpl: "shopify_pixel_dailychannel" },
      { rel: "shopify_pixel/extrapolated", tpl: "shopify_pixel_extrapolated" },
      { rel: "shopify_pixel/modeled", tpl: "shopify_pixel_modeled" },
      { rel: "shopify_pixel/percent_of_orders", tpl: "shopify_pixel_percentoforders" },
    ],
  },
  amazon: { 
    entries: [
      { rel: "amazon_ads", tpl: "amazon_ads" },
      { rel: "amazon_sellercentral", tpl: "amazon_sellercentral" }
    ]},
  applovin: { entries: [{ rel: "", tpl: "applovin" }] },
  bing_ads: { entries: [{ rel: "", tpl: "bing_ads" }] },
  pinterest_ads: { entries: [{ rel: "", tpl: "pinterest_ads" }] },
  snapchat_ads: { entries: [{ rel: "", tpl: "snapchat_ads" }] },
  tiktok_ads: {
    entries: [
      { rel: "campaigns", tpl: "tiktok_ads" },
      { rel: "creative", tpl: "tiktok_ads_creative" },
    ],
  },
  hdyhau_fairing: { entries: [{ rel: "", tpl: "fairing_hdyhau" }] },
  hdyhau_knocommerce: { 
    entries: [
      { rel: "all_responses", tpl: "knocommerce_allresponses" },
      { rel: "hdyhau", tpl: "knocommerce_hdyhau" }
    ] 
  },
  klaviyo: { 
    entries: [
      { rel: "leadgen_sms", tpl: "klaviyo_leadgen_sms" },
      { rel: "leadgen", tpl: "klaviyo_leadgen" }
    ] 
  },
  liveintent: { entries: [{ rel: "", tpl: "liveintent" }] },
  pacing: { entries: [{ rel: "", tpl: "pacing" }] },
  rakuten: { entries: [{ rel: "", tpl: "rakuten" }] },
  twitter_ads: { entries: [{ rel: "", tpl: "twitter_ads" }] },
};

function buildPlan(datasetId, projectId) {
  const parts = splitDataset(datasetId);
  if (!parts) return null;
  const { parent, client } = parts;
  const cfg = CATALOG[parent];
  if (!cfg) return null;

  const files = [];
  for (const e of cfg.entries) {
    const template = tPath(parent, e.rel, e.tpl);
    const outName = e.out ? e.out(e.tpl, client) : `${e.tpl}__${client}.sql`;
    files.push({ template, path: oPath(parent, e.rel, outName) });
  }
  return { files, vars: { datasetId, parent, client, project: projectId } };
}

// --- 2️⃣ GitHub dispatch helper ---
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

// --- 3️⃣ Express routes ---
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

    await dispatchToGitHub({
      datasetId,
      files: plan.files,
      vars: plan.vars,
    });

    console.log("✅ Dispatched to GitHub for", datasetId);
    res.status(204).end();
  } catch (e) {
    console.error("Handler error:", e);
    res.status(500).end();
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`listener on :${PORT}`));
