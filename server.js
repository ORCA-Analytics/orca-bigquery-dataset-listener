// server.js
const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

// --- Environment variables ---
// Stored in Secret Manager; trimmed because a pasted trailing newline makes
// every dispatch fail with an invalid Authorization header.
const GITHUB_TOKEN = (process.env.GITHUB_TOKEN || "").trim();
const GH_OWNER = "ORCA-Analytics";
const GH_REPO = "orca-dbt";
const PROJECT = "orcaanalytics";
// How long to keep waiting for a new dataset's tables before dispatching anyway.
const MAX_WAIT_HOURS = Number(process.env.MAX_WAIT_HOURS || 72);
// Supabase project with the `companies` table the ORCA app reads its settings from.
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // stored in Secret Manager
// The ORCA app's `source-dataset-created` edge function and the shared secret
// it checks (not the service-role key). Trimmed like GITHUB_TOKEN: a trailing
// newline is an invalid header value.
const SUPABASE_SOURCE_DATASET_URL = (process.env.SUPABASE_SOURCE_DATASET_URL || "").trim();
const SOURCE_DATASET_SECRET = (process.env.SOURCE_DATASET_SECRET || "").trim(); // stored in Secret Manager

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
//
// `optional` entries read a table that most connections don't sync. They don't
// hold up the dispatch: the file is scaffolded only when all of its tables
// already exist at dispatch time. With `waitHours`, a missing optional table
// holds the dispatch until the dataset is that old (capped by the max wait),
// for tables a separate connection is expected to land but may never do.
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
      // New clients get custom_searchterms and custom_pmax_searchterms from a
      // separate `_searchterms` connection; older connections never sync them,
      // and one with a failed OAuth never lands them, so the other models
      // don't wait past a day.
      {
        rel: "searchterms",
        tpl: "google_ads_searchterms",
        tables: ["google_ads__{client}.custom_searchterms", "google_ads__{client}.custom_pmax_searchterms"],
        optional: true,
        waitHours: 24,
      },
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
      {
        rel: "shopify_pixel/lasttouch_daily",
        tpl: "shopify_lasttouch_daily",
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
  const optional = [];
  for (const e of cfg.entries) {
    const template = tPath(baseDir, e.rel, e.tpl);
    const outName = e.out ? e.out(e.tpl, client) : `${e.tpl}__${client}.sql`;
    const file = { template, path: oPath(baseDir, e.rel, outName) };
    const entryTables = (e.tables || []).map((t) => t.replace("{client}", client));
    if (e.optional) {
      optional.push({ file, tables: entryTables, waitHours: e.waitHours || 0 });
      continue;
    }
    files.push(file);
    for (const t of entryTables) tables.add(t);
  }
  return {
    files,
    tables: [...tables],
    optional,
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

// The `companies.table_identifier` filter for a dataset's client: the client
// itself, or with matchClientPrefix any table_identifier the client starts with.
function tableIdentifierFilter(client, matchClientPrefix) {
  return matchClientPrefix
    ? `in.(${clientPrefixes(client).map(encodeURIComponent).join(",")})`
    : `eq.${encodeURIComponent(client)}`;
}

// The rows with the longest table_identifier, so acme_us_airbyte picks acme_us over acme.
function longestMatches(rows) {
  const idOf = (r) => String(r.table_identifier ?? "");
  const longest = rows.reduce((best, r) => (idOf(r).length > best.length ? idOf(r) : best), "");
  return rows.filter((r) => idOf(r) === longest);
}

// A call to Supabase's REST API with the service-role key. Each call is capped:
// this runs inside the Pub/Sub push request, and a hang past the ack deadline
// would make Pub/Sub redeliver and re-dispatch the scaffold.
function supabaseRequester({ url, serviceRoleKey, fetchImpl, timeoutMs }) {
  const headers = {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Content-Type": "application/json",
  };
  return async function request(path, init = {}) {
    const resp = await fetchImpl(`${url}/rest/v1/${path}`, {
      ...init,
      headers: { ...headers, ...init.headers },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!resp.ok) {
      const error = new Error(`Supabase ${init.method || "GET"} failed: ${resp.status} ${await resp.text()}`);
      throw Object.assign(error, { status: resp.status });
    }
    return resp.json();
  };
}

// Turns on the company setting for a new source, via Supabase's REST API.
// Matches the company on `table_identifier` (the dataset's client part, or for
// Amazon the longest table_identifier it starts with) and only writes when
// exactly one company matches and the column is still unset.
function createCompanyFlagSetter({ url, serviceRoleKey, fetchImpl = fetch, timeoutMs = 5000 }) {
  const request = supabaseRequester({ url, serviceRoleKey, fetchImpl, timeoutMs });

  return async function setCompanyFlag({ parent, client }) {
    const flag = companyFlagFor(parent);
    if (!flag) return { status: "no_flag" };
    const { column, value, unset } = flag;

    const match = tableIdentifierFilter(client, flag.matchClientPrefix);
    const rows = await request(`companies?table_identifier=${match}&select=id,company_name,table_identifier,${column}`);
    const companies = longestMatches(rows);
    if (companies.length !== 1) return { status: "company_not_unique", column, matches: companies.length };

    const company = companies[0];
    const current = company[column];
    if (current !== null && current !== unset) return { status: "already_set", column, company: company.company_name, current };

    // The unset filter is repeated in the update so a value someone set in the
    // meantime is never overwritten.
    const updated = await request(
      `companies?id=eq.${company.id}&or=(${column}.is.null,${column}.eq.${unset})`,
      { method: "PATCH", headers: { Prefer: "return=representation" }, body: JSON.stringify({ [column]: value }) }
    );
    if (!updated.length) return { status: "already_set", column, company: company.company_name };
    return { status: "set", column, company: company.company_name, from: current, to: value };
  };
}

// --- 4️⃣ Pacing model and scaffold settings ---
// orca-dbt's pacing template reads Shopify plus these spend and session
// sources. A scaffold for any of them also sends models/pacing/pacing__<client>.sql,
// once shopify__<client> has landed, so the model is written when the first
// source lands and re-rendered with each new one. It goes in the payload's
// `generated` list, which orca-dbt writes only while the file is missing or
// still carries its generated marker, so hand-built models are never touched.
// Keep in sync with PACING_SOURCE_MODELS in orca-dbt .github/scripts/scaffold.js.
const PACING_SOURCES = [
  "shopify",
  "facebook_ads",
  "google_ads",
  "google_analytics_4",
  "tiktok_ads",
  "applovin",
  "bing_ads",
  "liveintent",
  "pinterest_ads",
  "shareasale",
  "snapchat_ads",
  "twitter_ads",
];

// companies.revenue_measured_by -> the templates' revenue line (the pacing
// model's and shopify_newreturn's pacing_revenue). Unset means Gross Revenue,
// the ORCA app's default. A custom definition needs hand-built models, so the
// scaffolds keep Gross Revenue and no pacing file is sent.
const REVENUE_BASES = {
  "Gross Revenue": "gross",
  "Gross - Discounts": "gross_discounts",
  "Gross - Discounts + Shipping": "gross_discounts_shipping",
  "Gross + Shipping": "gross_shipping",
  "Net (Gross - Discounts - Returns)": "net",
  "Net + Shipping (Gross - Discounts - Returns + Shipping)": "net_shipping",
  "Total (Gross - Discounts - Returns + Shipping + Tax)": "total",
  "Gross - Discounts + Shipping + Tax": "gross_discounts_shipping_tax",
};

// The dbt Cloud schedules (`daily3_<tag>`) and a zone each one runs for.
const TIMEZONE_TAGS = {
  est: "America/New_York",
  cst: "America/Chicago",
  mst: "America/Denver",
  pst: "America/Los_Angeles",
  gmt: "Europe/London",
  aest: "Australia/Sydney",
};
const MID_JANUARY = new Date(Date.UTC(2026, 0, 15, 12));

function utcOffsetMinutes(timeZone, date) {
  const name = new Intl.DateTimeFormat("en-US", { timeZone, timeZoneName: "longOffset" })
    .formatToParts(date)
    .find((p) => p.type === "timeZoneName").value; // "GMT-05:00", or "GMT" for UTC
  const m = /GMT([+-])(\d{2}):(\d{2})/.exec(name);
  return m ? (m[1] === "-" ? -1 : 1) * (Number(m[2]) * 60 + Number(m[3])) : 0;
}

// companies.timezone -> the tag whose zone has the nearest UTC offset in
// January (America/Phoenix -> mst, Europe/Berlin -> gmt). Unset or not a zone
// means est, as the scaffolds use.
function timezoneTag(timezone) {
  if (!timezone) return "est"; // Intl would use the server's own zone
  let offset;
  try {
    offset = utcOffsetMinutes(timezone, MID_JANUARY);
  } catch {
    return "est"; // RangeError: not a zone
  }
  let best = "est";
  let bestDiff = Infinity;
  for (const [tag, zone] of Object.entries(TIMEZONE_TAGS)) {
    const diff = Math.abs(utcOffsetMinutes(zone, MID_JANUARY) - offset);
    if (diff < bestDiff) [best, bestDiff] = [tag, diff];
  }
  return best;
}

// Reads the settings a brand's scaffolds render with, for the one company whose
// table_identifier is the client (with matchClientPrefix, the longest one the
// client starts with, as for the Amazon company setting).
function createCompanySettingsReader({ url, serviceRoleKey, fetchImpl = fetch, timeoutMs = 5000 }) {
  const request = supabaseRequester({ url, serviceRoleKey, fetchImpl, timeoutMs });
  return async function readCompanySettings(client, { matchClientPrefix = false } = {}) {
    const rows = await request(
      `companies?table_identifier=${tableIdentifierFilter(client, matchClientPrefix)}&select=table_identifier,timezone,revenue_measured_by`
    );
    const companies = longestMatches(rows);
    if (companies.length !== 1) return { status: "company_not_unique", matches: companies.length };
    return { status: "ok", timezone: companies[0].timezone, revenueMeasuredBy: companies[0].revenue_measured_by };
  };
}

// The shape the ORCA app accepts for an IANA zone before it reaches BigQuery
// (orcaanalytics-webapp utils/bigquery/sanitizeTimezone.js). The zone is written
// into the scaffolds' SQL, so nothing else gets through.
const IANA_ZONE = /^[A-Za-z]+(?:[/_][A-Za-z0-9_+\-]+)*$/;

function isTimeZone(timezone) {
  if (typeof timezone !== "string" || !IANA_ZONE.test(timezone)) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone });
    return true;
  } catch {
    return false; // RangeError: not a zone
  }
}

// What every scaffold renders with before ENG-306, and still does when a
// setting is unknown: New York, its est schedules and Gross Revenue.
const SCAFFOLD_DEFAULTS = { timezone: "America/New_York", timezone_tag: "est", revenue: "gross" };

// A company's settings (a readCompanySettings result) -> the dispatch vars
// every scaffold renders with: `timezone` (the IANA zone the SQL buckets dates
// in), `timezone_tag` (its dbt Cloud schedule suffix) and `revenue` (a
// REVENUE_BASES key). `defaulted` holds each setting that couldn't be used.
function scaffoldSettings(settings) {
  const vars = { ...SCAFFOLD_DEFAULTS };
  if (settings.status !== "ok") return { vars, defaulted: { company: settings.status } };

  const defaulted = {};
  if (isTimeZone(settings.timezone)) {
    Object.assign(vars, { timezone: settings.timezone, timezone_tag: timezoneTag(settings.timezone) });
  } else {
    defaulted.timezone = settings.timezone ?? null;
  }
  const setting = (settings.revenueMeasuredBy || "").trim();
  if (REVENUE_BASES[setting]) vars.revenue = REVENUE_BASES[setting];
  else if (setting) defaulted.revenue_measured_by = setting;
  return { vars, defaulted };
}

// The pacing file to send with a scaffold of `parent`, or `skip` with the
// reason; null when `parent` isn't a pacing source. Landed sources are the
// dispatched one plus each other source whose dataset holds a table (orca-dbt
// leaves out any whose model isn't on main yet). `settings` is the company's
// readCompanySettings result. Throws on a BigQuery error.
async function planPacing({ bigquery, settings, parent, client }) {
  if (!PACING_SOURCES.includes(parent)) return null;
  if (settings.status === "disabled") return { skip: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set" };

  const holdsTables = await Promise.all(
    PACING_SOURCES.map(async (s) => s === parent || (await listTables(bigquery, `${s}__${client}`)).size > 0)
  );
  const sources = PACING_SOURCES.filter((_, i) => holdsTables[i]);
  if (!sources.includes("shopify")) return { skip: `shopify__${client} hasn't landed` };

  if (settings.status !== "ok") {
    const { status, ...detail } = settings;
    return { skip: status, ...detail };
  }
  const { vars, defaulted } = scaffoldSettings(settings);
  if (defaulted.revenue_measured_by) {
    return { skip: "revenue_measured_by needs a hand-built model", revenueMeasuredBy: defaulted.revenue_measured_by };
  }

  return {
    file: {
      template: "templates/pacing/pacing.sql",
      path: `models/pacing/pacing__${client}.sql`,
      vars: { sources, timezone_tag: vars.timezone_tag, revenue: vars.revenue },
    },
  };
}

// --- 5️⃣ GitHub dispatch helper ---
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

// --- 6️⃣ Source dataset ping ---
// Tells the ORCA app a brand's source dataset landed, so its onboarding
// checklist can tick that source as connected. Capped like the company flag
// calls: a hang past the Pub/Sub ack deadline would redeliver and re-dispatch.
function createSourceDatasetNotifier({ url, secret, fetchImpl = fetch, timeoutMs = 5000 }) {
  return async function notifySourceDataset(datasetId) {
    const resp = await fetchImpl(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-orca-source-secret": secret },
      body: JSON.stringify({ dataset_id: datasetId }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!resp.ok) throw new Error(`source-dataset-created failed: ${resp.status} ${await resp.text()}`);
    return resp.status;
  };
}

// --- 7️⃣ Express routes ---
function createApp({
  bigquery,
  dispatch,
  setCompanyFlag = null,
  notifySourceDataset = null,
  readCompanySettings = null,
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
      let absent = new Set(); // optional entries' tables included
      let landed = false;
      let tablesChecked = false;
      try {
        if (plan) {
          absent = new Set(
            await findMissingTables(bigquery, [...plan.tables, ...plan.optional.flatMap((o) => o.tables)])
          );
          missing = plan.tables.filter((t) => absent.has(t));
        }
        if (flag?.matchClientPrefix) landed = (await listTables(bigquery, datasetId)).size > 0;
        tablesChecked = true;
      } catch (e) {
        // Can't check (e.g. no BigQuery permission): dispatch now, as before.
        console.error("TABLE_CHECK_FAILED", { datasetId, error: e.message });
      }

      const awaitedOptional = plan
        ? plan.optional
            .filter((o) => waitedHours < o.waitHours)
            .flatMap((o) => o.tables.filter((t) => absent.has(t)))
        : [];
      const waiting = plan ? missing.length > 0 || awaitedOptional.length > 0 : tablesChecked && !landed;
      if (waiting && waitedHours < maxWaitHours) {
        console.log("WAITING_FOR_TABLES", { datasetId, missing: [...missing, ...awaitedOptional], waitedHours });
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
          const files = [...plan.files];
          for (const o of plan.optional) {
            const notThere = o.tables.filter((t) => absent.has(t));
            if (notThere.length) console.log("SKIPPING_OPTIONAL_MODEL", { datasetId, path: o.file.path, missing: notThere });
            else files.push(o.file);
          }

          // Nothing is dispatched yet, so a redelivery after an error is safe,
          // but a 4xx (bad key, no permission) won't clear by itself.
          const retry = (e) => {
            const status = e.status ?? e.code;
            return !(status >= 400 && status < 500) && waitedHours < maxWaitHours;
          };

          // Every scaffold renders in the brand's timezone and revenue basis.
          let settings = { status: "disabled" };
          if (readCompanySettings) {
            try {
              settings = await readCompanySettings(parts.client, { matchClientPrefix: Boolean(flag?.matchClientPrefix) });
            } catch (e) {
              if (retry(e)) {
                console.error("COMPANY_SETTINGS_FAILED", { datasetId, error: e.message, cause: e.cause?.message, waitedHours });
                return res.status(503).end();
              }
              settings = { status: "check_failed", error: e.message };
            }
          }
          const { vars: settingVars, defaulted } = scaffoldSettings(settings);

          // A failed table check says nothing about which sources landed.
          let pacing = null;
          if (!tablesChecked) {
            if (PACING_SOURCES.includes(parts.parent)) pacing = { skip: "tables_unchecked" };
          } else {
            try {
              pacing = await planPacing({ bigquery, settings, ...parts });
            } catch (e) {
              if (retry(e)) {
                console.error("PACING_CHECK_FAILED", { datasetId, error: e.message, cause: e.cause?.message, waitedHours });
                return res.status(503).end();
              }
              pacing = { skip: "check_failed", error: e.message };
            }
          }
          if (pacing?.skip) {
            const { skip, ...detail } = pacing;
            console.log("PACING_SKIPPED", { datasetId, reason: skip, ...detail });
          }

          await dispatch({
            datasetId,
            files,
            ...(pacing?.file && { generated: [pacing.file] }),
            vars: { ...plan.vars, ...settingVars },
            missingTables: missing,
          });
          console.log("✅ Dispatched to GitHub for", datasetId);
          console.log("SCAFFOLD_SETTINGS", { datasetId, ...settingVars, ...(Object.keys(defaulted).length && { defaulted }) });
          if (pacing?.file) console.log("PACING_INCLUDED", { datasetId, path: pacing.file.path, ...pacing.file.vars });

          // Errors are logged, not returned, because a retry would re-dispatch.
          if (!notifySourceDataset) {
            console.log("SOURCE_DATASET_PING_SKIPPED", { datasetId, reason: "SUPABASE_SOURCE_DATASET_URL or SOURCE_DATASET_SECRET not set" });
          } else {
            try {
              const status = await notifySourceDataset(datasetId);
              console.log("SOURCE_DATASET_PINGED", { datasetId, status });
            } catch (e) {
              // fetch's own message is just "fetch failed"; the cause says why.
              console.error("SOURCE_DATASET_PING_FAILED", { datasetId, error: e.message, cause: e.cause?.message });
            }
          }
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
  const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY ? { url: SUPABASE_URL, serviceRoleKey: SUPABASE_SERVICE_ROLE_KEY } : null;
  const setCompanyFlag = supabase ? createCompanyFlagSetter(supabase) : null;
  const readCompanySettings = supabase ? createCompanySettingsReader(supabase) : null;
  if (!supabase) {
    console.warn("COMPANY_FLAGS_DISABLED: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set (no pacing models, and scaffolds keep New York and Gross Revenue)");
  }
  const notifySourceDataset =
    SUPABASE_SOURCE_DATASET_URL && SOURCE_DATASET_SECRET
      ? createSourceDatasetNotifier({ url: SUPABASE_SOURCE_DATASET_URL, secret: SOURCE_DATASET_SECRET })
      : null;
  const app = createApp({
    bigquery: new BigQuery({ projectId: PROJECT }),
    dispatch: dispatchToGitHub,
    setCompanyFlag,
    notifySourceDataset,
    readCompanySettings,
  });
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`listener on :${PORT}`));
}

module.exports = {
  CATALOG,
  COMPANY_FLAGS,
  buildPlan,
  findMissingTables,
  createCompanyFlagSetter,
  createCompanySettingsReader,
  createSourceDatasetNotifier,
  createApp,
  scaffoldSettings,
  timezoneTag,
};
