const express = require("express");
const app = express();
app.use(express.json());

app.get("/healthz", (_, res) => res.status(200).send("ok"));

app.post("/", (req, res) => {
  try {
    const msg = req.body?.message;
    const data = msg?.data ? JSON.parse(Buffer.from(msg.data, "base64").toString()) : {};
    const entry = data?.protoPayload || {};
    const resourceName = entry.resourceName || ""; // "projects/123/datasets/facebook_ads__clientone"
    const datasetId = resourceName.split("/").pop() || "";

    console.log("NEW_DATASET_EVENT", {
      datasetId,
      resourceName,
      who: entry?.authenticationInfo?.principalEmail,
      locations: entry?.resourceLocation?.currentLocations
    });

    // TODO: call GitHub repository_dispatch here when you’re ready.
    // await dispatchToGitHub(datasetId)

    res.status(204).end();
  } catch (e) {
    console.error("Handler error:", e);
    res.status(500).end();
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`listener on :${PORT}`));
