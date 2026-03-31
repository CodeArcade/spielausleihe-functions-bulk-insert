import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const createdIds = [];
  const BATCH_SIZE = 10;
  const DELAY_MS = 150;

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  let body = req.bodyJson;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); }
    catch (e) { return res.json({ success: false, message: "Invalid JSON" }, 400); }
  }

  const { databaseId, collectionId, documents } = body || {};

  try {
    if (!databaseId || !collectionId || !Array.isArray(documents)) {
      throw new Error("Missing required fields.");
    }

    log(`Processing ${documents.length} documents in batches of ${BATCH_SIZE}...`);

    for (let i = 0; i < documents.length; i += BATCH_SIZE) {
      const batch = documents.slice(i, i + BATCH_SIZE);

      // Execute batch concurrently
      await Promise.all(batch.map(async (data) => {
        const doc = await databases.createDocument(databaseId, collectionId, ID.unique(), data);
        createdIds.push(doc.$id);
      }));

      // Small delay to let the network/API breathe
      if (i + BATCH_SIZE < documents.length) await sleep(DELAY_MS);
    }

    return res.json({ success: true, count: createdIds.length }, 200);

  } catch (err) {
    error("Batch execution failed: " + err.message);

    if (createdIds.length > 0) {
      log(`Rolling back ${createdIds.length} documents...`);

      // Batch the rollback as well to avoid secondary fetch failures
      for (let i = 0; i < createdIds.length; i += BATCH_SIZE) {
        const batchIds = createdIds.slice(i, i + BATCH_SIZE);
        try {
          await Promise.all(batchIds.map(id => databases.deleteDocument(databaseId, collectionId, id)));
          await sleep(50); // Shorter sleep for cleanup
        } catch (rbErr) {
          error(`Failed to delete IDs: ${batchIds.join(', ')}`);
        }
      }
    }

    return res.json({
      success: false,
      message: err.message,
      rolledBack: createdIds.length > 0,
      failedAtCount: createdIds.length
    }, 500);
  }
};