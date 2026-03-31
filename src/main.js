import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const createdIds = [];

  let body = req.bodyJson || {};
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (e) {
      return res.json({ success: false, message: "Invalid JSON" }, 400);
    }
  }

  const { databaseId, collectionId, documents } = body;

  try {
    if (!databaseId || !collectionId || !Array.isArray(documents)) {
      throw new Error("Missing databaseId, collectionId, or documents array.");
    }

    const BATCH_SIZE = 15;
    log(`Processing ${documents.length} documents...`);

    for (let i = 0; i < documents.length; i += BATCH_SIZE) {
      const chunk = documents.slice(i, i + BATCH_SIZE);

      const batchResults = await Promise.all(
        chunk.map(data =>
          databases.createDocument(databaseId, collectionId, ID.unique(), data)
        )
      );

      batchResults.forEach(doc => createdIds.push(doc.$id));
    }

    return res.json({ success: true, count: createdIds.length }, 200);

  } catch (err) {
    error("Batch failed: " + err.message);

    if (createdIds.length > 0) {
      log(`Rolling back ${createdIds.length} documents...`);
      for (const id of createdIds) {
        try {
          await databases.deleteDocument(databaseId, collectionId, id);
        } catch (rbErr) {
          error(`Failed to delete ID ${id} during rollback.`);
        }
      }
    }

    return res.json({
      success: false,
      message: err.message,
      failedAtCount: createdIds.length
    }, 500);
  }
};