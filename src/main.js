import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const createdIds = [];

  // Helper to handle body parsing across different trigger types
  let body = req.bodyJson;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (e) {
      return res.json({ success: false, message: "Invalid JSON body string" }, 400);
    }
  }

  const { databaseId, collectionId, documents } = body || {};

  try {
    if (!databaseId || !collectionId || !Array.isArray(documents)) {
      throw new Error("Missing databaseId, collectionId, or documents array.");
    }

    log(`Processing ${documents.length} documents...`);

    for (const data of documents) {
      try {
        const doc = await databases.createDocument(
          databaseId,
          collectionId,
          ID.unique(),
          data
        );
        createdIds.push(doc.$id);
      } catch (insertError) {
        // We throw a standard error but attach the Appwrite error message
        throw new Error(insertError.message || "Unknown database error");
      }
    }

    return res.json({ success: true, count: createdIds.length }, 200);

  } catch (err) {
    error("Transaction failed: " + err.message);

    // Rollback Logic
    if (createdIds.length > 0) {
      log(`Rolling back ${createdIds.length} documents...`);
      try {
        await Promise.all(
          createdIds.map(id => databases.deleteDocument(databaseId, collectionId, id))
        );
        log("Rollback successful.");
      } catch (rbErr) {
        error("Critical: Rollback failed for IDs: " + createdIds.join(', '));
      }
    }

    // Return the error to React with a 500 status code
    return res.json({
      success: false,
      message: err.message,
      rolledBack: createdIds.length > 0,
      failedAtCount: createdIds.length
    }, 500);
  }
};