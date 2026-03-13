import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const createdIds = []; // Track IDs for potential rollback

  try {
    const { databaseId, collectionId, documents } = JSON.parse(req.body);

    log(`Starting transaction-style insert for ${documents.length} docs...`);

    // We use a standard for...of loop here instead of Promise.all 
    // because it allows us to stop immediately at the first failure.
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
        // Stop the loop and throw to the main catch block for rollback
        throw new Error(`Insert failed: ${insertError.message}`);
      }
    }

    return res.json({ success: true, count: createdIds.length }, 200);

  } catch (err) {
    error("Transaction failed. Rolling back...");

    // ROLLBACK LOGIC: Delete everything we just created
    if (createdIds.length > 0) {
      try {
        const { databaseId, collectionId } = JSON.parse(req.body);
        await Promise.all(
          createdIds.map(id => databases.deleteDocument(databaseId, collectionId, id))
        );
        log(`Rollback complete. Deleted ${createdIds.length} partial records.`);
      } catch (rollbackError) {
        error("CRITICAL: Rollback failed! Manual cleanup required for: " + createdIds.join(', '));
      }
    }

    return res.json({
      success: false,
      message: err.message,
      rolledBack: true,
      failedAtCount: createdIds.length
    }, 500);
  }
};