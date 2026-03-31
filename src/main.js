import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const createdIds = [];

  const sleep = (ms) => new Promise(res => setTimeout(res, ms));

  const safeCreateDocument = async (dbId, collId, data, retries = 2) => {
    try {
      return await databases.createDocument(dbId, collId, ID.unique(), data);
    } catch (err) {
      // If it's a fetch failure and we have retries left, wait and try again
      if (retries > 0 && err.message.includes('fetch failed')) {
        await sleep(500);
        return safeCreateDocument(dbId, collId, data, retries - 1);
      }
      throw err;
    }
  };

  let body = req.bodyJson;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (e) { return res.json({ success: false }, 400); }
  }

  const { databaseId, collectionId, documents } = body || {};

  try {
    for (const data of documents) {
      const doc = await safeCreateDocument(databaseId, collectionId, data);
      createdIds.push(doc.$id);

      await sleep(50);
    }

    return res.json({ success: true, count: createdIds.length }, 200);

  } catch (err) {
    error(`Failed at doc ${createdIds.length + 1}: ${err.message}`);

    // Sequential Rollback (Reliable)
    if (createdIds.length > 0) {
      for (const id of createdIds) {
        try {
          await databases.deleteDocument(databaseId, collectionId, id);
          await sleep(30);
        } catch (e) { error(`Rollback failed for ${id}`); }
      }
    }

    return res.json({ success: false, message: err.message }, 500);
  }
};