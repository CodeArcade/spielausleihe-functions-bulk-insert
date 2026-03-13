import { Client, Databases, ID } from 'node-appwrite';

export default async ({ req, res, log, error }) => {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_FUNCTION_API_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);

  try {
    const { databaseId, collectionId, documents } = JSON.parse(req.body);

    const response = await databases.createDocuments(
      databaseId,
      collectionId,
      documents.map(data => ({
        documentId: ID.unique(),
        data: data
      }))
    );

    return res.json({ success: true, count: response.length });

  } catch (err) {
    error("Bulk insert failed: " + err.message);
    return res.json({ success: false, error: err.message }, 500);
  }
};