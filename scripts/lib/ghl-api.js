/**
 * GoHighLevel API helpers.
 * All requests use the v2 API with PIT (Private Integration Token) auth.
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });

const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = 'oLoom4anmXkIlomsYAyK';
const BASE_URL = 'https://services.leadconnectorhq.com';
const REQUEST_DELAY_MS = 500;

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function ghlFetch(endpoint, options = {}) {
  const url = endpoint.startsWith('http') ? endpoint : `${BASE_URL}${endpoint}`;
  const headers = {
    'Authorization': `Bearer ${GHL_API_KEY}`,
    'Version': '2021-07-28',
    'Content-Type': 'application/json',
    ...options.headers,
  };

  const maxRetries = 3;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const resp = await fetch(url, { ...options, headers });
      if (resp.status === 429) {
        const retryAfter = parseInt(resp.headers.get('retry-after') || '5');
        console.log(`  Rate limited, waiting ${retryAfter}s...`);
        await sleep(retryAfter * 1000);
        continue;
      }
      if (!resp.ok) {
        const text = await resp.text();
        if (attempt < maxRetries - 1) {
          console.log(`  API error ${resp.status}, retrying in 2s...`);
          await sleep(2000);
          continue;
        }
        throw new Error(`GHL API ${resp.status}: ${text.substring(0, 200)}`);
      }
      return await resp.json();
    } catch (err) {
      if (attempt < maxRetries - 1 && err.code === 'ECONNRESET') {
        await sleep(2000);
        continue;
      }
      throw err;
    }
  }
}

async function searchContact(name) {
  await sleep(REQUEST_DELAY_MS);
  try {
    const data = await ghlFetch(`/contacts/?query=${encodeURIComponent(name)}&locationId=${LOCATION_ID}&limit=1`, {
      method: 'GET',
    });
    if (data.contacts && data.contacts.length > 0) {
      return data.contacts[0];
    }
    return null;
  } catch (err) {
    console.log(`  Search failed for "${name}": ${err.message.substring(0, 100)}`);
    return null;
  }
}

async function getConversations(contactId) {
  await sleep(REQUEST_DELAY_MS);
  try {
    const data = await ghlFetch(`/conversations/search?contactId=${contactId}&locationId=${LOCATION_ID}&limit=5`);
    return data.conversations || [];
  } catch (err) {
    console.log(`  Conversations fetch failed: ${err.message.substring(0, 100)}`);
    return [];
  }
}

async function getMessages(conversationId, limit = 100) {
  await sleep(REQUEST_DELAY_MS);
  try {
    const data = await ghlFetch(`/conversations/${conversationId}/messages?limit=${limit}`, {
      method: 'GET',
    });
    return data.messages?.messages || data.messages || [];
  } catch (err) {
    console.log(`  Messages fetch failed: ${err.message.substring(0, 100)}`);
    return [];
  }
}

async function updateContact(contactId, body) {
  await sleep(REQUEST_DELAY_MS);
  return ghlFetch(`/contacts/${contactId}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

async function addContactTag(contactId, tag) {
  await sleep(REQUEST_DELAY_MS);
  // Fetch current tags first to avoid overwriting
  const contact = await ghlFetch(`/contacts/${contactId}`, { method: 'GET' });
  const currentTags = contact.contact?.tags || [];
  if (currentTags.includes(tag)) return { alreadyTagged: true };

  return updateContact(contactId, { tags: [...currentTags, tag] });
}

async function getContactNotes(contactId) {
  await sleep(REQUEST_DELAY_MS);
  try {
    const data = await ghlFetch(`/contacts/${contactId}/notes`);
    return data.notes || [];
  } catch (err) {
    console.log(`  Notes fetch failed: ${err.message.substring(0, 100)}`);
    return [];
  }
}

async function createContactNote(contactId, body) {
  await sleep(REQUEST_DELAY_MS);
  return ghlFetch(`/contacts/${contactId}/notes`, {
    method: 'POST',
    body: JSON.stringify({ body }),
  });
}

async function updateContactNote(contactId, noteId, body) {
  await sleep(REQUEST_DELAY_MS);
  return ghlFetch(`/contacts/${contactId}/notes/${noteId}`, {
    method: 'PUT',
    body: JSON.stringify({ body }),
  });
}

module.exports = {
  ghlFetch,
  searchContact,
  getConversations,
  getMessages,
  updateContact,
  addContactTag,
  getContactNotes,
  createContactNote,
  updateContactNote,
  sleep,
  GHL_API_KEY,
  LOCATION_ID,
  BASE_URL,
  REQUEST_DELAY_MS,
};
