/**
 * Automated GHL Enrichment Script
 *
 * For each Skool member:
 * 1. Search GHL by name → get contact ID
 * 2. Get conversations for that contact
 * 3. Pull messages, filter to INBOUND only (lead's own words)
 * 4. Classify based ONLY on lead's inbound messages
 * 5. Save progress after each batch
 *
 * Tags, custom fields, and pipeline stages are COMPLETELY IGNORED.
 * Only the lead's own inbound messages drive classification changes.
 */

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = 'oLoom4anmXkIlomsYAyK';
const BASE_URL = 'https://services.leadconnectorhq.com';

const PROGRESS_FILE = path.join(__dirname, '..', 'data', 'enriched', 'progress.json');
const OUTPUT_FILE = path.join(__dirname, '..', 'data', 'enriched', 'enriched_members.csv');
const INPUT_FILE = path.join(__dirname, '..', 'data', 'classified', 'classified_members.csv');

const BATCH_SIZE = 50;
const REQUEST_DELAY_MS = 500;

// --- API Helpers ---

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

// --- Classification from inbound messages ---

function classifyFromInboundMessages(inboundTexts, currentCategory) {
  if (!inboundTexts || inboundTexts.length === 0) return { category: null, reasoning: '' };

  const combined = inboundTexts.join(' ').toLowerCase();

  // Skip classification-irrelevant messages
  const skipPatterns = ['stop', 'unsubscribe', 'remove', 'opt out', 'please remove'];
  const meaningful = inboundTexts.filter(t => {
    const lower = t.toLowerCase().trim();
    return lower.length > 2 && !skipPatterns.some(p => lower === p);
  });

  if (meaningful.length === 0) return { category: null, reasoning: '' };

  const meaningfulCombined = meaningful.join(' ').toLowerCase();

  // --- Active venue owner signals ---
  const venueOwnerPatterns = [
    /i (?:own|have|run|manage|operate) (?:a |an |my |the |our )?(?:venue|event (?:space|center|hall)|banquet|ballroom|reception hall|wedding venue|barn|estate|manor)/i,
    /my (?:venue|event (?:space|center|hall)|banquet|ballroom|property)/i,
    /our (?:venue|event (?:space|center|hall)|banquet|ballroom)/i,
    /(?:i'?m |i am )(?:a )?venue owner/i,
    /already renting/i,
    /(?:i |we )(?:already |currently )?(?:host|book|rent out) (?:events|weddings|parties)/i,
    /(?:had|have) (?:a |an |my |our )?(?:event )?(?:hall|venue|space) for \d+ ?y/i,
    /(?:we|i) (?:do|get|have) (?:about |around )?\d+ (?:events?|bookings?|weddings?) (?:a |per )/i,
    /(?:i |we )(?:just )?opened (?:a |my |our )?(?:venue|space|hall)/i,
  ];

  for (const pattern of venueOwnerPatterns) {
    if (pattern.test(meaningfulCombined)) {
      return {
        category: 'active_venue_owner',
        reasoning: `Lead's own words indicate active venue ownership: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- Aspiring venue owner signals ---
  const aspiringPatterns = [
    /(?:looking|want|trying|planning|hoping|ready) to (?:get started|start|open|launch|build|create|find)/i,
    /get started/i,
    /(?:don'?t|do not) have (?:a )?(?:venue|space|property) yet/i,
    /looking for (?:a )?(?:property|venue|space|location|building)/i,
    /(?:want|hope|plan) to (?:have|open|start) (?:a |my |our )?(?:venue|event|space)/i,
    /aspiring/i,
    /(?:i'?m |i am )(?:just )?getting started/i,
    /not (?:yet|currently) (?:renting|operating|open)/i,
    /in the process of/i,
    /(?:looking|searching) for (?:a )?(?:commercial|residential) (?:property|space|building)/i,
  ];

  for (const pattern of aspiringPatterns) {
    if (pattern.test(meaningfulCombined)) {
      // Don't reclassify someone who's already active_venue_owner to aspiring
      // Their response might just mean "getting started with your service"
      if (currentCategory === 'active_venue_owner') {
        return { category: null, reasoning: '' };
      }
      return {
        category: 'aspiring_venue_owner',
        reasoning: `Lead's own words indicate aspiring venue ownership: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- Service provider signals ---
  const serviceProviderPatterns = [
    /(?:i |we )(?:am |are )?(?:a )?(?:photographer|dj|caterer|florist|planner|decorator|videographer|officiant|bartender|stylist)/i,
    /(?:i |we )(?:do |provide |offer )(?:photography|catering|floral|flowers|decorat|planning|video|bartending|hair|makeup|dj)/i,
    /(?:my |our )(?:photography|catering|floral|decor|planning|dj|video|photo ?booth|bakery|cake) (?:business|company|service)/i,
    /(?:i |we )(?:have|own|run|operate) (?:a )?(?:photo ?booth|bakery|cake|catering|floral|decor|planning|dj|rental) (?:business|company|service)?/i,
    /rent(?:ing)? out (?:my |our )?(?:equipment|tables|chairs|linens|photo ?booth|bounce|inflatab)/i,
    /(?:i |we )(?:sell|make|bake|create) (?:cakes?|desserts?|flowers?|arrangements?)/i,
  ];

  for (const pattern of serviceProviderPatterns) {
    if (pattern.test(meaningfulCombined)) {
      return {
        category: 'service_provider',
        reasoning: `Lead's own words indicate service provider: "${meaningful.find(t => pattern.test(t.toLowerCase())) || meaningful[0]}"`,
      };
    }
  }

  // --- "Other" signals (clearly not in the industry) ---
  const otherPatterns = [
    /(?:class|education|learn|course|school|training)/i,
    /(?:not interested|wrong number|who is this|don'?t know)/i,
    /(?:air ?b.?n.?b|airbnb|short.term rental)/i,
  ];

  // Only suggest "other" if currently in a venue-related category
  if (currentCategory !== 'other') {
    for (const pattern of otherPatterns) {
      if (pattern.test(meaningfulCombined)) {
        // Be conservative - don't reclassify based on weak signals
        // Only if the signal is very clear
        if (meaningfulCombined.includes('class') && meaningfulCombined.includes('education') && currentCategory !== 'active_venue_owner') {
          return {
            category: 'other',
            reasoning: `Lead's own words indicate they're interested in classes/education, not venue ownership: "${meaningful[0]}"`,
          };
        }
      }
    }
  }

  return { category: null, reasoning: '' };
}

// --- Progress management ---

function loadProgress() {
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
  } catch {
    return { processed: {} };
  }
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// --- Main processing ---

async function processOneMember(member) {
  const name = member.full_name;
  if (!name || name.trim() === '') {
    return {
      ...member,
      ghl_contact_found: false,
      ghl_contact_id: '',
      ghl_conversation_summary: '',
      updated_category: member.classification,
      category_changed: false,
      change_reasoning: '',
    };
  }

  // Step 1: Search for contact
  const contact = await searchContact(name);
  if (!contact) {
    return {
      ...member,
      ghl_contact_found: false,
      ghl_contact_id: '',
      ghl_conversation_summary: 'Not found in GHL',
      updated_category: member.classification,
      category_changed: false,
      change_reasoning: '',
    };
  }

  // Step 2: Get conversations
  const conversations = await getConversations(contact.id);

  // Step 3: Get messages from all conversations, filter inbound
  let allInbound = [];
  for (const convo of conversations.slice(0, 3)) {
    const messages = await getMessages(convo.id, 100);
    const inbound = (Array.isArray(messages) ? messages : [])
      .filter(m => m.direction === 'inbound')
      .map(m => m.body || '')
      .filter(m => m.trim().length > 0);
    allInbound.push(...inbound);
  }

  // Step 4: Classify from inbound messages
  const inboundSummary = allInbound.length > 0
    ? allInbound.join('; ').substring(0, 500)
    : 'No inbound messages';

  const { category, reasoning } = classifyFromInboundMessages(allInbound, member.classification);

  return {
    ...member,
    ghl_contact_found: true,
    ghl_contact_id: contact.id,
    ghl_conversation_summary: inboundSummary,
    updated_category: category || member.classification,
    category_changed: !!(category && category !== member.classification),
    change_reasoning: (category && category !== member.classification) ? reasoning : '',
  };
}

async function main() {
  if (!GHL_API_KEY) {
    console.error('ERROR: GHL_API_KEY not found in .env');
    process.exit(1);
  }

  console.log('=== GHL Enrichment Pipeline ===');
  console.log(`API Key: ${GHL_API_KEY.substring(0, 20)}...`);
  console.log(`Location: ${LOCATION_ID}`);
  console.log(`Delay: ${REQUEST_DELAY_MS}ms between requests\n`);

  // Load members
  const csv = fs.readFileSync(INPUT_FILE, 'utf8');
  const members = parse(csv, { columns: true });
  console.log(`Total members: ${members.length}`);

  // Load progress
  const progress = loadProgress();
  const alreadyProcessed = Object.keys(progress.processed).length;
  console.log(`Already processed: ${alreadyProcessed}`);

  // Filter to unprocessed
  const remaining = members.filter(m => !progress.processed[m.id]);
  console.log(`Remaining: ${remaining.length}\n`);

  if (remaining.length === 0) {
    console.log('All members already processed!');
    exportAndSummarize(members, progress);
    return;
  }

  // Process in batches
  let batchNum = 0;
  for (let i = 0; i < remaining.length; i += BATCH_SIZE) {
    batchNum++;
    const batch = remaining.slice(i, i + BATCH_SIZE);
    const batchStart = alreadyProcessed + i + 1;
    const batchEnd = Math.min(batchStart + batch.length - 1, members.length);
    console.log(`\n--- Batch ${batchNum} (members ${batchStart}-${batchEnd}) ---`);

    for (let j = 0; j < batch.length; j++) {
      const member = batch[j];
      const idx = batchStart + j;
      process.stdout.write(`  [${idx}/${members.length}] ${member.full_name}... `);

      try {
        const result = await processOneMember(member);
        progress.processed[member.id] = result;

        if (!result.ghl_contact_found) {
          console.log('NOT FOUND');
        } else if (result.category_changed) {
          console.log(`RECLASSIFIED: ${member.classification} -> ${result.updated_category}`);
        } else {
          const hasInbound = result.ghl_conversation_summary !== 'No inbound messages';
          console.log(hasInbound ? 'OK (has inbound)' : 'OK (no inbound)');
        }
      } catch (err) {
        console.log(`ERROR: ${err.message.substring(0, 80)}`);
        // Save as unprocessed with error note
        progress.processed[member.id] = {
          ...member,
          ghl_contact_found: false,
          ghl_contact_id: '',
          ghl_conversation_summary: `Error: ${err.message.substring(0, 200)}`,
          updated_category: member.classification,
          category_changed: false,
          change_reasoning: '',
        };
      }
    }

    // Save progress after each batch
    saveProgress(progress);
    const totalProcessed = Object.keys(progress.processed).length;
    const totalChanged = Object.values(progress.processed).filter(p => p.category_changed).length;
    console.log(`\n  Batch saved. Total: ${totalProcessed}/${members.length} processed, ${totalChanged} reclassified`);
  }

  // Final export
  exportAndSummarize(members, progress);
}

function exportAndSummarize(members, progress) {
  // Build enriched output
  const enriched = members.map(m => {
    const p = progress.processed[m.id];
    if (p) return p;
    return {
      ...m,
      ghl_contact_found: false,
      ghl_contact_id: '',
      ghl_conversation_summary: '',
      updated_category: m.classification,
      category_changed: false,
      change_reasoning: '',
    };
  });

  // Write CSV
  const csvOut = stringify(enriched, { header: true });
  fs.writeFileSync(OUTPUT_FILE, csvOut);

  // Summary
  const found = enriched.filter(e => e.ghl_contact_found === true || e.ghl_contact_found === 'true').length;
  const changed = enriched.filter(e => e.category_changed === true || e.category_changed === 'true').length;
  const withInbound = enriched.filter(e =>
    e.ghl_conversation_summary &&
    e.ghl_conversation_summary !== 'No inbound messages' &&
    e.ghl_conversation_summary !== 'Not found in GHL' &&
    !e.ghl_conversation_summary.startsWith('Error:')
  ).length;

  const categories = {};
  enriched.forEach(e => {
    const cat = e.updated_category;
    categories[cat] = (categories[cat] || 0) + 1;
  });

  console.log('\n\n========================================');
  console.log('       ENRICHMENT SUMMARY');
  console.log('========================================');
  console.log(`Total members:           ${enriched.length}`);
  console.log(`Found in GHL:            ${found}`);
  console.log(`Not found in GHL:        ${enriched.length - found}`);
  console.log(`Had inbound messages:    ${withInbound}`);
  console.log(`Reclassified from GHL:   ${changed}`);
  console.log('\nFinal category breakdown:');
  Object.entries(categories)
    .sort((a, b) => b[1] - a[1])
    .forEach(([cat, count]) => {
      console.log(`  ${cat}: ${count}`);
    });

  if (changed > 0) {
    console.log('\nAll reclassifications:');
    enriched.filter(e => e.category_changed === true || e.category_changed === 'true').forEach(e => {
      console.log(`  ${e.full_name}: ${e.classification} -> ${e.updated_category}`);
      console.log(`    ${e.change_reasoning}`);
    });
  }

  console.log(`\nOutput saved to: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
