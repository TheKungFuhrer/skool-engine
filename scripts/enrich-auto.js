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

const { searchContact, getConversations, getMessages, GHL_API_KEY, REQUEST_DELAY_MS, LOCATION_ID } = require('./lib/ghl-api');
const { classifyFromInboundMessages } = require('./lib/classify');
const { loadProgress, saveProgress, PATHS } = require('./lib/progress');

const PROGRESS_FILE = PATHS.progress;
const OUTPUT_FILE = PATHS.enrichedCsv;
const INPUT_FILE = PATHS.classifiedCsv;

const BATCH_SIZE = 50;

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
