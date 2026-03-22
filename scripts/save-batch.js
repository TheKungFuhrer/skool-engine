// Saves a batch of GHL enrichment results to progress.
// Classification is based ONLY on the lead's own inbound messages.
// Tags, custom fields, and pipeline stages are completely ignored.
//
// Usage: node scripts/save-batch.js <batch.json>
//
// batch.json format:
// [
//   {
//     "memberId": "skool-member-id",
//     "ghlContactId": "ghl-contact-id or null",
//     "inboundMessages": "concatenated text of lead's own messages only",
//     "updatedCategory": "category if changed, or null to keep original",
//     "changeReasoning": "why category changed, based on lead's own words"
//   }
// ]

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

const PROGRESS_FILE = path.join(__dirname, '..', 'data', 'enriched', 'progress.json');
const INPUT_FILE = path.join(__dirname, '..', 'data', 'classified', 'classified_members.csv');

function loadProgress() {
  try { return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8')); }
  catch { return { processed: {} }; }
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

const inputFile = process.argv[2];
if (!inputFile) { console.error('Usage: node scripts/save-batch.js <batch.json>'); process.exit(1); }
const input = fs.readFileSync(inputFile, 'utf8');
const batch = JSON.parse(input);

const csv = fs.readFileSync(INPUT_FILE, 'utf8');
const members = parse(csv, { columns: true });
const memberMap = {};
members.forEach(m => { memberMap[m.id] = m; });

const progress = loadProgress();
let saved = 0;
let changed = 0;

batch.forEach(({ memberId, ghlContactId, inboundMessages, updatedCategory, changeReasoning }) => {
  const member = memberMap[memberId];
  if (!member) return;

  const hasInbound = inboundMessages && inboundMessages !== 'No inbound messages';
  const categoryChanged = updatedCategory && updatedCategory !== member.classification;

  const result = {
    ...member,
    ghl_contact_found: !!ghlContactId,
    ghl_contact_id: ghlContactId || '',
    ghl_conversation_summary: inboundMessages || 'No inbound messages',
    updated_category: categoryChanged ? updatedCategory : member.classification,
    category_changed: !!categoryChanged,
    change_reasoning: categoryChanged ? changeReasoning : '',
  };

  if (categoryChanged) changed++;
  progress.processed[memberId] = result;
  saved++;
});

saveProgress(progress);
console.log(`Saved ${saved} results (${changed} reclassified). Total processed: ${Object.keys(progress.processed).length}/${members.length}`);
