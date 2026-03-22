const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');

const PROGRESS_FILE = path.join(__dirname, '..', 'data', 'enriched', 'progress.json');
const OUTPUT_FILE = path.join(__dirname, '..', 'data', 'enriched', 'enriched_members.csv');
const INPUT_FILE = path.join(__dirname, '..', 'data', 'classified', 'classified_members.csv');

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

function exportResults(members, progress) {
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

  const csv = stringify(enriched, { header: true });
  fs.writeFileSync(OUTPUT_FILE, csv);
  return enriched;
}

function printStats(enriched) {
  const found = enriched.filter(e => e.ghl_contact_found).length;
  const changed = enriched.filter(e => e.category_changed).length;

  const categories = {};
  enriched.forEach(e => {
    const cat = e.updated_category;
    categories[cat] = (categories[cat] || 0) + 1;
  });

  console.log('\n=== ENRICHMENT SUMMARY ===');
  console.log(`Total members: ${enriched.length}`);
  console.log(`Found in GHL: ${found}`);
  console.log(`Not found in GHL: ${enriched.length - found}`);
  console.log(`Reclassified from GHL data: ${changed}`);
  console.log('\nFinal category breakdown:');
  Object.entries(categories).sort((a, b) => b[1] - a[1]).forEach(([cat, count]) => {
    console.log(`  ${cat}: ${count}`);
  });

  if (changed > 0) {
    console.log('\nAll reclassifications:');
    enriched.filter(e => e.category_changed).forEach(e => {
      console.log(`  ${e.full_name}: ${e.classification} -> ${e.updated_category}`);
      console.log(`    Reason: ${e.change_reasoning}`);
    });
  }
}

const command = process.argv[2];

if (command === 'load') {
  const csv = fs.readFileSync(INPUT_FILE, 'utf8');
  const members = parse(csv, { columns: true });
  const progress = loadProgress();
  const remaining = members.filter(m => !progress.processed[m.id]);
  console.log(`Total members: ${members.length}`);
  console.log(`Already processed: ${Object.keys(progress.processed).length}`);
  console.log(`Remaining: ${remaining.length}`);

  const batchSize = parseInt(process.argv[3]) || 10;
  const batch = remaining.slice(0, batchSize);
  console.log(`\nNext batch (${batch.length}):`);
  batch.forEach(m => console.log(`  ${m.full_name} | ${m.id} | current: ${m.classification}`));

} else if (command === 'export') {
  const csv = fs.readFileSync(INPUT_FILE, 'utf8');
  const members = parse(csv, { columns: true });
  const progress = loadProgress();
  const enriched = exportResults(members, progress);
  printStats(enriched);

} else if (command === 'status') {
  const csv = fs.readFileSync(INPUT_FILE, 'utf8');
  const members = parse(csv, { columns: true });
  const progress = loadProgress();
  const processed = Object.values(progress.processed);
  const found = processed.filter(p => p.ghl_contact_found).length;
  const changed = processed.filter(p => p.category_changed).length;
  const withConvo = processed.filter(p => p.ghl_conversation_summary && p.ghl_conversation_summary !== 'No inbound messages').length;
  console.log(`Processed: ${processed.length}/${members.length}`);
  console.log(`Found in GHL: ${found}`);
  console.log(`Had inbound messages: ${withConvo}`);
  console.log(`Reclassified: ${changed}`);
  if (changed > 0) {
    console.log('\nChanges:');
    processed.filter(p => p.category_changed).forEach(p => {
      console.log(`  ${p.full_name}: ${p.classification} -> ${p.updated_category}`);
      console.log(`    ${p.change_reasoning}`);
    });
  }

} else {
  console.log('Usage: node scripts/enrich.js <command>');
  console.log('  load [batchSize] - Show next batch to process');
  console.log('  export           - Export enriched CSV');
  console.log('  status           - Show progress');
}
