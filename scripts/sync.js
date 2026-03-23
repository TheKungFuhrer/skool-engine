/**
 * Sync Orchestrator
 *
 * Runs the full sync pipeline:
 * 1. Ingest — detect new/updated members and DMs from CSV drops
 * 2. Writeback — create/update master notes on GHL contacts
 * 3. Export — generate enriched CSV
 *
 * Usage: npm run sync
 */

const fs = require('fs');
const { stringify } = require('csv-stringify/sync');
const { loadProgress, PATHS } = require('./lib/progress');

async function main() {
  console.log('╔════════════════════════════════════════╗');
  console.log('║         SKOOL ENGINE SYNC              ║');
  console.log('╚════════════════════════════════════════╝\n');

  // Step 1: Ingest
  const { main: ingest } = require('./sync-ingest');
  const ingestStats = ingest();

  console.log('');

  // Step 2: Writeback (master notes)
  const { main: writeback } = require('./sync-writeback');
  const writebackStats = await writeback();

  // Step 3: Export enriched CSV
  console.log('\n=== Exporting Enriched CSV ===');
  const progress = loadProgress();
  const members = Object.values(progress.processed);

  if (members.length > 0) {
    const csv = stringify(members, { header: true });
    fs.writeFileSync(PATHS.enrichedCsv, csv);
    console.log(`  Exported ${members.length} members to ${PATHS.enrichedCsv}`);
  }

  // Final category breakdown
  const categories = {};
  members.forEach(m => {
    const cat = m.updated_category || m.classification || 'unknown';
    categories[cat] = (categories[cat] || 0) + 1;
  });

  // Summary
  console.log('\n╔════════════════════════════════════════╗');
  console.log('║            SYNC SUMMARY                ║');
  console.log('╠════════════════════════════════════════╣');
  console.log(`║  New members found:     ${String(ingestStats.newMembers).padStart(6)}       ║`);
  console.log(`║  Updated profiles:      ${String(ingestStats.updatedProfiles).padStart(6)}       ║`);
  console.log(`║  Members with new DMs:  ${String(ingestStats.newDMs).padStart(6)}       ║`);
  console.log(`║  Reclassified:          ${String(ingestStats.reclassified).padStart(6)}       ║`);
  console.log(`║  Notes created:         ${String(writebackStats.created).padStart(6)}       ║`);
  console.log(`║  Notes updated:         ${String(writebackStats.updated).padStart(6)}       ║`);
  console.log(`║  GHL write errors:      ${String(writebackStats.errors).padStart(6)}       ║`);
  console.log('╠════════════════════════════════════════╣');
  console.log('║  Category Breakdown:                   ║');
  Object.entries(categories)
    .sort((a, b) => b[1] - a[1])
    .forEach(([cat, count]) => {
      console.log(`║    ${cat.padEnd(24)} ${String(count).padStart(5)}    ║`);
    });
  console.log('╚════════════════════════════════════════╝');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
