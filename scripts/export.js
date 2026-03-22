/**
 * Export classified members to CSV for easy review.
 * Reads data/classified/classified_members.json
 * Writes data/classified/classified_members.csv
 */

const fs = require("fs");
const path = require("path");
const { stringify } = require("csv-stringify/sync");

const IN_PATH = path.join(__dirname, "../data/classified/classified_members.json");
const OUT_PATH = path.join(__dirname, "../data/classified/classified_members.csv");

const members = JSON.parse(fs.readFileSync(IN_PATH, "utf-8"));

const rows = members.map((m) => ({
  id: m.id,
  full_name: m.full_name,
  username: m.username,
  email: m.email,
  bio: m.bio,
  location: m.location,
  classification: m.classification,
  confidence: m.classification_confidence,
  reasoning: m.classification_reasoning,
  dm_message_count: m.dm_message_count,
  website: m.website,
  skool_url: m.skool_url,
}));

const csv = stringify(rows, { header: true });
fs.writeFileSync(OUT_PATH, csv);
console.log(`Exported ${rows.length} classified members to ${OUT_PATH}`);

// Print breakdown
const counts = {};
for (const m of members) {
  const cat = m.classification;
  counts[cat] = (counts[cat] || 0) + 1;
}
console.log("\nBreakdown:");
for (const [cat, count] of Object.entries(counts).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${cat}: ${count} (${((count / members.length) * 100).toFixed(1)}%)`);
}
