/**
 * Merge Skoot CRM export (CSV) with Inbox Insights DM history (XLSX)
 * into unified member profiles at data/merged/members.json
 */

const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");
const XLSX = require("xlsx");

const CSV_PATH = path.join(__dirname, "../data/imports/export_plus_active_20251205_235149.csv");
const XLSX_PATH = path.join(__dirname, "../data/imports/Inbox Insights.xlsx");
const OUT_PATH = path.join(__dirname, "../data/merged/members.json");

// --- Load CSV ---
console.log("Loading CSV...");
const csvRaw = fs.readFileSync(CSV_PATH, "utf-8");
const members = parse(csvRaw, { columns: true, skip_empty_lines: true });
console.log(`  ${members.length} members loaded from CSV`);

// --- Load XLSX ---
console.log("Loading XLSX...");
const wb = XLSX.readFile(XLSX_PATH);
const ws = wb.Sheets[wb.SheetNames[0]];
const messages = XLSX.utils.sheet_to_json(ws);
console.log(`  ${messages.length} messages loaded from XLSX`);

// --- Group DMs by Chat name ---
const dmByName = {};
for (const msg of messages) {
  const chatName = (msg.Chat || "").trim();
  if (!chatName) continue;
  if (!dmByName[chatName]) dmByName[chatName] = [];
  dmByName[chatName].push({
    sender: msg.Sender || "",
    message: (msg.Message || "").substring(0, 2000), // cap very long messages
  });
}
console.log(`  ${Object.keys(dmByName).length} unique DM conversations`);

// --- Merge ---
let matched = 0;
const merged = members.map((m) => {
  const fullName = `${m["First Name"] || ""} ${m["Last Name"] || ""}`.trim();
  const dms = dmByName[fullName] || [];
  if (dms.length > 0) matched++;

  // Only include messages FROM the member (not from admin/Dylan)
  const memberDMs = dms.filter((d) => d.sender !== "You");
  const dmText = memberDMs
    .map((d) => d.message)
    .join("\n")
    .substring(0, 8000); // cap total DM context

  return {
    id: m.ID,
    first_name: m["First Name"] || "",
    last_name: m["Last Name"] || "",
    full_name: fullName,
    username: m.Name || "",
    email: m.Email || "",
    bio: m.Bio || "",
    location: m["Request Location"] || "",
    survey_q1: m["Survey Question 1"] || "",
    survey_a1: m["Survey Answer 1"] || "",
    survey_q2: m["Survey Question 2"] || "",
    survey_a2: m["Survey Answer 2"] || "",
    survey_q3: m["Survey Question 3"] || "",
    survey_a3: m["Survey Answer 3"] || "",
    points: m.Points || "",
    level: m.Level || "",
    role: m.Role || "",
    ace_score: m["ACE Score"] || "",
    ace_explanation: m["ACE Score Explanation"] || "",
    skool_url: m["Skool Profile URL"] || "",
    website: m["Website Link"] || "",
    facebook: m["Facebook Link"] || "",
    instagram: m["Instagram Link"] || "",
    linkedin: m["LinkedIn Link"] || "",
    dm_message_count: dms.length,
    dm_text: dmText,
  };
});

console.log(`  ${matched} members matched to DM conversations out of ${merged.length}`);
console.log(`  ${Object.keys(dmByName).length - matched} DM conversations had no CSV match`);

fs.writeFileSync(OUT_PATH, JSON.stringify(merged, null, 2));
console.log(`\nWrote ${merged.length} merged profiles to ${OUT_PATH}`);
