/**
 * Skool identity adapter.
 *
 * Loads Skool members into the shared identity DB, detects overlaps
 * with cold outreach leads, and manages GHL tagging state.
 */

const path = require("path");
const fs = require("fs");

// Import shared DB module from cold-outreach-engine
const COLD_OUTREACH_ROOT = "C:\\Users\\Administrator\\projects\\cold-outreach-engine";
const { openDb, getDb, closeDb, normalizeEmail } = require(path.join(COLD_OUTREACH_ROOT, "shared", "identity-db"));

// Load referral exclusion list from shared-data config
const CONFIG_PATH = path.join("C:", "Users", "Administrator", "projects", "shared-data", "config.json");

function loadConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
  } catch {
    return { referral_exclusions: [] };
  }
}

function detectSource(member) {
  const referralAnswer = (member.survey_a3 || "").trim().toLowerCase();
  if (!referralAnswer) return "skool_organic";

  const config = loadConfig();
  const exclusions = (config.referral_exclusions || []).map(s => s.toLowerCase());

  for (const exc of exclusions) {
    if (referralAnswer.includes(exc)) return "skool_organic";
  }

  return "skool_referred";
}

function extractDomain(email) {
  if (!email || !email.includes("@")) return "";
  return email.split("@")[1].trim().toLowerCase();
}

/**
 * Bulk-upsert Skool members into the identity DB.
 * Accepts an array of GHL contact objects (from the /contacts/ API).
 * Each contact has: id, email, firstName, lastName, phone, companyName, tags, website, etc.
 * @param {object[]} ghlContacts - Array of GHL contact objects tagged "skool"
 * @returns {{ inserted: number, skipped: number }}
 */
function loadSkoolMembers(ghlContacts) {
  const db = getDb();
  const now = new Date().toISOString();

  const upsert = db.prepare(`
    INSERT INTO contacts (email, domain, first_name, last_name, company_name, phone, website, source, skool_member, skool_member_id, skool_classification, ghl_contact_id, first_seen_skool, last_synced)
    VALUES (@email, @domain, @first_name, @last_name, @company_name, @phone, @website, @source, 1, @skool_member_id, @skool_classification, @ghl_contact_id, @now, @now)
    ON CONFLICT(email) DO UPDATE SET
      skool_member = 1,
      skool_member_id = COALESCE(excluded.skool_member_id, contacts.skool_member_id),
      skool_classification = COALESCE(excluded.skool_classification, contacts.skool_classification),
      ghl_contact_id = COALESCE(excluded.ghl_contact_id, contacts.ghl_contact_id),
      domain = COALESCE(contacts.domain, excluded.domain),
      first_name = COALESCE(NULLIF(contacts.first_name, ''), excluded.first_name),
      last_name = COALESCE(NULLIF(contacts.last_name, ''), excluded.last_name),
      company_name = COALESCE(NULLIF(contacts.company_name, ''), excluded.company_name),
      phone = COALESCE(NULLIF(contacts.phone, ''), excluded.phone),
      website = COALESCE(NULLIF(contacts.website, ''), excluded.website),
      first_seen_skool = COALESCE(contacts.first_seen_skool, excluded.first_seen_skool),
      last_synced = @now
  `);

  let inserted = 0;
  let skipped = 0;

  const runBatch = db.transaction((batch) => {
    for (const contact of batch) {
      const email = normalizeEmail(contact.email || "");
      if (!email || !email.includes("@")) { skipped++; continue; }

      upsert.run({
        email,
        domain: extractDomain(email),
        first_name: (contact.firstName || "").trim(),
        last_name: (contact.lastName || "").trim(),
        company_name: (contact.companyName || contact.businessName || "").trim(),
        phone: (contact.phone || "").trim(),
        website: (contact.website || "").trim(),
        source: "skool_organic",
        skool_member_id: "",
        skool_classification: "",
        ghl_contact_id: contact.id || null,
        now,
      });
      inserted++;
    }
  });

  for (let i = 0; i < ghlContacts.length; i += 500) {
    runBatch(ghlContacts.slice(i, i + 500));
  }

  return { inserted, skipped };
}

function getUntaggedOverlaps() {
  const db = getDb();
  return db.prepare(`
    SELECT email, ghl_contact_id, skool_member_id, company_name
    FROM contacts
    WHERE cold_outreach_lead = 1
      AND skool_member = 1
      AND ghl_tagged = 0
      AND ghl_contact_id IS NOT NULL
  `).all();
}

function markTagged(email) {
  const db = getDb();
  db.prepare("UPDATE contacts SET ghl_tagged = 1 WHERE email = ?").run(normalizeEmail(email));
}

function getStats() {
  const db = getDb();
  const total = db.prepare("SELECT COUNT(*) as c FROM contacts").get().c;
  const cold = db.prepare("SELECT COUNT(*) as c FROM contacts WHERE cold_outreach_lead = 1").get().c;
  const skool = db.prepare("SELECT COUNT(*) as c FROM contacts WHERE skool_member = 1").get().c;
  const overlaps = db.prepare("SELECT COUNT(*) as c FROM contacts WHERE cold_outreach_lead = 1 AND skool_member = 1").get().c;
  const suppressed = db.prepare("SELECT COUNT(*) as c FROM contacts WHERE smartlead_suppressed = 1").get().c;
  const tagged = db.prepare("SELECT COUNT(*) as c FROM contacts WHERE ghl_tagged = 1").get().c;

  const sourceRows = db.prepare("SELECT source, COUNT(*) as c FROM contacts GROUP BY source").all();
  const by_source = {};
  for (const row of sourceRows) {
    by_source[row.source || "unknown"] = row.c;
  }

  return { total, cold_outreach: cold, skool, overlaps, suppressed, tagged, by_source };
}

module.exports = {
  openDb,
  closeDb,
  loadSkoolMembers,
  getUntaggedOverlaps,
  markTagged,
  getStats,
  detectSource,
};
