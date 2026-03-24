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

function loadSkoolMembers(progressData) {
  const db = getDb();
  const now = new Date().toISOString();
  const members = Object.values(progressData.processed || {});

  const upsert = db.prepare(`
    INSERT INTO contacts (email, domain, first_name, last_name, company_name, phone, website, source, skool_member, skool_member_id, skool_classification, ghl_contact_id, first_seen_skool, last_synced)
    VALUES (@email, @domain, @first_name, @last_name, @company_name, @phone, @website, @source, 1, @skool_member_id, @skool_classification, @ghl_contact_id, @now, @now)
    ON CONFLICT(email) DO UPDATE SET
      skool_member = 1,
      skool_member_id = excluded.skool_member_id,
      skool_classification = excluded.skool_classification,
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
  let dualEmail = 0;

  const runBatch = db.transaction((batch) => {
    for (const member of batch) {
      const surveyEmail = normalizeEmail(member.survey_a1 || "");
      const profileEmail = normalizeEmail(member.email || "");

      const emails = [];
      if (surveyEmail && surveyEmail.includes("@")) emails.push(surveyEmail);
      if (profileEmail && profileEmail.includes("@") && profileEmail !== surveyEmail) emails.push(profileEmail);

      if (emails.length === 0) continue;
      if (emails.length === 2) dualEmail++;

      const source = detectSource(member);
      const nameParts = (member.full_name || "").trim().split(/\s+/);
      const firstName = nameParts[0] || "";
      const lastName = nameParts.slice(1).join(" ") || "";

      const params = {
        first_name: firstName,
        last_name: lastName,
        company_name: "",
        phone: (member.survey_a2 || "").trim(),
        website: (member.website || "").trim(),
        source,
        skool_member_id: member.id || "",
        skool_classification: member.updated_category || member.classification || "",
        ghl_contact_id: member.ghl_contact_id || null,
        now,
      };

      for (const email of emails) {
        upsert.run({
          ...params,
          email,
          domain: extractDomain(email),
        });
        inserted++;
      }
    }
  });

  for (let i = 0; i < members.length; i += 500) {
    runBatch(members.slice(i, i + 500));
  }

  return { inserted, dual_email: dualEmail };
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
