/**
 * Sync Ingestion Script
 *
 * Detects new/updated members and DMs from CSV drops in data/imports/.
 * Auto-detects which CSV is the Skool member export vs Inbox Insights export.
 * Compares against last-import state to find deltas.
 * Re-classifies members with new/updated DM messages using inbound message patterns.
 *
 * Usage: node scripts/sync-ingest.js
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { parse } = require('csv-parse/sync');
const XLSX = require('xlsx');

const { classifyFromInboundMessages } = require('./lib/classify');
const { loadProgress, saveProgress, loadLastImport, saveLastImport, PATHS } = require('./lib/progress');

// --- File detection ---

function findImportFiles(dir) {
  const files = fs.readdirSync(dir)
    .filter(f => f.endsWith('.csv') || f.endsWith('.xlsx'))
    .map(f => ({
      name: f,
      path: path.join(dir, f),
      mtime: fs.statSync(path.join(dir, f)).mtime,
    }))
    .sort((a, b) => b.mtime - a.mtime); // newest first

  let skoolFile = null;
  let inboxFile = null;

  for (const f of files) {
    if (skoolFile && inboxFile) break;

    // Detect by peeking at headers
    try {
      if (f.name.endsWith('.xlsx')) {
        const wb = XLSX.readFile(f.path);
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });
        const headers = (rows[0] || []).map(h => String(h).trim());
        if (headers.includes('Chat') && headers.includes('Sender') && headers.includes('Message')) {
          if (!inboxFile) inboxFile = f;
        } else if (headers.includes('ID') && headers.includes('First Name')) {
          if (!skoolFile) skoolFile = f;
        }
      } else {
        const head = fs.readFileSync(f.path, 'utf8').substring(0, 2000);
        const firstLine = head.split('\n')[0];
        if (firstLine.includes('Chat') && firstLine.includes('Sender') && firstLine.includes('Message')) {
          if (!inboxFile) inboxFile = f;
        } else if (firstLine.includes('ID') && firstLine.includes('First Name')) {
          if (!skoolFile) skoolFile = f;
        }
      }
    } catch (err) {
      console.log(`  Warning: couldn't read ${f.name}: ${err.message.substring(0, 80)}`);
    }
  }

  return { skoolFile, inboxFile };
}

// --- Parse Inbox Insights (CSV or XLSX) ---

function parseInboxInsights(file) {
  let rows;
  if (file.name.endsWith('.xlsx')) {
    const wb = XLSX.readFile(file.path);
    const ws = wb.Sheets[wb.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(ws);
  } else {
    const raw = fs.readFileSync(file.path, 'utf8');
    rows = parse(raw, { columns: true, skip_empty_lines: true, relax_column_count: true });
  }

  // Group by Chat name, filter to member messages only (exclude "You")
  const dmByName = {};
  for (const row of rows) {
    const chatName = (row.Chat || '').trim();
    const sender = (row.Sender || '').trim();
    const message = (row.Message || '').substring(0, 2000);
    const sentAt = row['Sent At (ISO)'] || '';

    if (!chatName || !message) continue;

    if (!dmByName[chatName]) dmByName[chatName] = [];
    dmByName[chatName].push({ sender, message, sentAt });
  }

  return dmByName;
}

// --- Parse Skool member export CSV ---

function parseSkoolExport(file) {
  const raw = fs.readFileSync(file.path, 'utf8');
  const rows = parse(raw, { columns: true, skip_empty_lines: true, relax_column_count: true });

  return rows.map(m => ({
    id: m.ID,
    first_name: m['First Name'] || '',
    last_name: m['Last Name'] || '',
    full_name: `${m['First Name'] || ''} ${m['Last Name'] || ''}`.trim(),
    username: m.Name || '',
    email: m.Email || '',
    bio: m.Bio || '',
    location: m['Request Location'] || m.Location || '',
    survey_q1: m['Survey Question 1'] || '',
    survey_a1: m['Survey Answer 1'] || '',
    survey_q2: m['Survey Question 2'] || '',
    survey_a2: m['Survey Answer 2'] || '',
    survey_q3: m['Survey Question 3'] || '',
    survey_a3: m['Survey Answer 3'] || '',
    skool_url: m['Skool Profile URL'] || '',
    website: m['Website Link'] || '',
  }));
}

// --- Hash a member's profile for change detection ---

function hashMember(member) {
  const data = `${member.bio}|${member.survey_a1}|${member.survey_a2}|${member.survey_a3}|${member.email}|${member.location}`;
  return crypto.createHash('md5').update(data).digest('hex');
}

// --- Main ---

function main() {
  console.log('=== Sync Ingestion ===\n');

  // 1. Find import files
  const { skoolFile, inboxFile } = findImportFiles(PATHS.importsDir);

  if (!skoolFile && !inboxFile) {
    console.log('No import files found in data/imports/. Drop a Skool export CSV and/or Inbox Insights CSV/XLSX.');
    return { newMembers: 0, updatedProfiles: 0, newDMs: 0, reclassified: 0 };
  }

  console.log(`Skool export: ${skoolFile ? skoolFile.name : 'NOT FOUND'}`);
  console.log(`Inbox Insights: ${inboxFile ? inboxFile.name : 'NOT FOUND'}`);
  console.log('');

  // 2. Load previous state
  const lastImport = loadLastImport();
  const progress = loadProgress();

  let stats = { newMembers: 0, updatedProfiles: 0, newDMs: 0, reclassified: 0 };

  // 3. Process Skool member export (if present)
  if (skoolFile) {
    console.log('--- Processing Skool member export ---');
    const members = parseSkoolExport(skoolFile);
    console.log(`  Loaded ${members.length} members`);

    for (const member of members) {
      const hash = hashMember(member);
      const prev = lastImport.members[member.id];

      if (!prev) {
        // New member
        stats.newMembers++;
        lastImport.members[member.id] = { hash, full_name: member.full_name };

        // Add to progress if not already there
        if (!progress.processed[member.id]) {
          progress.processed[member.id] = {
            ...member,
            classification: 'other',
            confidence: '0.5',
            reasoning: 'New member - pending classification',
            dm_message_count: 0,
            ghl_contact_found: false,
            ghl_contact_id: '',
            ghl_conversation_summary: '',
            updated_category: 'other',
            category_changed: false,
            change_reasoning: '',
          };
        }
      } else if (prev.hash !== hash) {
        // Profile changed
        stats.updatedProfiles++;
        lastImport.members[member.id] = { hash, full_name: member.full_name };

        // Update profile fields in progress
        if (progress.processed[member.id]) {
          const existing = progress.processed[member.id];
          progress.processed[member.id] = {
            ...existing,
            bio: member.bio,
            email: member.email,
            location: member.location,
            website: member.website,
          };
        }
      }
    }

    console.log(`  New members: ${stats.newMembers}`);
    console.log(`  Updated profiles: ${stats.updatedProfiles}`);
  }

  // 4. Process Inbox Insights (if present)
  if (inboxFile) {
    console.log('\n--- Processing Inbox Insights ---');
    const dmByName = parseInboxInsights(inboxFile);
    const chatNames = Object.keys(dmByName);
    console.log(`  Loaded ${chatNames.length} conversations`);

    // Build a lookup from full_name to member id
    const nameToId = {};
    for (const [id, data] of Object.entries(progress.processed)) {
      const name = data.full_name;
      if (name) nameToId[name] = id;
    }
    // Also check lastImport for name→id mapping
    for (const [id, data] of Object.entries(lastImport.members)) {
      if (data.full_name) nameToId[data.full_name] = id;
    }

    let matched = 0;
    let reclassified = 0;

    for (const chatName of chatNames) {
      const memberId = nameToId[chatName];
      if (!memberId) continue;

      matched++;
      const allDMs = dmByName[chatName];

      // Filter to member's own messages only (not "You")
      const memberMessages = allDMs
        .filter(d => d.sender !== 'You')
        .map(d => d.message)
        .filter(m => m.trim().length > 0);

      if (memberMessages.length === 0) continue;

      // Check if there are new messages since last sync
      const prevDMCount = lastImport.members[memberId]?.dmCount || 0;
      if (memberMessages.length <= prevDMCount) continue;

      stats.newDMs++;

      // Update DM count in state
      if (!lastImport.members[memberId]) {
        lastImport.members[memberId] = { hash: '', full_name: chatName };
      }
      lastImport.members[memberId].dmCount = memberMessages.length;

      // Re-classify using ALL member messages (Inbox Insights DMs)
      const existing = progress.processed[memberId];
      if (!existing) continue;

      const currentCategory = existing.updated_category || existing.classification || 'other';
      const { category, reasoning } = classifyFromInboundMessages(memberMessages, currentCategory);

      if (category && category !== currentCategory) {
        reclassified++;
        progress.processed[memberId] = {
          ...existing,
          ghl_conversation_summary: memberMessages.join('; ').substring(0, 500),
          updated_category: category,
          category_changed: true,
          change_reasoning: reasoning,
          dm_message_count: memberMessages.length,
        };
        console.log(`  RECLASSIFIED: ${chatName}: ${currentCategory} -> ${category}`);
      } else {
        // Update DM summary even if category didn't change
        progress.processed[memberId] = {
          ...existing,
          ghl_conversation_summary: memberMessages.join('; ').substring(0, 500),
          dm_message_count: memberMessages.length,
        };
      }
    }

    stats.reclassified = reclassified;
    console.log(`  Matched to members: ${matched}`);
    console.log(`  Members with new DMs: ${stats.newDMs}`);
    console.log(`  Reclassified: ${reclassified}`);
  }

  // 5. Save state
  lastImport.lastSyncTimestamp = new Date().toISOString();
  saveLastImport(lastImport);
  saveProgress(progress);

  console.log('\n--- Ingestion complete ---');
  console.log(`  State saved to ${PATHS.lastImport}`);
  console.log(`  Progress saved to ${PATHS.progress}`);

  return stats;
}

if (require.main === module) {
  main();
}

module.exports = { main };
