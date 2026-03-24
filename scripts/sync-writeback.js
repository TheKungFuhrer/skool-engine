/**
 * Sync Writeback — Master Note on GHL Contacts
 *
 * Creates or updates a single "master note" on each GHL contact consolidating
 * all Skool community activity (classification, profile, DM excerpts).
 *
 * The note is identified by the header line:
 *   --- SKOOL COMMUNITY ACTIVITY — OMG RENTALS ---
 *
 * Usage:
 *   node scripts/sync-writeback.js              # full run
 *   node scripts/sync-writeback.js --limit 10   # test run (first 10)
 *   node scripts/sync-writeback.js --resume     # resume interrupted run
 */

const crypto = require('crypto');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const { getContactNotes, createContactNote, updateContactNote, addContactTag, sleep, GHL_API_KEY } = require('./lib/ghl-api');
const { loadProgress, loadWritebackLog, saveWritebackLog } = require('./lib/progress');

let identityAvailable = false;
try {
  var identityAdapter = require('./lib/identity');
  identityAvailable = true;
} catch {
  // Identity layer not installed — skip tagging
}

const NOTE_HEADER = '--- SKOOL COMMUNITY ACTIVITY — OMG RENTALS ---';
const BATCH_SIZE = 50;
const BATCH_PAUSE_MS = 5000;

// --- Build note content ---

function buildNoteBody(member) {
  const category = member.updated_category || member.classification || 'other';
  const conf = parseFloat(member.confidence) || 0;
  const confidenceLabel = conf >= 0.8 ? 'high' : conf >= 0.5 ? 'medium' : 'low';
  const now = new Date().toISOString();

  const surveyLines = [];
  if (member.survey_a1) surveyLines.push(`  Q: ${member.survey_q1 || 'Survey 1'}\n  A: ${member.survey_a1}`);
  if (member.survey_a2) surveyLines.push(`  Q: ${member.survey_q2 || 'Survey 2'}\n  A: ${member.survey_a2}`);
  if (member.survey_a3) surveyLines.push(`  Q: ${member.survey_q3 || 'Survey 3'}\n  A: ${member.survey_a3}`);

  const bio = (member.bio || '').substring(0, 500);
  const email = member.email || '(none)';
  const dmCount = member.dm_message_count || 0;

  const classificationSignals = member.change_reasoning
    ? member.change_reasoning
    : 'No reclassification signals detected';

  const originalReasoning = member.reasoning
    ? member.reasoning.substring(0, 500)
    : '(none)';

  const conversationSummary = member.ghl_conversation_summary || '';
  let dmExcerpts = '';
  if (conversationSummary && conversationSummary !== 'No inbound messages' && conversationSummary !== 'Not found in GHL' && !conversationSummary.startsWith('Error:')) {
    const msgs = conversationSummary.split('; ').slice(-5);
    dmExcerpts = msgs.map(m => `- "${m.substring(0, 200)}"`).join('\n');
  }

  let body = `${NOTE_HEADER}
Last synced: ${now}

CLASSIFICATION: ${category}
Confidence: ${confidenceLabel}

PROFILE SUMMARY:
- Name: ${member.full_name || '(unknown)'}
- Email: ${email}
- Bio: ${bio || '(none)'}`;

  if (surveyLines.length > 0) {
    body += `\n- Survey answers:\n${surveyLines.join('\n')}`;
  }

  body += `

DM ACTIVITY:
- Messages from them: ${dmCount}

KEY MESSAGES THAT DROVE CLASSIFICATION:
${classificationSignals}

CLASSIFICATION REASONING:
${originalReasoning}`;

  if (dmExcerpts) {
    body += `

RECENT DM EXCERPTS (from member):
${dmExcerpts}`;
  }

  return body;
}

function contentHash(body) {
  // Hash everything except the "Last synced" line so re-syncing with same data doesn't trigger an update
  const stable = body.replace(/Last synced: .+/, 'Last synced: STABLE');
  return crypto.createHash('md5').update(stable).digest('hex');
}

// --- Main ---

async function main() {
  const args = process.argv.slice(2);
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) || 10 : Infinity;
  const resume = args.includes('--resume');

  console.log('=== Sync Writeback — Master Notes ===\n');

  if (!GHL_API_KEY) {
    console.error('ERROR: GHL_API_KEY not found in .env');
    process.exit(1);
  }

  if (limit < Infinity) {
    console.log(`TEST MODE: Processing only ${limit} contacts\n`);
  }
  if (resume) {
    console.log('RESUME MODE: Skipping contacts already in writeback log\n');
  }

  const progress = loadProgress();
  const writebackLog = loadWritebackLog();
  const members = Object.values(progress.processed);

  // Filter to contacts with GHL IDs
  const withGHL = members.filter(m => m.ghl_contact_id);
  console.log(`Total members: ${members.length}`);
  console.log(`With GHL contact ID: ${withGHL.length}`);

  // Determine which contacts need processing
  const toProcess = [];
  for (const member of withGHL) {
    const contactId = member.ghl_contact_id;
    const noteBody = buildNoteBody(member);
    const hash = contentHash(noteBody);

    const existing = writebackLog.contacts[contactId];

    // Skip if content hash matches (already up to date)
    if (existing && existing.contentHash === hash) continue;

    // In resume mode, skip contacts that were already written (even if hash differs, they were handled)
    if (resume && existing && existing.noteId) continue;

    toProcess.push({ member, contactId, noteBody, hash });
  }

  console.log(`Contacts needing writeback: ${toProcess.length}\n`);

  if (toProcess.length === 0) {
    console.log('All notes are up to date. Nothing to write.');
    return { created: 0, updated: 0, skipped: 0, errors: 0 };
  }

  // Apply limit
  const batch = toProcess.slice(0, limit);
  if (limit < Infinity) {
    console.log(`Processing ${batch.length} of ${toProcess.length} contacts\n`);
  }

  let created = 0, updated = 0, skipped = 0, errors = 0;

  for (let i = 0; i < batch.length; i++) {
    const { member, contactId, noteBody, hash } = batch[i];
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(batch.length / BATCH_SIZE);

    // Batch pause (every BATCH_SIZE, except the first)
    if (i > 0 && i % BATCH_SIZE === 0) {
      console.log(`\n  Batch ${batchNum - 1}/${totalBatches} complete — ${created + updated} notes written`);
      console.log(`  Pausing ${BATCH_PAUSE_MS / 1000}s before next batch...`);
      await sleep(BATCH_PAUSE_MS);
    }

    process.stdout.write(`  [${i + 1}/${batch.length}] ${member.full_name}... `);

    try {
      // Fetch existing notes to find our managed note
      const notes = await getContactNotes(contactId);
      const existingNote = notes.find(n =>
        (n.body && n.body.startsWith(NOTE_HEADER)) ||
        (n.bodyText && n.bodyText.startsWith(NOTE_HEADER))
      );

      let action;
      let noteId;

      if (existingNote) {
        // Update existing note
        await updateContactNote(contactId, existingNote.id, noteBody);
        action = 'updated';
        noteId = existingNote.id;
        updated++;
      } else {
        // Create new note
        const result = await createContactNote(contactId, noteBody);
        action = 'created';
        noteId = result.note?.id || 'unknown';
        created++;
      }

      writebackLog.contacts[contactId] = {
        noteId,
        contentHash: hash,
        action,
        fullName: member.full_name,
        category: member.updated_category || member.classification,
        writtenAt: new Date().toISOString(),
      };

      console.log(`${action.toUpperCase()} (noteId: ${noteId})`);

      // Preview note in test mode
      if (limit <= 20) {
        console.log('    --- NOTE PREVIEW ---');
        noteBody.split('\n').forEach(line => console.log('    ' + line));
        console.log('    --- END PREVIEW ---\n');
      }
    } catch (err) {
      errors++;
      console.log(`ERROR: ${err.message.substring(0, 100)}`);
    }

    // Save log periodically
    if ((i + 1) % BATCH_SIZE === 0 || i === batch.length - 1) {
      saveWritebackLog(writebackLog);
    }
  }

  // Final save
  saveWritebackLog(writebackLog);

  // --- Identity layer: tag overlaps in GHL ---
  if (identityAvailable) {
    try {
      identityAdapter.openDb();
      const untagged = identityAdapter.getUntaggedOverlaps();
      if (untagged.length > 0) {
        console.log(`\n--- Identity Layer: Tagging ${untagged.length} overlaps in GHL ---`);
        let tagged = 0;
        for (const overlap of untagged) {
          try {
            await addContactTag(overlap.ghl_contact_id, "cold_outreach_overlap");
            identityAdapter.markTagged(overlap.email);
            tagged++;
            console.log(`  Tagged: ${overlap.email}`);
          } catch (err) {
            console.log(`  Failed to tag ${overlap.email}: ${err.message.substring(0, 80)}`);
          }
        }
        console.log(`  Tagged ${tagged}/${untagged.length} overlaps`);
      }
      identityAdapter.closeDb();
    } catch (err) {
      console.log(`Identity layer warning: ${err.message} — continuing without tagging`);
    }
  }

  console.log('\n--- Writeback Summary ---');
  console.log(`  Created: ${created}`);
  console.log(`  Updated: ${updated}`);
  console.log(`  Skipped (up to date): ${toProcess.length - batch.length + skipped}`);
  console.log(`  Errors: ${errors}`);

  return { created, updated, skipped, errors };
}

if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = { main };
