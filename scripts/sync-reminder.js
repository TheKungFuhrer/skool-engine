/**
 * Sync Reminder — Email notification when CSV import is stale.
 *
 * Checks how many days since the last CSV import and sends a reminder
 * email if it's been too long (default: 7 days).
 *
 * Usage: node scripts/sync-reminder.js
 *
 * .env config (all optional):
 *   SYNC_REMINDER_DAYS=7           (default: 7)
 *   GHL_OWNER_EMAIL=you@email.com  (required for email)
 *   SMTP_HOST=smtp.gmail.com       (required for email)
 *   SMTP_PORT=587                  (default: 587)
 *   SMTP_USER=you@gmail.com        (required for email)
 *   SMTP_PASS=app-password         (required for email)
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const { loadProgress, loadLastImport, loadJSON, saveJSON, PATHS } = require('./lib/progress');

const REMINDER_DAYS = parseInt(process.env.SYNC_REMINDER_DAYS) || 7;
const OWNER_EMAIL = process.env.GHL_OWNER_EMAIL;
const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = parseInt(process.env.SMTP_PORT) || 587;
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;

function getDaysSinceImport() {
  const lastImport = loadLastImport();
  if (!lastImport.lastSyncTimestamp) return Infinity;
  const lastDate = new Date(lastImport.lastSyncTimestamp);
  const now = new Date();
  return Math.floor((now - lastDate) / (1000 * 60 * 60 * 24));
}

function buildReminderBody(daysSince) {
  const progress = loadProgress();
  const members = Object.values(progress.processed);
  const total = members.length;

  const categories = {};
  members.forEach(m => {
    const cat = m.updated_category || m.classification || 'unknown';
    categories[cat] = (categories[cat] || 0) + 1;
  });

  const breakdown = Object.entries(categories)
    .sort((a, b) => b[1] - a[1])
    .map(([cat, count]) => `  ${cat}: ${count}`)
    .join('\n');

  const daysText = daysSince === Infinity
    ? 'No import has ever been run'
    : `It has been ${daysSince} days since your last Skool data import`;

  return `Hi Dylan,

${daysText}.

Current member stats (${total} total):
${breakdown}

To update your data:
1. Export your Skool member CSV from the community admin panel
2. Run Inbox Insights bookmarklet to export the latest DMs
3. Drop both files into the data/imports/ folder
4. Run: npm run sync

This will detect new members, classify them, and update GHL contact notes automatically.

— Skool Engine Sync Reminder`;
}

async function sendEmail(subject, body) {
  const nodemailer = require('nodemailer');
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_PORT === 465,
    auth: { user: SMTP_USER, pass: SMTP_PASS },
  });

  await transporter.sendMail({
    from: SMTP_USER,
    to: OWNER_EMAIL,
    subject,
    text: body,
  });
}

async function main() {
  console.log('=== Sync Reminder Check ===\n');

  const daysSince = getDaysSinceImport();
  const daysText = daysSince === Infinity ? 'never' : `${daysSince} days ago`;
  console.log(`Last import: ${daysText}`);
  console.log(`Reminder threshold: ${REMINDER_DAYS} days\n`);

  if (daysSince < REMINDER_DAYS) {
    console.log(`No reminder needed — last import was ${daysSince} day(s) ago.`);
    return;
  }

  // Check if we already sent a reminder recently (within 24h)
  const reminderState = loadJSON(PATHS.lastReminder) || {};
  if (reminderState.lastReminderSent) {
    const lastSent = new Date(reminderState.lastReminderSent);
    const hoursSince = (Date.now() - lastSent.getTime()) / (1000 * 60 * 60);
    if (hoursSince < 24) {
      console.log(`Reminder already sent ${Math.floor(hoursSince)} hours ago — skipping to avoid spam.`);
      return;
    }
  }

  // Build reminder
  const subject = `Skool Sync Reminder — ${daysSince === Infinity ? 'never imported' : daysSince + ' days since last import'}`;
  const body = buildReminderBody(daysSince);

  // Send or print
  const smtpConfigured = SMTP_HOST && SMTP_USER && SMTP_PASS && OWNER_EMAIL;

  if (smtpConfigured) {
    console.log(`Sending reminder email to ${OWNER_EMAIL}...`);
    try {
      await sendEmail(subject, body);
      console.log('Email sent successfully.');
    } catch (err) {
      console.log(`Email failed: ${err.message}`);
      console.log('Falling back to console output:\n');
      console.log(`Subject: ${subject}\n`);
      console.log(body);
    }
  } else {
    console.log('SMTP not configured — printing reminder to console:\n');
    console.log(`Subject: ${subject}\n`);
    console.log(body);
  }

  // Record that we sent a reminder
  saveJSON(PATHS.lastReminder, { lastReminderSent: new Date().toISOString() });
  console.log('\nReminder state saved.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
