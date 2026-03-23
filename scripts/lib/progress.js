/**
 * Progress/state file helpers for enrichment and sync.
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');

const PATHS = {
  progress: path.join(DATA_DIR, 'enriched', 'progress.json'),
  enrichedCsv: path.join(DATA_DIR, 'enriched', 'enriched_members.csv'),
  classifiedCsv: path.join(DATA_DIR, 'classified', 'classified_members.csv'),
  lastImport: path.join(DATA_DIR, 'sync', 'last-import.json'),
  writebackLog: path.join(DATA_DIR, 'sync', 'writeback-log.json'),
  lastReminder: path.join(DATA_DIR, 'sync', 'last-reminder.json'),
  importsDir: path.join(DATA_DIR, 'imports'),
  syncDir: path.join(DATA_DIR, 'sync'),
};

function loadJSON(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function saveJSON(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

function loadProgress() {
  return loadJSON(PATHS.progress) || { processed: {} };
}

function saveProgress(progress) {
  saveJSON(PATHS.progress, progress);
}

function loadLastImport() {
  return loadJSON(PATHS.lastImport) || { members: {}, lastSyncTimestamp: null };
}

function saveLastImport(state) {
  saveJSON(PATHS.lastImport, state);
}

function loadWritebackLog() {
  return loadJSON(PATHS.writebackLog) || { contacts: {} };
}

function saveWritebackLog(log) {
  saveJSON(PATHS.writebackLog, log);
}

module.exports = {
  PATHS,
  loadJSON,
  saveJSON,
  loadProgress,
  saveProgress,
  loadLastImport,
  saveLastImport,
  loadWritebackLog,
  saveWritebackLog,
};
