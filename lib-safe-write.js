// Safe atomic write for data/properties.json with backup + sanity check.
// Refuses to overwrite if the new payload is suspiciously smaller than the
// existing one (anti-data-loss guard). Keeps a rolling daily backup.
const fs   = require('fs');
const path = require('path');

function safeWriteProperties(outFile, output, opts) {
  opts = opts || {};
  const minRatio       = typeof opts.minRatio === 'number' ? opts.minRatio : 0.7;  // new must be ≥70% of old
  const force          = !!opts.force;
  const backupDir      = opts.backupDir || path.join(path.dirname(outFile), 'backups');
  const newCount       = (output.properties || []).length;

  // Sanity check vs existing
  if (!force && fs.existsSync(outFile)) {
    try {
      const existing = JSON.parse(fs.readFileSync(outFile, 'utf8'));
      const oldCount = (existing.properties || []).length;
      if (oldCount > 100 && newCount < oldCount * minRatio) {
        const msg = `🛡  REFUSED to overwrite: new=${newCount} < ${Math.round(minRatio*100)}% of old=${oldCount}. ` +
                    `Pass {force:true} to override. Existing file kept intact.`;
        console.error(msg);
        // Still drop a side file so a human can inspect the rejected payload
        try {
          fs.writeFileSync(outFile + '.rejected.json', JSON.stringify(output));
        } catch {}
        throw new Error('safeWriteProperties: refused (size shrink guard)');
      }
    } catch (e) {
      if (e.message && e.message.startsWith('safeWriteProperties:')) throw e;
      console.warn('safeWriteProperties: could not parse existing file:', e.message);
    }
  }

  // Daily backup of current file before overwriting
  if (fs.existsSync(outFile)) {
    try {
      if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });
      const day = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
      const bak = path.join(backupDir, `properties-${day}.json`);
      if (!fs.existsSync(bak)) fs.copyFileSync(outFile, bak);
      // Prune backups older than 14 days
      const cutoff = Date.now() - 14 * 86400 * 1000;
      for (const f of fs.readdirSync(backupDir)) {
        const fp = path.join(backupDir, f);
        try {
          if (fs.statSync(fp).mtimeMs < cutoff) fs.unlinkSync(fp);
        } catch {}
      }
    } catch (e) {
      console.warn('safeWriteProperties: backup failed:', e.message);
    }
  }

  // Atomic write: temp file + rename
  const tmp = outFile + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(output));
  fs.renameSync(tmp, outFile);
  console.log(`💾 Safe-write OK: ${newCount} props → ${path.basename(outFile)}`);
}

module.exports = { safeWriteProperties };
