#!/usr/bin/env node
require('dotenv').config();
const jobBoards = require('./watchers/job-boards');

async function main() {
  console.log('Running signal scan...');
  const count = await jobBoards.run();
  console.log(`Done. ${count} new signals found.`);
  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
