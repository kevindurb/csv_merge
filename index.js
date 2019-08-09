#!/usr/bin/env node

require('events').EventEmitter.defaultMaxListeners = Infinity;
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const args = process.argv;
const cwd = process.cwd();

const readFirstLine = (file) => (
  new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file);
      stream.pipe(csv.parse())
      .on('error', reject)
      .on('data', (line) => {
        stream.destroy();
        resolve(line);
      });
  })
);

const unique = list => list.reduce((acc, item) => (
  item && !acc.includes(item)
  ? [ ...acc, item ]
  : acc
), []);

const run = async (inputDirectory, outputFile) => {
  if (
    !fs.existsSync(inputDirectory)
    || !fs.statSync(inputDirectory).isDirectory()
  ) {
    console.error('first argument must be input directory');
    process.exit(1);
  }

  const files = fs.readdirSync(inputDirectory)
    .map(name => path.join(inputDirectory, name));

  const headerKeys = unique(
    (await Promise.all(files.map(readFirstLine))).flat()
  );

  const outStream = csv.format({ headers: headerKeys });
  outStream.pipe(fs.createWriteStream(outputFile));

  files.forEach(file =>
    fs.createReadStream(file)
      .pipe(csv.parse({ headers: true }))
      .pipe(outStream)
  );
};


run(path.join(cwd, args[2]), path.join(cwd, args[3]));
