const fs = require("node:fs/promises");
const { pipeline } = require("node:stream/promises");
const csv = require("csv-parser");
const { Transform } = require("node:stream");

//custom transform to filter and enrich csv data
class EnrichCSV extends Transform {
  constructor() {
    super({ objectMode: true });
  }
  _transform(record, encoding, callback) {
    const age = parseInt(record.age, 10);
    if (age < 18) {
      const enrichRecord = { ...record, timestamp: new Date().toISOString() };
      this.push(enrichRecord);
    }
    callback();
  }
}

//Function to extract CSV
async function extractCSV() {
  console.log("extractCSV...");
  const fileStream = await fs.open("data/input.csv", "r");
  const readStream = fileStream.createReadStream();
  return readStream.pipe(csv({ columns: true }));
}
//Function to process logs
async function processLogs() {
  console.log("processLogs");
}
//Simulate API data stream
async function apiDataStream() {
  console.log("apiDataStream");
}
//ETL pipeline
async function runETL() {
  console.log("Running ETL...");
  const csvStream = await extractCSV();
  const apiStream = await apiDataStream();

  //setup the pipeline
  await pipeline(csvStream, new EnrichCSV(), async (source) => {
    console.log("Pipeline...");
    const destFile = await fs.open("data/output.json", "w");
    const writableStream = destFile.createWriteStream();
    for await (const record of source) {
      writableStream.write(`${JSON.stringify(record)}`);
    }
  });
}

runETL().catch((err) => {
  console.log("ETL failed", err);
});
