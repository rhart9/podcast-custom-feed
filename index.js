require('dotenv').config();
let { v4: uuidv4 } = require('uuid');

let _itemCount, _client;
const _now = new Date();
const _batchUUID = uuidv4();

exports.handler = async (event) => {
  await connect();

  if (event.mode == "analyze-reset") {
    return analyzeReset();
  }
  else if (event.mode == "prod-reset") {
    return prodReset();
  }

  let items = await fetch();

  if (event.mode == "analyze") {
    return analyze(items);
  }
  else if (event.mode == "prod") {
    return prod(items)
  }

  return event;
};

async function connect() {
  const {MongoClient} = require('mongodb');
  let mongoUri = process.env.MONGO_URI;
  _client = new MongoClient(mongoUri);
  
  await _client.connect();
}

async function fetch() {
  let promise = new Promise(function(resolve, reject) {
    let https = require('https');

    let options = new URL(process.env.PODCAST_URL);
    
    let callback = function(res) {
      let responseData = '';

      res.on('data', function (chunk) {
        responseData += chunk;
      });

      res.on('end', async function () {
        let convert = require('xml-js');
        let result = convert.xml2js(responseData, {compact: true, spaces: 0});
        
        let items = result.rss.channel.item;
        _itemCount = items.length;

        resolve(items);
      });
    }
    
    https.get(options, callback)
        .on('error', function(e) {
            reject(Error(e));
        })
        .end();
  });

  return promise;
}

async function analyze(items) {
  let db = _client.db(process.env.MONGO_DB);

  items.forEach((item, index) => {
    item.index = index;
    if (item.description && item.description._text) {
      item.text = item.description._text.replace(/(\n|&nbsp;|<([^>]+)>)/ig, '');
    }
    addBatchValues(item);
  });
  
  let entries = db.collection("rss-entry-analyze");
  await entries.insertMany(items);

  let batch = {}
  addBatchValues(batch);

  await db.collection("rss-entry-analyze-batch").insertOne(batch);
  
  return "Success";
}

function addBatchValues(item) {
  item.batch_uuid = _batchUUID;
  item.count = _itemCount;
  item.download_dt = _now;
  item.download_dt_local = _now.toLocaleString('en-US', { timeZone: 'America/New_York' });
}

async function analyzeReset() {
  let db = _client.db(process.env.MONGO_DB);

  await db.collection("rss-entry-analyze").deleteMany({});
  await db.collection("rss-entry-analyze-batch").deleteMany({});
}

async function prod(items) {
  let db = _client.db(process.env.MONGO_DB);
  let rssEntries = db.collection("rss-entry");

  await Promise.all(items.map((item, index) => new Promise(async (resolve, reject) => {
    let query = { "guid._text": item.guid._text };
    let dbItem = await rssEntries.findOne(query);

    if (!dbItem) {
      dbItem = item;
      dbItem.created_dt = _now;
      rssEntries.insertOne(dbItem);
    }
    else {
      let write_log = false;
      let log = {}

      if (dbItem.title._text != item.title._text) {
        write_log = true;
        log.old_title = dbItem.title._text;
        log.new_title = item.title._text;
        dbItem.title._text = item.title._text;
      }
      if (dbItem.description._text != item.description._text) {
        write_log = true;
        log.old_description = dbItem.description._text;
        log.new_description = item.description._text;
        dbItem.description._text = item.description._text;
      }
      if (write_log) {
        log.updated_dt = _now;
        if (!dbItem.changelog) {
          dbItem.changelog = [];
        }
        dbItem.changelog.push(log);
      }
    }

    dbItem.batch_uuid = _batchUUID;
    dbItem.updated_dt = _now;
    await rssEntries.replaceOne(query, dbItem);

    resolve();
  })));

  let batch = {}
  addBatchValues(batch);

  await db.collection("rss-entry-batch").insertOne(batch);
}

async function prodReset() {
  let db = _client.db(process.env.MONGO_DB);

  await db.collection("rss-entry").deleteMany({});
  await db.collection("rss-entry-batch").deleteMany({});
}