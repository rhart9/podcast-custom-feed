require('dotenv').config();

let _itemCount, _client;
const _now = new Date();

exports.handler = async (event) => {
  await connect();

  if (event.mode == "analyze-reset") {
    return analyzeReset();
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

  db.collection("rss-entry-analyze-batch").insertOne(batch);
  
  return "Success";
}

function addBatchValues(item) {
  item.count = _itemCount;
  item.download_dt = _now;
  item.download_dt_local = _now.toLocaleString('en-US', { timeZone: 'America/New_York' });
}

async function analyzeReset() {
  let db = _client.db(process.env.MONGO_DB);

  db.collection("rss-entry-analyze").deleteMany({});
  db.collection("rss-entry-analyze-batch").deleteMany({});
}

async function prod(items) {
  return "Not implemented"
}