require('dotenv').config();

exports.handler = async (event) => {
  let items = await fetch();

  if (event.mode == "analyze") {
    return analyze(items);
  }
  else if (event.mode == "prod") {
    return prod(items)
  }

  return event;
};

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
  let now = new Date();

  const {MongoClient} = require('mongodb');
  let mongoUri = process.env.MONGO_URI;
  let client = new MongoClient(mongoUri);
  
  await client.connect();
  
  let db = client.db(process.env.MONGO_DB);

  items.forEach((item, index) => {
    item.index = index;
    if (item.description && item.description._text) {
      item.text = item.description._text.replace(/(\n|&nbsp;|<([^>]+)>)/ig, '');
    }
    item.count = items.length;
    item.download_dt = now;
    item.download_dt_local = now.toLocaleString('en-US', { timeZone: 'America/New_York' });
  });
  
  let entries = db.collection("rss-entry-analyze");
  await entries.insertMany(items);
  
  return "Success";
}

async function prod(items) {
  return "Not implemented"
}