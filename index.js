/**
 * Get values from .env
 */
require("dotenv").config();

/**
 * Imports
 */
const sqlite3 = require("sqlite3").verbose();
const firebase = require("firebase/app");
const firestore = require("firebase/firestore");
const geofire = require("geofire-common");
const os = require("os");
const fs = require("fs");
const stream = require("stream");
const { promisify } = require("util");
/**
 * Firebase Config
 */
const { FIREBASE_API_KEY, FIREBASE_AUTH_DOMAIN, FIREBASE_PROJECT_ID } =
  process.env;

/**
 * Path to hitchwiki dump url
 */
const DUMP_URL = "https://hitchmap.com/dump.sqlite";

/**
 * Path to hitchwiki dump local file
 */
const SQLITE_FILE = os.tmpdir() + "/hitchwiki.sqlite";

/**
 * Max number of operations before ending process (stay within daily free tier)
 */
const MAX_WRITES = 10000;

/**
 * Download dump
 */
const downloadDump = async () => {
  console.log("Downloading file from", DUMP_URL, "to", SQLITE_FILE);

  const pipeline = promisify(stream.pipeline);
  const { default: got } = await import("got"); // got is esm only
  await pipeline(got.stream(DUMP_URL), fs.createWriteStream(SQLITE_FILE));
};

/**
 * Connect to the SQLite db
 */
const getSrcDb = () => {
  console.log("Connecting to sqlite");
  const srcDb = new sqlite3.Database(SQLITE_FILE);
  return srcDb;
};

/**
 * Connect to firestore
 */
const getDstDb = () => {
  console.log("Connecting to firebase");

  const firebaseConfig = {
    apiKey: FIREBASE_API_KEY,
    authDomain: FIREBASE_AUTH_DOMAIN,
    projectId: FIREBASE_PROJECT_ID,
  };

  const app = firebase.initializeApp(firebaseConfig);
  const targetDb = firestore.getFirestore(app);

  return targetDb;
};

/**
 * Fetch all ingestible rows from hitchwiki dump
 */
const getSrcRows = (db, cutoffTimestamp) => {
  console.log(`Getting rows after ${new Date(cutoffTimestamp * 1000)}`);

  const cutoffDate = `DATETIME(${cutoffTimestamp}, 'unixepoch')`;

  let sql = `
    SELECT id, lat, lon, rating, name, datetime 
    FROM points 
    WHERE 
        banned = 0 AND 
        reviewed = 1 AND 
        rating > 2 AND
        datetime > ${cutoffDate}
    ORDER BY datetime
  `;

  const promise = new Promise((resolve, reject) =>
    db.all(sql, [], (err, rows) => (err ? reject(err) : resolve(rows)))
  );

  return promise;
};

/**
 * Get the most recently ingested row
 */
const getLastIngestedRow = async (db) => {
  console.log("Getting last ingested row");

  const { getDocs, query, collection, where, orderBy, limit } = firestore;

  const snapshot = await getDocs(
    query(
      collection(db, "hitching-spots"),
      where("source.type", "==", "hitchwiki"),
      orderBy("submittedTimestamp", "desc"),
      limit(1)
    )
  );

  return snapshot.docs[0]?.data();
};

/**
 * Given a row from the dump, return a doc that's ready to be added to firestore
 */
const formatRow = ({ id, lat, lon, rating, name, datetime }) => {
  const { GeoPoint, Timestamp } = firestore;

  const dstRow = {
    /**
     * The coords of the spot
     */
    coords: new GeoPoint(lat, lon),
    /**
     * The geohash of the coords
     * @see https://firebase.google.com/docs/firestore/solutions/geoqueries
     */
    geohash: geofire.geohashForLocation([lat, lon]),
    /**
     * Our system rates 1-3 rather than 1-5
     */
    rating: rating - 2,
    /**
     * Author
     */
    user: {
      type: "guest",
      name: (name || "").replace("(Hitchwiki)", "").trim(),
    },
    /**
     * Time originally created
     */
    submittedTimestamp: Timestamp.fromDate(new Date(datetime)),
    /**
     * Keep a reference to source data
     */
    source: {
      type: "hitchwiki",
      id: id,
      ingestedTimestamp: Timestamp.now(),
    },
  };

  return dstRow;
};

/**
 * Add rows to firebase
 */
const insertRows = async (db, rows) => {
  console.log(`Inserting ${rows.length} rows`);

  const { setDoc, doc } = firestore;

  let done = 0;
  const total = rows.length;

  for (const row of rows) {
    await setDoc(
      doc(db, "hitching-spots", `${row.source.type}-${row.source.id}`),
      row
    );
    console.log(
      `${++done}/${total}: ${row.source.id} ${row.submittedTimestamp.toDate()}`
    );

    if (done >= MAX_WRITES) {
      console.info("Reached quota: continue process tomorrow to avoid cost");
    }
  }
};

/**
 * Ingestion logic
 */
const ingest = async () => {
  console.log("Running");

  await downloadDump();

  const srcDb = getSrcDb();
  const dstDb = getDstDb();

  let cutoff = 0;
  const lastIngestedRow = await getLastIngestedRow(dstDb);
  if (lastIngestedRow) {
    cutoff = lastIngestedRow.submittedTimestamp.seconds;
  }

  const srcRows = await getSrcRows(srcDb, cutoff);
  const dstRows = srcRows.map(formatRow);

  try {
    await insertRows(dstDb, dstRows);
    console.info(`Completed without error`);
  } catch (err) {
    console.error("Failed:", err);
  }
};

/**
 * Run script
 */
ingest();
