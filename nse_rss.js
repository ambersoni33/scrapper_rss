
import fs from "fs";
import csv from "csv-parser";
import RSSParser from "rss-parser";
import pLimit from "p-limit";
import { Client } from "pg";
import dotenv from "dotenv";
dotenv.config();

/* ---------- CONFIG ---------- */
const parser = new RSSParser();
const DB = new Client({
   user: process.env.USER,
  password: process.env.PASSWORD,
  host: process.env.HOST,
  port: process.env.PORT,
  database:process.env.DATABASE,
});

const CSV = "./nse.csv";
const EXCHANGE = "NSE";
const CONCURRENCY = 6;
const MAX_SAVE = 8;
const MAX_PER_QUERY = 10;
const MIN_RESULTS = 3;
const LOCALE = { hl: "en-IN", gl: "IN", ceid: "IN:en" };

/* ---------- READ SYMBOLS FROM CSV ---------- */
function readCsv(path) {
  return new Promise((resolve, reject) => {
    const out = [];
    fs.createReadStream(path)
      .pipe(csv())
      .on("data", (row) => {
        const symbol = (row["SYMBOL"] || "").toString().trim();
        const name =
          (row["NAME OF COMPANY"] || row["NAME"] || "").toString().trim();
        if (symbol) out.push({ symbol, name: name || symbol });
      })
      .on("end", () => resolve(out))
      .on("error", reject);
  });
}

/* ---------- BUILD GOOGLE NEWS RSS URL ---------- */
function googleRssUrl(q) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(
    q
  )}&hl=${LOCALE.hl}&gl=${LOCALE.gl}&ceid=${LOCALE.ceid}`;
}

/* ---------- STRONGER QUERY BUILDER ---------- */
function buildQueries(symbol, name) {
  const sym = symbol.replace(/[^A-Za-z0-9_.-]/g, "");

  return [
    `"${name}" stock price`,
    `"${name}" share news"`,
    `"${sym}" NSE"`,
    `"${name}" results"`,
    `"${name}" earnings"`,
    `"${name}" business news"`,
    `"${sym}" stock"`
  ];
}

/* ---------- FILTER, SCORE, CLEAN ---------- */
function cleanAndScore(items, symbol, name) {
  const seen = new Set();
  const results = [];

  const up = (s) => (s || "").toString().toUpperCase();

  const symU = up(symbol);
  const nameU = up(name);
  const nameParts = nameU.split(" ").filter((x) => x.length > 3);
  const now = Date.now();

  for (const it of items) {
    if (!it || !it.link || !it.title) continue;

    const url = it.link;

    // FIX: allow Google article links but skip search/redirect
    if (url.includes("/search?")) continue;

    if (seen.has(url)) continue;
    seen.add(url);

    const headline = it.title;
    const snippet = (it.contentSnippet || it.content || "").toString();
    const headU = up(headline + " " + snippet);

    // FIX: allow 120-day window
    const publishedAt = it.pubDate ? new Date(it.pubDate) : null;
    if (
      publishedAt &&
      now - publishedAt.getTime() > 1000 * 60 * 60 * 24 * 120
    )
      continue;

    /* ---- SCORING ---- */
    let score = 0;

    // Strong matches
    if (headU.includes(symU)) score += 6;
    if (headU.includes(nameU)) score += 8;

    // Partial name matches
    for (const part of nameParts) {
      if (headU.includes(part)) score += 3;
    }

    // Stock keywords
    if (headU.includes("STOCK")) score += 5;
    if (headU.includes("SHARE")) score += 4;
    if (headU.includes("MARKET")) score += 3;
    if (headU.includes(EXCHANGE)) score += 3;

    // Business/earnings keywords
    if (
      headU.includes("EARNINGS") ||
      headU.includes("RESULT") ||
      headU.includes("Q1") ||
      headU.includes("Q2") ||
      headU.includes("Q3") ||
      headU.includes("Q4") ||
      headU.includes("REVENUE") ||
      headU.includes("PROFIT")
    )
      score += 5;

    results.push({
      headline,
      url,
      source:
        it.source && it.source.title ? it.source.title : it.creator || null,
      published_at: publishedAt ? publishedAt.toISOString() : null,
      score,
    });
  }

  // Sort by score + recency
  results.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const ta = a.published_at ? new Date(a.published_at).getTime() : 0;
    const tb = b.published_at ? new Date(b.published_at).getTime() : 0;
    return tb - ta;
  });

  return results;
}

/* ---------- SAVE ONE NEWS ITEM ---------- */
async function saveToDb(symbol, item) {
  const q = `
    INSERT INTO nse_news (symbol, headline, url, source, published_at)
    VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (url) DO NOTHING;
  `;
  await DB.query(q, [
    symbol,
    item.headline,
    item.url,
    item.source,
    item.published_at,
  ]);
}

/* ---------- FETCH NEWS FOR ONE COMPANY ---------- */
async function fetchForSymbol(row) {
  const { symbol, name } = row;
  const queries = buildQueries(symbol, name);
  let collected = [];

  for (const q of queries) {
    try {
      const url = googleRssUrl(q);
      const feed = await parser.parseURL(url);
      if (feed?.items?.length) {
        collected.push(...feed.items.slice(0, MAX_PER_QUERY));
      }
    } catch (e) {
      console.warn(`RSS error for ${symbol}: ${e.message}`);
    }

    if (collected.length >= MIN_RESULTS) break;

    await new Promise((r) =>
      setTimeout(r, 150 + Math.random() * 300)
    );
  }

  const cleaned = cleanAndScore(collected, symbol, name);
  const toSave = cleaned.slice(0, MAX_SAVE);

  for (const it of toSave) {
    try {
      await saveToDb(symbol, it);
    } catch (e) {
      console.warn(`DB save failed for ${symbol}: ${e.message}`);
    }
  }

  console.log(`NSE ${symbol} → saved ${toSave.length}`);
}

/* ---------- MAIN ---------- */
async function main() {
  await DB.connect();

  const list = await readCsv(CSV);
  const limit = pLimit(CONCURRENCY);

  await Promise.all(
    list.map((r) =>
      limit(async () => {
        await fetchForSymbol(r);
        await new Promise((res) =>
          setTimeout(res, 100 + Math.random() * 200)
        );
      })
    )
  );

  await DB.end();
  console.log("NSE RSS scraping complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
