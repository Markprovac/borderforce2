import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { readFile, writeFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { Unzip, UnzipInflate } from "fflate";

const VERSION = "8.1-github-actions-gtfs";
const MAX_SCHEDULE_DAYS = 180;
const TIMEZONE = "Europe/Paris";

const SNCF_GTFS_URL = process.env.SNCF_GTFS_URL ||
  "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip";
const LIGURIA_DATASET_API =
  "https://dati.regione.liguria.it/api/3/action/package_show?id=ds-637";
const LIGURIA_DATASET_PAGE =
  "https://dati.regione.liguria.it/dataset/ds-637";
const OUTPUT_PATH = process.env.OUTPUT_PATH || "schedule.json";
const PREVIOUS_PATH = process.env.PREVIOUS_SCHEDULE_PATH || "previous-schedule.json";

const STATIONS = {
  "breil": {
    key: "breil",
    stationName: "Breil-sur-Roya",
    uic: "87756833",
    aliases: ["breil-sur-roya", "breil sur roya", "breil"]
  },
  "fontan-saorge": {
    key: "fontan-saorge",
    stationName: "Fontan-Saorge",
    uic: "87756858",
    aliases: ["fontan-saorge", "fontan saorge"]
  },
  "saint-dalmas-de-tende": {
    key: "saint-dalmas-de-tende",
    stationName: "Saint-Dalmas-de-Tende",
    uic: "87756866",
    aliases: ["saint-dalmas-de-tende", "saint dalmas de tende", "st dalmas de tende"]
  },
  "la-brigue": {
    key: "la-brigue",
    stationName: "La Brigue",
    uic: "87756874",
    aliases: ["la brigue", "la-brigue"]
  },
  "tende": {
    key: "tende",
    stationName: "Tende",
    uic: "87756882",
    aliases: ["tende"]
  }
};

function clean(value) {
  return String(value ?? "").trim();
}

function normalizeText(value) {
  return clean(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’']/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalStopName(value) {
  return normalizeText(value)
    .replace(/\b(gare|station|halte)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dateToYmd(date) {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0")
  ].join("-");
}

function compactDateToYmd(value) {
  const text = clean(value);
  if (!/^\d{8}$/.test(text)) return "";
  return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
}

function addDaysYmd(ymd, days) {
  const [year, month, day] = ymd.split("-").map(Number);
  return dateToYmd(new Date(Date.UTC(year, month - 1, day + days)));
}

function compareYmd(a, b) {
  return clean(a).localeCompare(clean(b));
}

function weekdayKey(ymd) {
  const [year, month, day] = ymd.split("-").map(Number);
  return ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"][
    new Date(Date.UTC(year, month - 1, day)).getUTCDay()
  ];
}

function parseGtfsTime(value) {
  const match = clean(value).match(/^(\d{1,3}):([0-5]\d):([0-5]\d)$/);
  if (!match) return null;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
}

function secondsToClock(seconds) {
  const normalized = ((seconds % 86400) + 86400) % 86400;
  const hour = Math.floor(normalized / 3600);
  const minute = Math.floor((normalized % 3600) / 60);
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function zonedDateTimeToEpoch(ymd, hhmm, timeZone = TIMEZONE) {
  const [year, month, day] = ymd.split("-").map(Number);
  const [hour, minute] = hhmm.split(":").map(Number);
  const desiredUtc = Date.UTC(year, month - 1, day, hour, minute, 0);
  let guess = desiredUtc;

  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  });

  for (let i = 0; i < 3; i += 1) {
    const parts = Object.fromEntries(
      formatter.formatToParts(new Date(guess))
        .filter(part => part.type !== "literal")
        .map(part => [part.type, Number(part.value)])
    );
    const representedUtc = Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second
    );
    guess += desiredUtc - representedUtc;
  }
  return guess;
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let quoted = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      if (quoted && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        quoted = !quoted;
      }
    } else if (char === "," && !quoted) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  values.push(current);
  return values;
}

function createCsvConsumer(onRow) {
  let headers = null;
  return line => {
    const cleanLine = line.replace(/\r$/, "");
    if (!cleanLine) return;
    const values = parseCsvLine(cleanLine);
    if (!headers) {
      headers = values.map((value, index) =>
        (index === 0 ? value.replace(/^\uFEFF/, "") : value).trim()
      );
      return;
    }
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? "";
    });
    onRow(row);
  };
}

function basenameLower(path) {
  return clean(path).split("/").pop().toLowerCase();
}

async function downloadFile(url, path) {
  console.log(`Téléchargement : ${url}`);
  const response = await fetch(url, {
    headers: { "Accept": "application/zip, application/octet-stream, */*" },
    redirect: "follow",
    signal: AbortSignal.timeout(180000)
  });
  if (!response.ok || !response.body) {
    throw new Error(`Téléchargement impossible (${response.status}) : ${url}`);
  }
  await pipeline(Readable.fromWeb(response.body), createWriteStream(path));
}

async function streamZipCsv(zipPath, csvHandlers) {
  const expected = new Set(Object.keys(csvHandlers).map(name => name.toLowerCase()));
  const seen = new Set();
  const fileTasks = [];

  const unzip = new Unzip(file => {
    const name = basenameLower(file.name);
    if (!expected.has(name)) return;

    seen.add(name);
    const onLine = csvHandlers[name];
    const decoder = new TextDecoder("utf-8");
    let remainder = "";

    let resolveTask;
    let rejectTask;
    const task = new Promise((resolve, reject) => {
      resolveTask = resolve;
      rejectTask = reject;
    });
    fileTasks.push(task);

    file.ondata = (error, data, final) => {
      if (error) {
        rejectTask(error);
        return;
      }
      remainder += decoder.decode(data, { stream: !final });
      const lines = remainder.split(/\n/);
      remainder = final ? "" : (lines.pop() || "");
      try {
        for (const line of lines) onLine(line);
        if (final && remainder) onLine(remainder);
        if (final) resolveTask();
      } catch (consumerError) {
        rejectTask(consumerError);
      }
    };
    file.start();
  });

  unzip.register(UnzipInflate);
  for await (const chunk of createReadStream(zipPath)) {
    unzip.push(new Uint8Array(chunk), false);
  }
  unzip.push(new Uint8Array(0), true);
  await Promise.resolve();
  await Promise.all(fileTasks);
  return { seen: [...seen] };
}

function stationForStopRow(row) {
  const stopId = clean(row.stop_id);
  const name = canonicalStopName(row.stop_name);
  for (const station of Object.values(STATIONS)) {
    if (stopId.includes(station.uic)) return station;
    if (station.aliases.map(canonicalStopName).includes(name)) return station;
  }
  return null;
}

async function loadTargetStopIds(zipPath) {
  const rows = [];
  await streamZipCsv(zipPath, {
    "stops.txt": createCsvConsumer(row => {
      rows.push({
        id: clean(row.stop_id),
        name: clean(row.stop_name),
        parent: clean(row.parent_station)
      });
    })
  });
  if (!rows.length) throw new Error("Le GTFS ne contient pas de stops.txt exploitable.");

  const idToStationKey = new Map();
  for (const row of rows) {
    const station = stationForStopRow({ stop_id: row.id, stop_name: row.name });
    if (station) idToStationKey.set(row.id, station.key);
  }

  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rows) {
      if (!idToStationKey.has(row.id) && row.parent && idToStationKey.has(row.parent)) {
        idToStationKey.set(row.id, idToStationKey.get(row.parent));
        changed = true;
      }
    }
  }

  if (!idToStationKey.size) {
    throw new Error("Aucune des cinq gares Roya n'a été reconnue dans le GTFS.");
  }
  return idToStationKey;
}

function buildServiceDates(calendarRows, calendarDateRows, horizonStart, horizonEnd) {
  const map = new Map();
  function setFor(serviceId) {
    if (!map.has(serviceId)) map.set(serviceId, new Set());
    return map.get(serviceId);
  }

  for (const row of calendarRows) {
    const serviceId = clean(row.service_id);
    let start = compactDateToYmd(row.start_date);
    let end = compactDateToYmd(row.end_date);
    if (!serviceId || !start || !end) continue;
    if (compareYmd(start, horizonStart) < 0) start = horizonStart;
    if (compareYmd(end, horizonEnd) > 0) end = horizonEnd;
    if (compareYmd(start, end) > 0) continue;

    const dates = setFor(serviceId);
    for (let date = start; compareYmd(date, end) <= 0; date = addDaysYmd(date, 1)) {
      if (clean(row[weekdayKey(date)]) === "1") dates.add(date);
    }
  }

  for (const row of calendarDateRows) {
    const serviceId = clean(row.service_id);
    const date = compactDateToYmd(row.date);
    const exceptionType = clean(row.exception_type);
    if (!serviceId || !date) continue;
    if (compareYmd(date, horizonStart) < 0 || compareYmd(date, horizonEnd) > 0) continue;
    const dates = setFor(serviceId);
    if (exceptionType === "1") dates.add(date);
    if (exceptionType === "2") dates.delete(date);
  }
  return map;
}

async function parseGtfsFeed(zipPath, feedUrl, source, horizonStart, horizonEnd) {
  const stopIdToStationKey = await loadTargetStopIds(zipPath);
  const stopEvents = [];
  const trips = new Map();
  const routes = new Map();
  const agencies = new Map();
  const calendarRows = [];
  const calendarDateRows = [];
  const feedInfoRows = [];

  const handlers = {
    "stop_times.txt": createCsvConsumer(row => {
      const stationKey = stopIdToStationKey.get(clean(row.stop_id));
      if (!stationKey) return;
      const arrivalSeconds = parseGtfsTime(row.arrival_time);
      const departureSeconds = parseGtfsTime(row.departure_time);
      const eventSeconds = arrivalSeconds ?? departureSeconds;
      if (eventSeconds === null) return;
      stopEvents.push({
        stationKey,
        tripId: clean(row.trip_id),
        arrivalSeconds,
        departureSeconds,
        eventSeconds
      });
    }),
    "trips.txt": createCsvConsumer(row => {
      const tripId = clean(row.trip_id);
      if (!tripId) return;
      trips.set(tripId, {
        routeId: clean(row.route_id),
        serviceId: clean(row.service_id),
        headsign: clean(row.trip_headsign),
        shortName: clean(row.trip_short_name)
      });
    }),
    "routes.txt": createCsvConsumer(row => {
      const routeId = clean(row.route_id);
      if (!routeId) return;
      routes.set(routeId, {
        agencyId: clean(row.agency_id),
        shortName: clean(row.route_short_name),
        longName: clean(row.route_long_name)
      });
    }),
    "agency.txt": createCsvConsumer(row => {
      agencies.set(clean(row.agency_id) || "__default__", clean(row.agency_name));
    }),
    "calendar.txt": createCsvConsumer(row => calendarRows.push(row)),
    "calendar_dates.txt": createCsvConsumer(row => calendarDateRows.push(row)),
    "feed_info.txt": createCsvConsumer(row => feedInfoRows.push(row))
  };

  const { seen } = await streamZipCsv(zipPath, handlers);
  if (!seen.includes("stop_times.txt") || !seen.includes("trips.txt")) {
    throw new Error("GTFS incomplet : stop_times.txt ou trips.txt absent.");
  }

  const serviceDates = buildServiceDates(
    calendarRows,
    calendarDateRows,
    horizonStart,
    horizonEnd
  );

  const events = [];
  const dedupe = new Set();
  for (const stopEvent of stopEvents) {
    const trip = trips.get(stopEvent.tripId);
    if (!trip?.serviceId) continue;
    const dates = serviceDates.get(trip.serviceId);
    if (!dates?.size) continue;
    const route = routes.get(trip.routeId) || {};
    const agencyName = agencies.get(route.agencyId) || agencies.get("__default__") ||
      (source === "sncf" ? "SNCF" : "Trenitalia");

    for (const serviceDate of dates) {
      const dayOffset = Math.floor(stopEvent.eventSeconds / 86400);
      const eventDate = addDaysYmd(serviceDate, dayOffset);
      if (compareYmd(eventDate, horizonStart) < 0 || compareYmd(eventDate, horizonEnd) > 0) continue;
      const clock = secondsToClock(stopEvent.eventSeconds);
      const station = STATIONS[stopEvent.stationKey];
      if (!station) continue;
      const id = [source, stopEvent.stationKey, eventDate, clock, stopEvent.tripId].join("|");
      if (dedupe.has(id)) continue;
      dedupe.add(id);

      events.push({
        id,
        source,
        operator: agencyName || (source === "sncf" ? "SNCF" : "Trenitalia"),
        stationKey: station.key,
        stationName: station.stationName,
        date: eventDate,
        time: clock,
        epochMs: zonedDateTimeToEpoch(eventDate, clock),
        tripId: stopEvent.tripId,
        trainNumber: trip.shortName || route.shortName || "",
        routeName: route.longName || route.shortName || "",
        destination: trip.headsign || "",
        arrivalTime: stopEvent.arrivalSeconds === null ? "" : secondsToClock(stopEvent.arrivalSeconds),
        departureTime: stopEvent.departureSeconds === null ? "" : secondsToClock(stopEvent.departureSeconds)
      });
    }
  }

  events.sort((a, b) => a.epochMs - b.epochMs || a.stationKey.localeCompare(b.stationKey));
  const feedInfo = feedInfoRows[0] || {};
  const starts = calendarRows.map(row => compactDateToYmd(row.start_date)).filter(Boolean).sort();
  const ends = calendarRows.map(row => compactDateToYmd(row.end_date)).filter(Boolean).sort();
  return {
    source,
    url: feedUrl,
    refreshedAt: new Date().toISOString(),
    validFrom: compactDateToYmd(feedInfo.feed_start_date) || starts[0] || horizonStart,
    validUntil: compactDateToYmd(feedInfo.feed_end_date) || ends.at(-1) || horizonEnd,
    eventCount: events.length,
    matchedStopCount: stopIdToStationKey.size,
    events
  };
}

async function resolveTrenitaliaUrl() {
  if (clean(process.env.TRENITALIA_GTFS_URL)) return clean(process.env.TRENITALIA_GTFS_URL);
  try {
    const response = await fetch(LIGURIA_DATASET_API, {
      headers: { "Accept": "application/json" },
      signal: AbortSignal.timeout(30000)
    });
    if (response.ok) {
      const data = await response.json();
      const candidates = (data?.result?.resources || [])
        .map(resource => clean(resource.url))
        .filter(url => /gtfs/i.test(url) && /\.zip(?:$|\?)/i.test(url));
      if (candidates.length) return candidates.at(-1);
    }
  } catch (error) {
    console.warn(`Découverte API Ligurie impossible : ${error.message}`);
  }

  const page = await fetch(LIGURIA_DATASET_PAGE, {
    headers: { "Accept": "text/html" },
    signal: AbortSignal.timeout(30000)
  });
  if (!page.ok) throw new Error("Impossible de trouver le GTFS Trenitalia Ligurie.");
  const html = await page.text();
  const matches = [...html.matchAll(/https:\/\/[^\"' <]+GTFS[^\"' <]+\.zip/gi)]
    .map(match => match[0].replace(/&amp;/g, "&"));
  if (!matches.length) throw new Error("Aucune ressource ZIP GTFS Trenitalia trouvée.");
  return matches.at(-1);
}

function mergeEvents(...eventLists) {
  const byKey = new Map();
  for (const event of eventLists.flat()) {
    const key = [
      event.stationKey,
      event.date,
      event.time,
      normalizeText(event.trainNumber),
      normalizeText(event.destination)
    ].join("|");
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, event);
      continue;
    }
    const existingScore = [existing.trainNumber, existing.destination, existing.routeName].filter(Boolean).length;
    const candidateScore = [event.trainNumber, event.destination, event.routeName].filter(Boolean).length;
    if (candidateScore > existingScore) byKey.set(key, event);
  }
  return [...byKey.values()].sort((a, b) => a.epochMs - b.epochMs);
}

async function readPrevious() {
  if (!existsSync(PREVIOUS_PATH)) return { events: [], sources: {}, refreshedAt: null };
  try {
    return JSON.parse(await readFile(PREVIOUS_PATH, "utf8"));
  } catch (error) {
    console.warn(`Ancien planning ignoré : ${error.message}`);
    return { events: [], sources: {}, refreshedAt: null };
  }
}

async function main() {
  const tempDir = await mkdtemp(join(tmpdir(), "borderforce-gtfs-"));
  const previous = await readPrevious();
  const now = new Date();
  const horizonStart = dateToYmd(new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1
  )));
  const horizonEnd = addDaysYmd(horizonStart, MAX_SCHEDULE_DAYS);
  const results = {};
  const errors = {};

  try {
    const sncfPath = join(tempDir, "sncf.zip");
    await downloadFile(SNCF_GTFS_URL, sncfPath);
    results.sncf = await parseGtfsFeed(sncfPath, SNCF_GTFS_URL, "sncf", horizonStart, horizonEnd);
    console.log(`SNCF : ${results.sncf.eventCount} passages.`);
  } catch (error) {
    errors.sncf = error?.message || String(error);
    console.error(`SNCF : ${errors.sncf}`);
  }

  try {
    const trenitaliaUrl = await resolveTrenitaliaUrl();
    const trenitaliaPath = join(tempDir, "trenitalia.zip");
    await downloadFile(trenitaliaUrl, trenitaliaPath);
    results.trenitalia = await parseGtfsFeed(
      trenitaliaPath, trenitaliaUrl, "trenitalia", horizonStart, horizonEnd
    );
    console.log(`Trenitalia : ${results.trenitalia.eventCount} passages.`);
  } catch (error) {
    errors.trenitalia = error?.message || String(error);
    console.error(`Trenitalia : ${errors.trenitalia}`);
  }

  const sncfEvents = results.sncf?.events ||
    (previous.events || []).filter(event => event.source === "sncf");
  const trenitaliaEvents = results.trenitalia?.events ||
    (previous.events || []).filter(event => event.source === "trenitalia");
  const events = mergeEvents(sncfEvents, trenitaliaEvents)
    .filter(event => Number(event.epochMs) >= Date.now() - 86400000);

  if (!events.length) {
    throw new Error(`Aucun passage exploitable. Erreurs : ${JSON.stringify(errors)}`);
  }

  const schedule = {
    version: 1,
    builderVersion: VERSION,
    refreshedAt: new Date().toISOString(),
    horizonStart,
    horizonEnd,
    eventCount: events.length,
    sources: {
      sncf: results.sncf ? { ...results.sncf, events: undefined } :
        previous.sources?.sncf || { error: errors.sncf },
      trenitalia: results.trenitalia ? { ...results.trenitalia, events: undefined } :
        previous.sources?.trenitalia || { error: errors.trenitalia }
    },
    errors,
    events
  };

  await writeFile(OUTPUT_PATH, JSON.stringify(schedule), "utf8");
  const bytes = Buffer.byteLength(JSON.stringify(schedule));
  console.log(`Planning écrit : ${OUTPUT_PATH}`);
  console.log(`Passages : ${events.length} — taille : ${(bytes / 1024 / 1024).toFixed(2)} Mio.`);
  if (bytes > 24 * 1024 * 1024) {
    throw new Error("Le planning dépasse 24 Mio et approche la limite KV de 25 Mio.");
  }
  await rm(tempDir, { recursive: true, force: true });
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
