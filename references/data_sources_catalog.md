# Public Data Sources Catalog & Ingestion Reference

This catalog maps external public data sources to their respective ingestion archetypes, URLs, authentication methods, update schedules, and BigQuery target strategies.

---

## Ingestion Archetypes

- **Archetype A (Direct File / Static URL):** Source provides a static or predictable URL to a daily/monthly CSV, JSON, or XML file. Stream to GCS $\rightarrow$ trigger BigQuery LoadJob (`WRITE_TRUNCATE` or `WRITE_APPEND`).
- **Archetype B (Authenticated / Paginated REST API):** Source requires API key and/or query parameters, returns JSON/GeoJSON. Fetch with retry session $\rightarrow$ write NDJSON to GCS $\rightarrow$ trigger BigQuery LoadJob.
- **Archetype C (SDMX Central Bank / Macro API):** Standardized statistical web service. Fetch SDMX-CSV $\rightarrow$ write to GCS $\rightarrow$ trigger BigQuery LoadJob.
- **Archetype D (Bulk Zipped Archive):** Large compressed multi-file or single archive. Stream download, extract in chunks to GCS $\rightarrow$ trigger BigQuery LoadJob.

---

## Data Source Reference Matrix

### 1. UN Comtrade
- **Archetype:** D (Bulk Zipped Archive) or B (API for filtered subsets)
- **URL / Endpoint:** `https://comtradeapi.un.org/bulk/v1/download/{type}/{freq}?reporterCode={code}&period={period}`
- **Documentation:** `https://comtradeapi.un.org/`
- **Auth:** API Key (Header: `Ocp-Apim-Subscription-Key`). Store in Secret Manager as `COMTRADE_API_KEY`.
- **Payload Format:** Zipped CSV
- **Update Cadence:** Monthly / Annual (`0 2 15 * *`)
- **BigQuery Schema Strategy:** Partition by `period` (Integer or Date), cluster by `reporterCode`, `partnerCode`, `cmdCode`.

### 2. IMF PortWatch
- **Archetype:** A (Direct CSV Mirror) or B (ArcGIS REST Feature Layer)
- **Primary Bulk URL:** Mirrored weekly on Humanitarian Data Exchange (HDX) / PortWatch portal: `https://portwatch.imf.org`
- **API Endpoint:** ArcGIS Feature Service REST API: `https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query` (and `Daily_Ports_Data`)
- **Auth:** None required for public portal / HDX mirrors.
- **Payload Format:** CSV / GeoJSON / ArcGIS JSON
- **Update Cadence:** Published mid-week (typically Wednesdays ~12:30–14:20 UTC); schedule daily check at 15:00 UTC (`0 15 * * *`) with a 14-day rolling lookback (`days=14`).
- **BigQuery Schema Strategy:** Partition by `date`, cluster by `portname`, `portid` (chokepoints) or `country`, `ISO3`, `portname` (ports). Deduplicate by `(date, portid)` keeping latest `_ingested_at`.

### 3. JODI (Joint Organisations Data Initiative - Oil & Gas)
- **Archetype:** A (Direct Compressed CSV)
- **Download URLs:**
  - JODI-Oil: `https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip` (and oil equivalent)
- **Auth:** None
- **Payload Format:** Zipped CSV
- **Update Cadence:** Monthly (`0 6 18 * *`)
- **BigQuery Schema Strategy:** Partition by `TIME_PERIOD` (month), cluster by `REF_AREA`, `ENERGY_PRODUCT`, `FLOW_BREAKDOWN`.

### 4. U.S. EIA (Energy Information Administration)
- **Archetype:** B (REST API v2) or A (Bulk Manifest)
- **Bulk Manifest URL:** `https://api.eia.gov/v2/bulk-data`
- **REST Endpoint:** `https://api.eia.gov/v2/{category}/data/?api_key={KEY}`
- **Auth:** Free API Key. Store in Secret Manager as `EIA_API_KEY`.
- **Payload Format:** JSON / Zipped JSON lines
- **Update Cadence:** Weekly or Monthly depending on category (e.g., Weekly Petroleum `0 16 * * 3`).
- **BigQuery Schema Strategy:** Partition by `period` (Date), cluster by `series_id`, `state_id`.

### 5. GIE AGSI+ (European Gas Storage Inventory)
- **Archetype:** B (Authenticated REST API)
- **API Endpoint:** `https://agsi.gie.eu/api?date={YYYY-MM-DD}` (also supports country / facility query)
- **Auth:** Free API Key (Header: `x-key`). Store in Secret Manager as `AGSI_API_KEY`.
- **Payload Format:** JSON
- **Update Cadence:** Daily at 20:00 CET (`0 20 * * *`)
- **BigQuery Schema Strategy:** Partition by `gasDayStart` (Date), cluster by `country`, `company_name`. `WriteDisposition.WRITE_APPEND`.

### 6. Open-Meteo
- **Archetype:** B (REST API) or A (Parquet open bucket)
- **API Endpoint:** `https://api.open-meteo.com/v1/forecast` / `https://archive-api.open-meteo.com/v1/archive`
- **Auth:** None for non-commercial (< 10k calls/day). Commercial requires API key parameter.
- **Payload Format:** JSON / Parquet
- **Update Cadence:** Hourly (`0 * * * *`) or Daily.
- **BigQuery Schema Strategy:** Partition by `time` (Timestamp), cluster by `latitude`, `longitude`.

### 7. GDACS (Global Disaster Alert and Coordination System)
- **Archetype:** B (GeoJSON Feed / REST API)
- **Endpoint:** `https://www.gdacs.org/xml/gdacs.geojson` or `https://www.gdacs.org/gdacsapi/api/events/geteventlist`
- **Auth:** None
- **Payload Format:** GeoJSON
- **Update Cadence:** Every 30 minutes (`*/30 * * * *`)
- **BigQuery Schema Strategy:** Partition by `fromdate` (Date), cluster by `eventtype`, `alertlevel`. Use `GEOGRAPHY` data type for GeoJSON coordinates.

### 8. NASA FIRMS (Fire Information for Resource Management System)
- **Archetype:** B (Active Fire API) or A (Daily Country Archive CSV)
- **API Endpoint:** `https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{BBOX}/{DAYS}`
- **Archive URL:** `https://firms.modaps.eosdis.nasa.gov/download/`
- **Auth:** Free `MAP_KEY`. Store in Secret Manager as `NASA_FIRMS_MAP_KEY`.
- **Payload Format:** CSV / GeoJSON
- **Update Cadence:** Daily or every 6 hours (`0 */6 * * *`)
- **BigQuery Schema Strategy:** Partition by `acq_date` (Date), cluster by `satellite`, `instrument`, `confidence`.

### 9. OFAC (Sanctions: SDN & Consolidated Lists)
- **Archetype:** A (Direct CSV / JSON)
- **Direct URLs:**
  - SDN: `https://www.treasury.gov/ofac/downloads/sdn.csv`
  - Consolidated: `https://www.treasury.gov/ofac/downloads/consolidated/consolidated.csv`
- **Auth:** None
- **Payload Format:** CSV (or modern XML/JSON)
- **Update Cadence:** Daily (`0 8 * * *`)
- **BigQuery Schema Strategy:** `WRITE_TRUNCATE` into current active table, or append with `ingestion_date` partition to preserve audit history.

### 10. FATF (Financial Action Task Force Lists)
- **Archetype:** A (via OpenSanctions structured export)
- **Direct Export URL:** `https://data.opensanctions.org/datasets/latest/fatf/entities.ftm.json` (or CSV export)
- **Auth:** None
- **Payload Format:** JSON Lines (FollowTheMoney) or CSV
- **Update Cadence:** Daily or weekly (`0 4 * * 1`)
- **BigQuery Schema Strategy:** Partition by `ingestion_date`, cluster by `schema`, `country`.

### 11. CISA KEV (Known Exploited Vulnerabilities)
- **Archetype:** A (Direct JSON / CSV Feed)
- **Direct URLs:**
  - CSV: `https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.csv`
  - JSON: `https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json`
- **Auth:** None
- **Payload Format:** CSV or JSON
- **Update Cadence:** Daily (`0 9 * * *`)
- **BigQuery Schema Strategy:** `WRITE_TRUNCATE` for current catalog table, or append snapshot with `_snapshot_date` partition.

### 12. Central Banks / Macro (IMF, BIS, ECB, Eurostat)
- **Archetype:** C (SDMX REST / CSV)
- **Endpoints:**
  - **IMF:** `https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{dataset}/{key}`
  - **ECB:** `https://data-api.ecb.europa.eu/service/data/{flowRef}/{key}?format=csvdata`
  - **BIS:** `https://stats.bis.org/api/v1/data/{flowRef}/{key}?format=csv`
  - **Eurostat:** `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{datasetCode}?format=JSON`
- **Auth:** None for public data endpoints.
- **Payload Format:** SDMX-CSV or SDMX-JSON
- **Update Cadence:** Daily or Monthly depending on series.
- **BigQuery Schema Strategy:** Partition by `TIME_PERIOD` (Date), cluster by series identifier dimensions.

### 13. FAO Food Price Index
- **Archetype:** A (Direct Monthly CSV/Excel) or B (FAOSTAT API)
- **Bulk Page:** `https://www.fao.org/worldfoodsituation/foodpricesindex/en/`
- **FAOSTAT API:** `https://fenixservices.fao.org/faostat/api/v1/en/data/{domainCode}`
- **Auth:** None
- **Payload Format:** CSV
- **Update Cadence:** Monthly (first Thursday of each month `0 10 1-7 * 4`)
- **BigQuery Schema Strategy:** Partition by `date` (Monthly), cluster by `commodity_group`.

### 14. UCDP (Uppsala Conflict Data Program)
- **Archetype:** A (Versioned Bulk CSV) or B (REST API)
- **REST API:** `https://ucdpapi.pcr.uu.se/api/gedevents/{version}?page={page}&pagesize=1000`
- **Direct Download:** `https://ucdp.uu.se/downloads/ged/` (Direct CSV download for full GED history)
- **Auth:** None
- **Payload Format:** CSV / JSON
- **Update Cadence:** Quarterly / Annual.
- **BigQuery Schema Strategy:** Partition by `date_start` (Date), cluster by `country`, `conflict_name`, `type_of_violence`.

### 15. OpenSky Network (ADS-B)
- **Archetype:** B (Live REST API with Cloud Fallback) or D (Monthly Research Parquet Dumps)
- **Live Primary Endpoint:** `https://opensky-network.org/api/states/all`
- **Cloud-Compatible Fallback Endpoint:** `https://opendata.adsb.fi/api/v2/lat/{lat}/lon/{lon}/dist/{dist}` (used automatically when OpenSky drops TCP connections from cloud provider egress IPs)
- **Historical Dumps:** OpenSky data repository (Parquet / CSV dumps by month)
- **Auth:** Optional HTTP Basic Auth for higher rate limits on OpenSky.
- **Payload Format:** JSON (live states) or Parquet (historical)
- **Update Cadence:** Hourly (`0 * * * *`) for live snapshots, Monthly for historical archives.
- **BigQuery Schema Strategy:** Partition by `snapshot_date` (DAY), cluster by `origin_country`, `callsign`, `icao24`, with post-load idempotent deduplication on `(snapshot_date, icao24)` keeping the latest `snapshot_time` (preferring non-null `callsign`).

