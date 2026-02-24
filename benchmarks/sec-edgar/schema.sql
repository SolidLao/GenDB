-- =============================================================================
-- SEC EDGAR Financial Statement Data Sets Schema
-- =============================================================================
-- Source: https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets
-- Docs:   https://www.sec.gov/files/aqfs.pdf
--
-- SCENARIO:
-- The SEC (U.S. Securities and Exchange Commission) requires all publicly
-- traded companies to file periodic financial reports (10-K annual, 10-Q
-- quarterly, etc.) through its EDGAR system. These filings contain XBRL-tagged
-- financial statements — balance sheets, income statements, cash flow
-- statements, and equity statements.
--
-- The Financial Statement Data Sets are published quarterly by the SEC's
-- Division of Economic and Risk Analysis (DERA). They provide structured,
-- machine-readable extractions of every numeric fact, XBRL tag definition,
-- and presentation layout from these filings, enabling large-scale
-- quantitative analysis of public company financials without parsing raw XBRL.
--
-- The dataset consists of 4 relational tables:
--   SUB  — one row per filing (the "header": who filed, when, what form)
--   NUM  — one row per numeric fact (the actual reported dollar amounts, etc.)
--   TAG  — one row per XBRL concept definition (what each line item means)
--   PRE  — one row per presentation line (how items are displayed in statements)
--
-- Key relationships:
--   sub.adsh  <--  num.adsh      (a filing has many numeric facts)
--   sub.adsh  <--  pre.adsh      (a filing has many presentation lines)
--   (tag.tag, tag.version) <-- (num.tag, num.version)  (facts reference tags)
--   (tag.tag, tag.version) <-- (pre.tag, pre.version)  (presentation references tags)
-- =============================================================================


-- =============================================================================
-- SUB: Submissions (one row per EDGAR filing)
-- Each row represents a single SEC filing (e.g., a 10-K annual report or
-- 10-Q quarterly report) submitted by a registrant through EDGAR.
-- =============================================================================
CREATE TABLE sub (
    adsh       VARCHAR(20) PRIMARY KEY,  -- Accession Number: unique 20-char SEC filing ID (nnnnnnnnnn-nn-nnnnnn)
    cik        INTEGER,                  -- Central Index Key: unique SEC-assigned numeric ID for the registrant (company)
    name       VARCHAR(150),             -- Registrant Name: legal name of the filing entity
    sic        INTEGER,                  -- Standard Industrial Classification: 4-digit industry code (e.g., 7372=Software)
    countryba  VARCHAR(2),               -- Country of Business Address: ISO 3166-1 two-letter code (e.g., "US")
    stprba     VARCHAR(2),               -- State/Province of Business Address: two-letter code (US/CA filers only)
    cityba     VARCHAR(30),              -- City of Business Address
    countryinc VARCHAR(3),               -- Country of Incorporation: where the entity is legally incorporated
    form       VARCHAR(10),              -- Form Type: SEC form (e.g., "10-K", "10-Q", "8-K", "20-F")
    period     INTEGER,                  -- Period of Report: balance sheet / reporting end date (YYYYMMDD as integer)
    fy         INTEGER,                  -- Fiscal Year: the fiscal year this filing pertains to (e.g., 2023)
    fp         VARCHAR(2),               -- Fiscal Period: "FY"=full year, "Q1".."Q4"=quarters, "H1"/"H2"=halves
    filed      INTEGER,                  -- Date Filed: date submitted to SEC (YYYYMMDD as integer)
    accepted   VARCHAR(24),              -- Acceptance Datetime: when EDGAR accepted the filing ("YYYY-MM-DD HH:MM:SS")
    prevrpt    INTEGER,                  -- Previous Report Flag: 1=this filing was superseded by an amendment, 0=current
    nciks      INTEGER,                  -- Number of CIKs: count of registrants in the filing (>1 for consolidated)
    afs        VARCHAR(5),               -- Filer Status: "1-LAF"=Large Accelerated, "2-ACC"=Accelerated, etc.
    wksi       INTEGER,                  -- Well-Known Seasoned Issuer: 1=yes (large, established issuer), 0=no
    fye        VARCHAR(4),               -- Fiscal Year End: month and day of fiscal year end (MMDD, e.g., "1231"=Dec 31)
    instance   VARCHAR(64)               -- XBRL Instance Document: filename of the XBRL source within the filing
);


-- =============================================================================
-- NUM: Numeric Data (one row per reported financial fact)
-- Each row is a single numeric value extracted from a filing's XBRL-tagged
-- financial statements — e.g., total revenue of $50B, shares outstanding of
-- 15.2B, or earnings per share of $6.13.
-- Composite key: (adsh, tag, version, ddate, qtrs, uom, coreg)
-- =============================================================================
CREATE TABLE num (
    adsh       VARCHAR(20),    -- Accession Number: FK to sub.adsh identifying the source filing
    tag        VARCHAR(256),   -- XBRL Tag: concept name (e.g., "Revenues", "Assets", "NetIncomeLoss")
    version    VARCHAR(20),    -- Taxonomy Version: e.g., "us-gaap/2023" for standard tags, or adsh for custom tags
    ddate      INTEGER,        -- Data Date: end date of the fact's period (YYYYMMDD); for instant items, the measurement date
    qtrs       INTEGER,        -- Quarters: duration in quarters (0=instant/point-in-time, 1=one quarter, 4=full year)
    uom        VARCHAR(20),    -- Unit of Measure: "USD", "shares", "USD/shares" (EPS), "pure" (ratios/percentages)
    coreg      VARCHAR(256),   -- Coregistrant: identifies subsidiary entity if not the parent; NULL for primary entity
    value      DOUBLE,         -- Numeric Value: the reported amount (dollars, shares, ratio, etc.)
    footnote   VARCHAR(1024),  -- Footnote: text of any footnote attached to this fact; NULL if none
    FOREIGN KEY (adsh) REFERENCES sub(adsh)
);



-- =============================================================================
-- TAG: Tag Definitions (one row per unique XBRL concept)
-- Each row defines an XBRL tag — a standardized or custom accounting concept
-- used to label line items in financial statements (e.g., "Assets" is a
-- standard US-GAAP tag meaning total assets on a balance sheet).
-- Composite primary key: (tag, version)
-- =============================================================================
CREATE TABLE tag (
    tag        VARCHAR(256),   -- Tag Name: machine-readable concept name (e.g., "Assets", "EarningsPerShareBasic")
    version    VARCHAR(20),    -- Taxonomy Version: "us-gaap/2023" for standard tags; filing adsh for custom tags
    custom     INTEGER,        -- Custom Flag: 1=filer-defined extension tag, 0=standard taxonomy tag
    abstract   INTEGER,        -- Abstract Flag: 1=grouping/header concept (no numeric value), 0=concrete reportable concept
    datatype   VARCHAR(20),    -- Data Type: "monetary", "shares", "perShare", "pure", "integer", "decimal", etc.
    iord       VARCHAR(1),     -- Instant or Duration: "I"=point-in-time (balance sheet), "D"=over-a-period (income stmt)
    crdr       VARCHAR(1),     -- Credit or Debit: natural accounting balance — "C"=credit (revenue, liabilities), "D"=debit (assets, expenses)
    tlabel     VARCHAR(512),   -- Tag Label: human-readable name from the taxonomy (e.g., "Assets, Total")
    doc        TEXT,           -- Documentation: official accounting definition of the concept from the taxonomy
    PRIMARY KEY (tag, version)
);


-- =============================================================================
-- PRE: Presentation (one row per line in a rendered financial statement)
-- Each row represents how a specific line item appears in the filer's
-- financial statements — its position, label, and which statement it belongs
-- to (Balance Sheet, Income Statement, etc.).
-- Composite key: (adsh, report, line)
-- =============================================================================
CREATE TABLE pre (
    adsh       VARCHAR(20),    -- Accession Number: FK to sub.adsh identifying the source filing
    report     INTEGER,        -- Report Number: sequential ID of the statement within the filing
    line       INTEGER,        -- Line Number: display order position within the statement
    stmt       VARCHAR(2),     -- Statement Type: "BS"=Balance Sheet, "IS"=Income Statement, "CF"=Cash Flow, "EQ"=Equity, "CI"=Comprehensive Income
    inpth      INTEGER,        -- Parenthetical Flag: 1=shown in parentheses (supplementary), 0=standalone line item
    rfile      VARCHAR(1),     -- Report File Source: "H"=HTML rendering, "R"=XBRL viewer rendering
    tag        VARCHAR(256),   -- XBRL Tag: the concept assigned to this line item; FK to tag.tag
    version    VARCHAR(20),    -- Taxonomy Version: FK to tag.version
    plabel     VARCHAR(1024),  -- Presentation Label: filer-customized display text (may differ from tag.tlabel)
    negating   INTEGER,        -- Negating Flag: 1=sign should be flipped for display (e.g., COGS as positive subtracted), 0=normal
    FOREIGN KEY (adsh) REFERENCES sub(adsh)
);

