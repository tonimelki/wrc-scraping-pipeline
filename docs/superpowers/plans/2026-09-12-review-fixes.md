# Review fixes

Implement the accepted review findings without deleting existing captures.

1. Scraper: regression tests for redirected/error detail pages and binary attachments;
   validate before storage, preserve binary bytes, use available HTTP validators,
   and refetch missing cached objects. Files: spider, items, detail tests.
2. Storage: hash-versioned immutable objects, safe retry after partial writes,
   immutable landing metadata snapshots with a separate current-state index.
   Existing landing data remains readable. Test preservation and retry behavior.
3. Transform: stream the requested publication-date range, stable per-document
   directories containing identifier.ext, reject invalid ranges, retain lineage.
4. Orchestration: configurable calendar partitions and start, pass configuration
   to ingestion, preserve correct task dependency. Add regression tests.
5. Update documentation and verification scripts, run offline and available
   container tests, lint, then independently review the complete diff.

No live crawl, data deletion, commits, or publishing are required for these fixes.
