-- Benchmark: array_contains_all with larger load and bigger haystacks
.timer on

.print === Benchmark: 500K rows, haystack size 50-100 ===

CREATE OR REPLACE TABLE bench_large AS
SELECT
    i as id,
    -- Large haystack: 50-100 elements
    list_transform(range(1, 50 + (i % 51)), lambda x: 'item_' || (x + (i * 7) % 500)::VARCHAR) as haystack,
    -- Needle: 5-10 elements
    list_transform(range(1, 5 + (i % 6)), lambda x: 'item_' || (x + (i * 3) % 200)::VARCHAR) as needle
FROM range(1, 500001) t(i);

SELECT 'Rows: ' || COUNT(*) as info FROM bench_large;
SELECT 'Avg haystack size: ' || ROUND(AVG(len(haystack)), 1) as info FROM bench_large;
SELECT 'Avg needle size: ' || ROUND(AVG(len(needle)), 1) as info FROM bench_large;

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_large WHERE array_contains_all(haystack, needle, false);

DROP TABLE bench_large;

.print
.print === Benchmark: 1M rows, haystack size 100-200 ===

CREATE OR REPLACE TABLE bench_xlarge AS
SELECT
    i as id,
    -- Very large haystack: 100-200 elements
    list_transform(range(1, 100 + (i % 101)), lambda x: 'item_' || (x + (i * 7) % 1000)::VARCHAR) as haystack,
    -- Needle: 5-15 elements
    list_transform(range(1, 5 + (i % 11)), lambda x: 'item_' || (x + (i * 3) % 500)::VARCHAR) as needle
FROM range(1, 1000001) t(i);

SELECT 'Rows: ' || COUNT(*) as info FROM bench_xlarge;
SELECT 'Avg haystack size: ' || ROUND(AVG(len(haystack)), 1) as info FROM bench_xlarge;
SELECT 'Avg needle size: ' || ROUND(AVG(len(needle)), 1) as info FROM bench_xlarge;

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_xlarge WHERE array_contains_all(haystack, needle, false);

DROP TABLE bench_xlarge;

.print
.print === Benchmark: 500K rows, haystack 200 elements, high match rate ===

CREATE OR REPLACE TABLE bench_high_match AS
SELECT
    i as id,
    -- Fixed large haystack with predictable items
    list_transform(range(1, 201), lambda x: 'item_' || x::VARCHAR) as haystack,
    -- Needle always matches (items 1-10)
    list_transform(range(1, 11), lambda x: 'item_' || x::VARCHAR) as needle
FROM range(1, 500001) t(i);

SELECT 'Rows: ' || COUNT(*) as info FROM bench_high_match;
SELECT 'Haystack size: ' || len(haystack) as info FROM bench_high_match LIMIT 1;
SELECT 'Needle size: ' || len(needle) as info FROM bench_high_match LIMIT 1;

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_high_match WHERE array_contains_all(haystack, needle, false);

DROP TABLE bench_high_match;

.print
.print === Benchmark: 500K rows, haystack 200 elements, low match rate ===

CREATE OR REPLACE TABLE bench_low_match AS
SELECT
    i as id,
    -- Fixed large haystack
    list_transform(range(1, 201), lambda x: 'item_' || x::VARCHAR) as haystack,
    -- Needle with one item that doesn't exist
    ['item_1', 'item_2', 'item_999'] as needle
FROM range(1, 500001) t(i);

SELECT 'Rows: ' || COUNT(*) as info FROM bench_low_match;

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_low_match WHERE array_contains_all(haystack, needle, false);

DROP TABLE bench_low_match;

.print
.print === Benchmark: 200K rows, haystack 500 elements ===

CREATE OR REPLACE TABLE bench_huge_haystack AS
SELECT
    i as id,
    list_transform(range(1, 501), lambda x: 'element_' || (x + (i % 100))::VARCHAR) as haystack,
    list_transform(range(1, 8), lambda x: 'element_' || (x + (i % 50))::VARCHAR) as needle
FROM range(1, 200001) t(i);

SELECT 'Rows: ' || COUNT(*) as info FROM bench_huge_haystack;
SELECT 'Avg haystack size: ' || ROUND(AVG(len(haystack)), 1) as info FROM bench_huge_haystack;
SELECT 'Avg needle size: ' || ROUND(AVG(len(needle)), 1) as info FROM bench_huge_haystack;

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM bench_huge_haystack WHERE array_contains_all(haystack, needle, false);

DROP TABLE bench_huge_haystack;

.print
.print === Benchmark Complete ===
