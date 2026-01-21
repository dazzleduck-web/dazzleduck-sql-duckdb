-- Benchmark: array_contains_all implementations
-- Compares row-based (false) vs columnar (true) processing

.timer on

-- Create test data with varying sizes
CREATE OR REPLACE TABLE benchmark_data AS
SELECT
    i as id,
    -- Haystack: random strings, varying sizes
    list_transform(range(1, (i % 20) + 5), lambda x: 'item_' || (x + (i * 7) % 100)::VARCHAR) as haystack,
    -- Needle: subset of possible values
    list_transform(range(1, (i % 5) + 2), lambda x: 'item_' || (x + (i * 3) % 50)::VARCHAR) as needle
FROM range(1, 10001) t(i);

SELECT 'Test data created: ' || COUNT(*) || ' rows' as status FROM benchmark_data;
SELECT 'Avg haystack size: ' || AVG(len(haystack))::INT as info FROM benchmark_data;
SELECT 'Avg needle size: ' || AVG(len(needle))::INT as info FROM benchmark_data;

-- Warm up
SELECT COUNT(*) FROM benchmark_data WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) FROM benchmark_data WHERE array_contains_all(haystack, needle, false);

.print
.print === Benchmark: 10,000 rows ===
.print

-- Benchmark columnar (true) - run 3 times
.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, true);

-- Benchmark row-based (false) - run 3 times
.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_data WHERE array_contains_all(haystack, needle, false);

.print
.print === Benchmark: 100,000 rows ===
.print

-- Larger dataset
CREATE OR REPLACE TABLE benchmark_data_large AS
SELECT
    i as id,
    list_transform(range(1, (i % 20) + 5), lambda x: 'item_' || (x + (i * 7) % 100)::VARCHAR) as haystack,
    list_transform(range(1, (i % 5) + 2), lambda x: 'item_' || (x + (i * 3) % 50)::VARCHAR) as needle
FROM range(1, 100001) t(i);

-- Benchmark columnar (true)
.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, true);

-- Benchmark row-based (false)
.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_data_large WHERE array_contains_all(haystack, needle, false);

.print
.print === Benchmark: High failure rate (most rows fail early) ===
.print

-- Dataset where most rows will fail (needle has items not in haystack)
CREATE OR REPLACE TABLE benchmark_high_fail AS
SELECT
    i as id,
    list_transform(range(1, 10), lambda x: 'hay_' || x::VARCHAR) as haystack,
    ['hay_1', 'hay_2', 'needle_not_found'] as needle  -- Last item won't be found
FROM range(1, 50001) t(i);

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM benchmark_high_fail WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_high_fail WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM benchmark_high_fail WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_high_fail WHERE array_contains_all(haystack, needle, false);

.print
.print === Benchmark: All rows succeed ===
.print

-- Dataset where all rows will succeed
CREATE OR REPLACE TABLE benchmark_all_pass AS
SELECT
    i as id,
    ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'] as haystack,
    ['a', 'b', 'c'] as needle
FROM range(1, 50001) t(i);

.print --- Columnar Implementation (true) ---
SELECT COUNT(*) as matches FROM benchmark_all_pass WHERE array_contains_all(haystack, needle, true);
SELECT COUNT(*) as matches FROM benchmark_all_pass WHERE array_contains_all(haystack, needle, true);

.print --- Row-based Implementation (false) ---
SELECT COUNT(*) as matches FROM benchmark_all_pass WHERE array_contains_all(haystack, needle, false);
SELECT COUNT(*) as matches FROM benchmark_all_pass WHERE array_contains_all(haystack, needle, false);

-- Cleanup
DROP TABLE IF EXISTS benchmark_data;
DROP TABLE IF EXISTS benchmark_data_large;
DROP TABLE IF EXISTS benchmark_high_fail;
DROP TABLE IF EXISTS benchmark_all_pass;

.print
.print === Benchmark Complete ===
