# Quantexa Coding Assignment

This project analyzes flight and passenger data to answer several questions about flight patterns, frequent flyers, and passengers who travel together.

## Requirements

- JDK 1.8
- Scala 2.12.10
- Apache Spark 2.4.8
- SBT (Scala Build Tool)

## Project Structure

```
.
├── src/
│   ├── main/scala/com/quantexa/solution/
│   │   ├── Main.scala            # Main application
│   │   ├── FlightAnalyser.scala  # Core logic for questions
│   │   └── models/               # Case classes
│   │       ├── Flight.scala
│   │       └── Passenger.scala
│   └── test/scala/com/quantexa/solution/
│       └── FlightAnalyserSpec.scala # Unit tests
├── data/                   # Input data
│   ├── flightData.csv
│   └── passengers.csv
├── output/                 # Output CSV files
├── build.sbt               # SBT build file
├── project/
│   ├── build.properties
│   └── plugins.sbt
└── README.md               # This file
```

## Building the Project

To build the project, run:

```bash
sbt clean compile
```

To run the tests:

```bash
sbt test
```

To create a fat JAR file that includes all dependencies:

```bash
sbt assembly
```

## Running the Application

### Using SBT

```bash
sbt "run [flightDataPath] [passengersPath]"
```

Where:
- `flightDataPath` is the path to the flight data CSV file (default: `data/flightData.csv`)
- `passengersPath` is the path to the passengers CSV file (default: `data/passengers.csv`)

### Using the JAR file

```bash
java -jar target/scala-2.12/quantexa-coding-assignment-assembly-1.0.jar [flightDataPath] [passengersPath]
```

## Questions and Implementation Approaches

The application answers the following questions, with detailed implementation explanations:

### 1. Total Flights per Month

**Problem**: Count the total number of flights for each month.

**Implementation**:
- Used Spark's DataFrame API with functional transformations.
- Applied the `month()` function to extract month from the date string.
- Grouped by month and used aggregation to count flights.
- Time complexity: O(n) where n is the number of flight records.

**Code Pattern**:
```scala
flightData
  .withColumn("month", month(to_date($"date", "yyyy-MM-dd")))
  .groupBy("month")
  .agg(count("*").as("Number of Flights"))
  .orderBy("month")
```

**Output**: `output/q1_flights_per_month.csv`

### 2. 100 Most Frequent Flyers

**Problem**: Identify the 100 passengers who have taken the most flights.

**Implementation**:
- Used a two-step approach:
  1. Aggregated flight counts per passenger using `groupBy` and `count`.
  2. Joined with passenger data to retrieve names.
- Ordered results by flight count in descending order.
- Used `limit(100)` to restrict to top 100 passengers.
- Performance optimization: The join operation uses a small passenger dataset that can be broadcast to all worker nodes.

**Code Pattern**:
```scala
val flightsPerPassenger = flightData
  .groupBy("passengerId")
  .agg(count("*").as("Number of Flights"))
  
flightsPerPassenger
  .join(passengerData, "passengerId")
  .select(...)
  .orderBy(col("Number of Flights").desc)
  .limit(100)
```

**Output**: `output/q2_frequent_flyers.csv`

### 3. Longest Run Without UK

**Problem**: For each passenger, find the greatest number of countries they have been in without being in the UK.

**Implementation**:
- Chose Spark SQL for this complex problem due to its expressiveness with window functions.
- Uses a multi-step approach:
  1. Creates a flattened view of all countries visited (both departure and destination).
  2. Marks UK visits with a flag.
  3. Uses window functions to partition travel sequences by UK visits.
  4. Counts distinct countries within each partition.
  5. Finds the maximum run for each passenger.
- This SQL-based approach provides better readability while maintaining performance through Spark's query optimizer.
- The algorithm handles several edge cases:
  - Passengers who never visit the UK
  - Passengers who only visit the UK
  - Consecutive flights to the same country

**Code Pattern**:
```sql
-- Step 1: Flatten all countries (departure and destination)
WITH flights_with_country AS (...)
-- Step 2: Find UK visits and mark them
ordered_countries AS (...)
-- Step 3: Assign group IDs for runs between UK visits
uk_groups AS (...)
-- Step 4: Count distinct countries in each run
country_runs AS (...)
-- Step 5: Find max run per passenger
max_runs AS (...)
```

**Output**: `output/q3_longest_run_no_uk.csv`

### 4. Passengers on >3 Flights Together

**Problem**: Find pairs of passengers who have been on more than 3 flights together.

**Implementation**:
- Used a self-join strategy to find passenger pairs on the same flight.
- Added a condition `passengerId1 < passengerId2` to prevent double-counting pairs.
- Grouped by passenger pair and counted shared flights.
- Filtered for pairs with more than 3 flights together.
- Performance considerations:
  - The self-join can be expensive for large datasets, but is optimized by Spark's query planner.
  - The `passengerId1 < passengerId2` condition reduces the join output size by half.
  - For very large datasets, we could consider partitioning by `flightId` to improve join performance.

**Code Pattern**:
```scala
flightData.as("f1")
  .join(flightData.as("f2"), 
    col("f1.flightId") === col("f2.flightId") && 
    col("f1.passengerId") < col("f2.passengerId"))
  .select(...)
  .groupBy("passenger1Id", "passenger2Id")
  .agg(count("flightId").as("flightsTogether"))
  .filter(col("flightsTogether") > minFlights)
```

**Output**: `output/q4_flights_together.csv`

### 5. Extra: Flown Together in Date Range

**Problem**: Find pairs of passengers who have been on more than N flights together within a specified date range.

**Implementation**:
- Extended the approach from Question 4 with date filtering.
- Used the `between` function to filter flights within the specified date range.
- This approach allows for flexible configuration of the minimum number of flights and date range.
- The implementation handles date parsing and comparison efficiently using Spark's built-in date functions.

**Code Pattern**:
```scala
val filteredFlights = flightData
  .filter(to_date(col("date"), "yyyy-MM-dd").between(lit(from), lit(to)))

// Self-join, grouping, and filtering as in Question 4
```

**Output**: `output/q_extra_flights_together_daterange.csv`

## Implementation Details

### Functional Programming Principles

The solution follows functional programming principles:
- Immutability: All datasets and dataframes are treated as immutable.
- Pure functions: Functions have no side effects and return the same output for the same input.
- Higher-order functions: Using `map`, `filter`, `groupBy`, etc. to transform data.
- Function composition: Building complex data pipelines by chaining operations.

### Data Representation

- Used **case classes** (`Flight` and `Passenger`) to leverage Scala's type safety.
- Employed Spark's Dataset API to combine the benefits of strongly-typed datasets and Spark SQL's optimized execution.
- This approach allows for both type-safe code and optimized query execution.

### Testing Strategy

The test suite includes:
- Unit tests for each analytical function
- Test data covering:
  - Typical usage patterns
  - Edge cases (e.g., passengers who never visit the UK or always stay in the UK)
  - Boundary conditions (e.g., different flight counts for pairs of passengers)

### Performance Considerations

Several optimizations have been applied to ensure the solution remains efficient for large datasets:

- **Minimized Shuffles**: Careful ordering of operations to reduce data movement between partitions.
- **Broadcast Joins**: The small passenger dataset can be broadcast to all workers when joining with flight data.
- **Predicate Pushdown**: Filtering (e.g., by date range) is done early to reduce the amount of processed data.
- **Column Pruning**: Only required columns are selected to minimize memory footprint.
- **Smart Partitioning**: The outputs are coalesced to a single file for each question to ensure CSV output correctness.

For production use with very large datasets, additional optimizations could include:
- Custom partitioning of input data
- Caching intermediary results for reuse
- Cluster tuning (executor memory, cores, etc.) 