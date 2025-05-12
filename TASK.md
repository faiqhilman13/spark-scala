# Quantexa Coding Assignment Tasks

## Setup
- [x] **Set up Scala/Spark Project**: Initialize sbt project, configure Spark dependencies (Spark 2.4.8, Scala 2.12.10), JDK 1.8. Add `flightData.csv` and `passengers.csv` to a `data/` directory or `src/main/resources`.
- [x] **Create Case Classes**: Define case classes for `Flight` and `Passenger` data.
- [x] **Data Loading Functionality**: Implement a function to load and parse `flightData.csv` and `passengers.csv` into Spark DataFrames/Datasets.

## Core Questions
- [x] **Q1: Flights per Month** (Date: 2023-05-12)
    - Calculate the total number of flights for each month.
    - Output format: Month, Number of Flights.
    - Save output to `output/q1_flights_per_month.csv` or print to console.
- [x] **Q2: 100 Most Frequent Flyers** (Date: 2023-05-12)
    - Identify the top 100 most frequent flyers.
    - Output format: Passenger ID, Number of Flights, First name, Last name.
    - Join flight data with passenger data.
    - Save output to `output/q2_frequent_flyers.csv` or print to console.
- [x] **Q3: Longest Run Without UK** (Date: 2023-05-12)
    - For each passenger, find the greatest number of *distinct* countries they have been in consecutively without being in the UK.
    - Order output by 'longest run' in descending order.
    - Output format: Passenger ID, Longest Run.
    - Save output to `output/q3_longest_run_no_uk.csv` or print to console.
- [x] **Q4: Passengers on >3 Flights Together** (Date: 2023-05-12)
    - Find pairs of passengers who have been on more than 3 flights together.
    - Output format: Passenger 1 ID, Passenger 2 ID, Number of flights together.
    - Order output by 'number of flights flown together' in descending order.
    - Save output to `output/q4_flights_together.csv` or print to console.

## Extra Marks
- [x] **Extra: Flown Together (N times, Date Range)** (Date: 2023-05-12)
    - Implement `def flownTogether(atLeastNTimes: Int, from: Date, to: Date)`.
    - Find pairs of passengers who have been on more than `atLeastNTimes` flights together within the given date range (`from`, `to`).
    - Output format: Passenger 1 ID, Passenger 2 ID, Number of flights together, From, To.
    - Save output to `output/q_extra_flights_together_daterange.csv` or print to console.

## Final Steps
- [x] **Unit Tests**: Write comprehensive unit tests for the core logic (especially `FlightAnalyser.scala`). Include expected use, edge cases, and failure cases.
- [x] **README.md**: Create/update `README.md` with clear instructions on how to build and run the project, dependencies, and any other relevant information.
- [x] **Code Review & Refactor**: Ensure code is functional, clear, concise, efficient, and adheres to Scala best practices.
- [ ] **Packaging**: Prepare the submission as a zip file including all required components (source code, VCS files, README, output examples if generating files).

## Discovered During Work
- Added `.gitignore` file to exclude build artifacts and other unnecessary files.
- Implemented the solution using Spark SQL for Q3 (longest run without UK) for better readability and performance.
- Used a self-join strategy for Q4 (passengers flying together) to avoid double-counting. 