# CURSOR GLOBAL RULES
This is a SaaS app.
- Backend logic = modular
- Routes call services/
- FE uses fetch/Axios
- No extra tools or design systems unless approved

# Project: Quantexa Coding Assignment

## 1. Overview
This project aims to solve a coding assignment provided by Quantexa. The primary goal is to analyze flight data using Scala and Spark, focusing on functional programming principles, code clarity, conciseness, and efficiency.

## 2. Technologies
- **Language:** Scala (version 2.12.10)
- **Framework/Library:** Apache Spark (version 2.4.8)
- **Build Tool:** sbt (suggested for Scala projects)
- **IDE:** IntelliJ IDEA (suggested)
- **JDK:** 1.8

## 3. Data
Two CSV files are provided:
- `flightData.csv`: Contains flight details (passengerId, flightId, from, to, date).
- `passengers.csv`: Contains passenger details (passengerId, firstName, lastName).

## 4. Core Requirements (Questions)
1.  **Q1:** Find the total number of flights for each month.
2.  **Q2:** Find the names of the 100 most frequent flyers.
3.  **Q3:** Find the greatest number of countries a passenger has been in without being in the UK.
4.  **Q4:** Find the passengers who have been on more than 3 flights together.
5.  **Extra Marks:** Find passengers on more than N flights together within a date range `(from, to)`.

## 5. Deliverables
- A zip file containing:
    - Scala/Spark source code.
    - CSV file(s) with answers for each question (or console output).
    - Instructions on how to run the code.
    - VCS files (e.g., `.git` directory).
    - `README.md` (if applicable).
    - Unit tests.

## 6. Key Assessment Criteria
- Correctness
- Performance (especially for large datasets)
- Code and engineering quality (functional style, clarity, conciseness)
- Testing strategies
- Presentation (documentation, ease of usage)

## 7. Project Structure (Initial thought - may evolve)
```
.
├── project/              # sbt build definition
│   └── build.properties
│   └── plugins.sbt
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/quantexa/solution/
│   │   │       ├── Main.scala            # Main application
│   │   │       ├── FlightAnalyser.scala  # Core logic for questions
│   │   │       └── models/               # Case classes
│   │   │           ├── Flight.scala
│   │   │           └── Passenger.scala
│   │   └── resources/            # Input CSV files can be placed here for dev
│   │       ├── flightData.csv
│   │       └── passengers.csv
│   └── test/
│       └── scala/
│           └── com/quantexa/solution/
│               └── FlightAnalyserSpec.scala # Unit tests
├── data/                   # Input data (as provided) - this is where your current files are
│   ├── flightData.csv
│   └── passengers.csv
├── output/                 # Output CSV files
│   ├── q1_flights_per_month.csv
│   ├── q2_frequent_flyers.csv
│   ├── q3_longest_run_no_uk.csv
│   ├── q4_flights_together.csv
│   └── q_extra_flights_together_daterange.csv (if implemented)
├── build.sbt               # sbt build file
├── README.md               # Instructions and project overview
└── .gitignore
```

## 8. Style & Conventions
- Language: Scala
- Functional programming principles.
- Clear and concise code.
- Case classes for data representation.
- Unit tests (ScalaTest or similar).

## 9. Security & Production-Grade Safety
- While not a web application, input validation (e.g., for date parsing) and robust error handling are important.
- Ensure data processing is efficient and handles potential data inconsistencies gracefully.

## 10. Notes on Efficiency with Spark
- Use DataFrame/Dataset API where possible for optimized execution.
- Minimize shuffles.
- Use appropriate partitioning.
- Broadcast small DataFrames when joining with large ones.
- Persist intermediate DataFrames that are used multiple times. 