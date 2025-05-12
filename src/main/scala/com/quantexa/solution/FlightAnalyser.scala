package com.quantexa.solution

import com.quantexa.solution.models.{Flight, Passenger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date

/**
 * FlightAnalyser provides methods to analyze flight and passenger data
 * to answer the questions in the Quantexa coding assignment.
 */
class FlightAnalyser(spark: SparkSession) {
  import spark.implicits._

  /**
   * Loads flight data from CSV file
   *
   * @param path Path to the flightData.csv file
   * @return Dataset of Flight objects
   */
  def loadFlightData(path: String): Dataset[Flight] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[Flight]
  }

  /**
   * Loads passenger data from CSV file
   *
   * @param path Path to the passengers.csv file
   * @return Dataset of Passenger objects
   */
  def loadPassengerData(path: String): Dataset[Passenger] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[Passenger]
  }

  /**
   * Question 1: Find the total number of flights for each month.
   *
   * @param flightData Dataset of Flight objects
   * @return DataFrame with columns (Month, Number of Flights)
   */
  def flightsPerMonth(flightData: Dataset[Flight]): DataFrame = {
    flightData
      .withColumn("month", month(to_date($"date", "yyyy-MM-dd")))
      .groupBy("month")
      .agg(count("*").as("Number of Flights"))
      .orderBy("month")
      .withColumnRenamed("month", "Month")
  }

  /**
   * Question 2: Find the names of the 100 most frequent flyers.
   *
   * @param flightData Dataset of Flight objects
   * @param passengerData Dataset of Passenger objects
   * @return DataFrame with columns (Passenger ID, Number of Flights, First name, Last name)
   */
  def frequentFlyers(flightData: Dataset[Flight], passengerData: Dataset[Passenger]): DataFrame = {
    // Count flights per passenger
    val flightsPerPassenger = flightData
      .groupBy("passengerId")
      .agg(count("*").as("Number of Flights"))
      
    // Join with passenger data to get names
    flightsPerPassenger
      .join(passengerData, "passengerId")
      .select(
        col("passengerId").as("Passenger ID"),
        col("Number of Flights"),
        col("firstName").as("First name"),
        col("lastName").as("Last name")
      )
      .orderBy(col("Number of Flights").desc)
      .limit(100)
  }

  /**
   * Question 3: Find the greatest number of countries a passenger has been in without being in the UK.
   * For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK,
   * the correct answer would be 3 countries.
   *
   * @param flightData Dataset of Flight objects
   * @return DataFrame with columns (Passenger ID, Longest Run)
   */
  def longestRunWithoutUK(flightData: Dataset[Flight]): DataFrame = {
    // Register a temporary view for SQL query
    flightData.createOrReplaceTempView("flights")
    
    // Use SQL for window functions to find consecutive runs
    val result = spark.sql("""
      WITH flights_with_country AS (
        -- Get all countries visited (both departure and destination)
        SELECT 
          passengerId, 
          flightId,
          date,
          from as country,
          'from' as type
        FROM flights
        UNION ALL
        SELECT 
          passengerId, 
          flightId,
          date,
          to as country,
          'to' as type
        FROM flights
      ),
      ordered_countries AS (
        -- Order countries by passenger and date
        SELECT 
          passengerId,
          country,
          date,
          -- Create a flag when country is UK
          CASE WHEN country = 'UK' THEN 1 ELSE 0 END as is_uk
        FROM flights_with_country
        ORDER BY passengerId, date
      ),
      uk_groups AS (
        -- Assign group IDs for runs between UK visits
        SELECT 
          passengerId,
          country,
          date,
          is_uk,
          SUM(is_uk) OVER (PARTITION BY passengerId ORDER BY date) as uk_group
        FROM ordered_countries
      ),
      country_runs AS (
        -- Count distinct countries in each run
        SELECT
          passengerId,
          uk_group,
          COUNT(DISTINCT country) - SUM(is_uk) as countries_in_run
        FROM uk_groups
        GROUP BY passengerId, uk_group
      ),
      max_runs AS (
        -- Find max run for each passenger
        SELECT
          passengerId,
          MAX(countries_in_run) as longest_run
        FROM country_runs
        GROUP BY passengerId
      )
      -- Final result
      SELECT 
        passengerId as `Passenger ID`,
        longest_run as `Longest Run`
      FROM max_runs
      ORDER BY longest_run DESC
    """)
    
    result
  }

  /**
   * Question 4: Find the passengers who have been on more than 3 flights together.
   *
   * @param flightData Dataset of Flight objects
   * @return DataFrame with columns (Passenger 1 ID, Passenger 2 ID, Number of flights together)
   */
  def passengersFlownTogether(flightData: Dataset[Flight], minFlights: Int = 3): DataFrame = {
    // Self-join flights on flightId to find pairs of passengers on the same flight
    val flightPairs = flightData.as("f1")
      .join(flightData.as("f2"), 
        col("f1.flightId") === col("f2.flightId") && 
        col("f1.passengerId") < col("f2.passengerId")) // Ensure we don't double count
      .select(
        col("f1.passengerId").as("passenger1Id"),
        col("f2.passengerId").as("passenger2Id"),
        col("f1.flightId")
      )
    
    // Count how many flights each pair has been on together
    val flightsTogether = flightPairs
      .groupBy("passenger1Id", "passenger2Id")
      .agg(count("flightId").as("flightsTogether"))
      .filter(col("flightsTogether") > minFlights)
      .select(
        col("passenger1Id").as("Passenger 1 ID"),
        col("passenger2Id").as("Passenger 2 ID"),
        col("flightsTogether").as("Number of flights together")
      )
      .orderBy(col("Number of flights together").desc)
    
    flightsTogether
  }

  /**
   * Extra Question: Find the passengers who have been on more than N flights together within the range (from, to).
   *
   * @param flightData Dataset of Flight objects
   * @param atLeastNTimes Minimum number of flights together
   * @param from Start date (inclusive)
   * @param to End date (inclusive)
   * @return DataFrame with columns (Passenger 1 ID, Passenger 2 ID, Number of flights together, From, To)
   */
  def flownTogether(
    flightData: Dataset[Flight], 
    atLeastNTimes: Int, 
    from: Date, 
    to: Date
  ): DataFrame = {
    // Convert string dates to Date objects for comparison
    val filteredFlights = flightData
      .filter(to_date(col("date"), "yyyy-MM-dd").between(lit(from), lit(to)))
    
    // Self-join flights on flightId to find pairs of passengers on the same flight
    val flightPairs = filteredFlights.as("f1")
      .join(filteredFlights.as("f2"), 
        col("f1.flightId") === col("f2.flightId") && 
        col("f1.passengerId") < col("f2.passengerId")) // Ensure we don't double count
      .select(
        col("f1.passengerId").as("passenger1Id"),
        col("f2.passengerId").as("passenger2Id"),
        col("f1.flightId")
      )
    
    // Count how many flights each pair has been on together
    val flightsTogether = flightPairs
      .groupBy("passenger1Id", "passenger2Id")
      .agg(count("flightId").as("flightsTogether"))
      .filter(col("flightsTogether") >= atLeastNTimes)
      .select(
        col("passenger1Id").as("Passenger 1 ID"),
        col("passenger2Id").as("Passenger 2 ID"),
        col("flightsTogether").as("Number of flights together"),
        lit(from.toString).as("From"),
        lit(to.toString).as("To")
      )
      .orderBy(col("Number of flights together").desc)
    
    flightsTogether
  }
} 