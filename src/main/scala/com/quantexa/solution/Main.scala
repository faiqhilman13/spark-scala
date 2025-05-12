package com.quantexa.solution

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.Date

/**
 * Main application that runs the flight analysis and outputs results
 */
object Main {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Quantexa Flight Analysis")
      .master("local[*]")
      .getOrCreate()
    
    // Set log level to reduce console output
    spark.sparkContext.setLogLevel("WARN")
    
    println("Starting Quantexa Flight Analysis...")
    
    // Define input paths
    val flightDataPath = if (args.length > 0) args(0) else "data/flightData.csv"
    val passengersPath = if (args.length > 1) args(1) else "data/passengers.csv"
    
    // Create FlightAnalyser
    val analyser = new FlightAnalyser(spark)
    
    try {
      // Load data
      println("Loading flight data from " + flightDataPath)
      val flightData = analyser.loadFlightData(flightDataPath)
      println(s"Loaded ${flightData.count()} flight records")
      
      println("Loading passenger data from " + passengersPath)
      val passengerData = analyser.loadPassengerData(passengersPath)
      println(s"Loaded ${passengerData.count()} passenger records")
      
      // Question 1: Flights per month
      println("\nQ1: Calculating flights per month...")
      val q1Result = analyser.flightsPerMonth(flightData)
      q1Result.show()
      q1Result.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/q1_flights_per_month.csv")
      println("Q1 results saved to output/q1_flights_per_month.csv")
      
      // Question 2: Top 100 frequent flyers
      println("\nQ2: Finding the 100 most frequent flyers...")
      val q2Result = analyser.frequentFlyers(flightData, passengerData)
      q2Result.show(10) // Show only top 10 for console output
      q2Result.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/q2_frequent_flyers.csv")
      println("Q2 results saved to output/q2_frequent_flyers.csv")
      
      // Question 3: Longest run without UK
      println("\nQ3: Finding longest runs without visiting UK...")
      val q3Result = analyser.longestRunWithoutUK(flightData)
      q3Result.show(10) // Show only top 10 for console output
      q3Result.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/q3_longest_run_no_uk.csv")
      println("Q3 results saved to output/q3_longest_run_no_uk.csv")
      
      // Question 4: Passengers who have flown together more than 3 times
      println("\nQ4: Finding passengers who have flown together more than 3 times...")
      val q4Result = analyser.passengersFlownTogether(flightData)
      q4Result.show(10) // Show only top 10 for console output
      q4Result.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/q4_flights_together.csv")
      println("Q4 results saved to output/q4_flights_together.csv")
      
      // Extra: Passengers who have flown together within date range
      println("\nExtra: Finding passengers who have flown together within date range...")
      val startDate = Date.valueOf("2017-01-01")
      val endDate = Date.valueOf("2017-12-31")
      val extraResult = analyser.flownTogether(flightData, 5, startDate, endDate)
      extraResult.show(10) // Show only top 10 for console output
      extraResult.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/q_extra_flights_together_daterange.csv")
      println("Extra results saved to output/q_extra_flights_together_daterange.csv")
      
      println("\nAnalysis complete!")
    } catch {
      case e: Exception => 
        println(s"Error during analysis: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop Spark session
      spark.stop()
    }
  }
} 