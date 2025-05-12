package com.quantexa.solution

import com.quantexa.solution.models.{Flight, Passenger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Date

class FlightAnalyserSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  // Create Spark session for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("FlightAnalyserSpec")
    .master("local[*]")
    .getOrCreate()
  
  import spark.implicits._
  
  // Create test data
  val testFlights: Dataset[Flight] = Seq(
    // Normal case - multiple flights
    Flight(1, 101, "UK", "FR", "2017-01-15"),
    Flight(1, 102, "FR", "DE", "2017-02-20"),
    Flight(1, 103, "DE", "IT", "2017-03-10"),
    Flight(1, 104, "IT", "UK", "2017-04-05"),
    Flight(1, 105, "UK", "ES", "2017-05-15"),
    Flight(1, 106, "ES", "UK", "2017-06-20"),
    
    // Edge case - passenger never visits UK
    Flight(2, 201, "FR", "DE", "2017-01-10"),
    Flight(2, 202, "DE", "IT", "2017-02-15"),
    Flight(2, 203, "IT", "ES", "2017-03-20"),
    
    // Edge case - passenger always in UK
    Flight(3, 301, "UK", "UK", "2017-01-05"),
    Flight(3, 302, "UK", "UK", "2017-02-10"),
    
    // Passengers flying together
    Flight(4, 401, "UK", "FR", "2017-01-15"),
    Flight(5, 401, "UK", "FR", "2017-01-15"),
    Flight(4, 402, "FR", "DE", "2017-02-20"),
    Flight(5, 402, "FR", "DE", "2017-02-20"),
    Flight(4, 403, "DE", "IT", "2017-03-10"),
    Flight(5, 403, "DE", "IT", "2017-03-10"),
    Flight(4, 404, "IT", "UK", "2017-04-05"),
    Flight(5, 404, "IT", "UK", "2017-04-05"),
    
    // Another pair flying together less frequently
    Flight(6, 401, "UK", "FR", "2017-01-15"),
    Flight(7, 401, "UK", "FR", "2017-01-15"),
    Flight(6, 402, "FR", "DE", "2017-02-20"),
    Flight(7, 402, "FR", "DE", "2017-02-20")
  ).toDS()
  
  val testPassengers: Dataset[Passenger] = Seq(
    Passenger(1, "John", "Doe"),
    Passenger(2, "Jane", "Smith"),
    Passenger(3, "Bob", "Johnson"),
    Passenger(4, "Alice", "Brown"),
    Passenger(5, "Charlie", "Davis"),
    Passenger(6, "Eve", "Wilson"),
    Passenger(7, "Frank", "Miller")
  ).toDS()
  
  // Create analyser
  val analyser = new FlightAnalyser(spark)
  
  // Test for Question 1: Flights per month
  "flightsPerMonth" should "count distinct flights correctly for each month" in {
    val result = analyser.flightsPerMonth(testFlights)
    
    // Expected: Jan (3), Feb (3), Mar (2), Apr (1), May (1), Jun (1)
    // Note: Multiple passengers on same flight should count as 1 flight
    result.collect().length should be(6)
    
    val resultMap = result.collect().map(row => (row.getAs[Int]("Month"), row.getAs[Long]("Number of Flights"))).toMap
    resultMap(1) should be(3) // January: 301, 401, 201
    resultMap(2) should be(3) // February: 302, 402, 202
    resultMap(3) should be(2) // March: 403, 203
    resultMap(4) should be(1) // April: 404
    resultMap(5) should be(1) // May: 105
    resultMap(6) should be(1) // June: 106
  }
  
  // Test for Question 2: Frequent flyers
  "frequentFlyers" should "identify passengers with most flights" in {
    val result = analyser.frequentFlyers(testFlights, testPassengers)
    
    // Expected order: Passenger 1 (6 flights), Passenger 4 & 5 (4 flights each), etc.
    val resultArray = result.collect()
    
    resultArray.length should be(7) // All 7 test passengers
    
    // Check the top passenger
    resultArray(0).getAs[Int]("Passenger ID") should be(1)
    resultArray(0).getAs[Long]("Number of Flights") should be(6)
    resultArray(0).getAs[String]("First name") should be("John")
    
    // Check the limit works
    val limitedResult = analyser.frequentFlyers(testFlights, testPassengers).limit(3)
    limitedResult.count() should be(3)
  }
  
  // Test for Question 3: Longest run without UK
  "longestRunWithoutUK" should "calculate correct run lengths" in {
    val result = analyser.longestRunWithoutUK(testFlights)
    
    val resultMap = result.collect().map(row => 
      (row.getAs[Int]("Passenger ID"), row.getAs[Long]("Longest Run"))).toMap
    
    // Passenger 1: UK -> FR -> DE -> IT -> UK, so longest run is 3
    resultMap(1) should be(3)
    
    // Passenger 2: FR -> DE -> IT -> ES, never visits UK, so longest run is 4
    resultMap(2) should be(4)
    
    // Passenger 3: Always in UK, so longest run is 0
    resultMap(3) should be(0)
    
    // Check ordering
    val resultArray = result.collect()
    resultArray(0).getAs[Int]("Passenger ID") should be(2) // Passenger 2 has longest run
    resultArray(1).getAs[Int]("Passenger ID") should be(1) // Passenger 1 has second longest
  }
  
  // Test for Question 4: Passengers flown together
  "passengersFlownTogether" should "find passengers who flew together multiple times" in {
    val result = analyser.passengersFlownTogether(testFlights, 3)
    
    // Only one pair flew together more than 3 times: passengers 4 & 5 (4 times)
    result.count() should be(1)
    
    val row = result.collect()(0)
    row.getAs[Int]("Passenger 1 ID") should be(4)
    row.getAs[Int]("Passenger 2 ID") should be(5)
    row.getAs[Long]("Number of flights together") should be(4)
    
    // Test with lower threshold
    val resultWithLowerThreshold = analyser.passengersFlownTogether(testFlights, 2)
    resultWithLowerThreshold.count() should be(2) // Should include passengers 6 & 7 now
  }
  
  // Test for Extra Question: Flown together within date range
  "flownTogether" should "find passengers who flew together within date range" in {
    val startDate = Date.valueOf("2017-01-01")
    val endDate = Date.valueOf("2017-02-28")
    
    val result = analyser.flownTogether(testFlights, 2, startDate, endDate)
    
    // Two pairs flew together twice in Jan-Feb: 4&5 and 6&7
    result.count() should be(2)
    
    // Test with different date range
    val laterStartDate = Date.valueOf("2017-03-01")
    val laterEndDate = Date.valueOf("2017-04-30")
    
    val laterResult = analyser.flownTogether(testFlights, 2, laterStartDate, laterEndDate)
    
    // Only one pair flew together twice in Mar-Apr: 4&5
    laterResult.count() should be(1)
    val row = laterResult.collect()(0)
    row.getAs[Int]("Passenger 1 ID") should be(4)
    row.getAs[Int]("Passenger 2 ID") should be(5)
    row.getAs[Long]("Number of flights together") should be(2)
  }
  
  // Clean up after tests
  override def afterAll(): Unit = {
    spark.stop()
  }
} 