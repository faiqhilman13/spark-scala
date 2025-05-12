package com.quantexa.solution.models

/**
 * Represents a flight record from flightData.csv
 *
 * @param passengerId Integer representing the id of a passenger
 * @param flightId Integer representing the id of a flight
 * @param from String representing the departure country
 * @param to String representing the destination country
 * @param date String representing the date of a flight
 */
case class Flight(
  passengerId: Int,
  flightId: Int,
  from: String,
  to: String,
  date: String
) 