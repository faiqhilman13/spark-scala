package com.quantexa.solution.models

/**
 * Represents a passenger record from passengers.csv
 *
 * @param passengerId Integer representing the id of a passenger
 * @param firstName String representing the first name of a passenger
 * @param lastName String representing the last name of a passenger
 */
case class Passenger(
  passengerId: Int,
  firstName: String,
  lastName: String
) 