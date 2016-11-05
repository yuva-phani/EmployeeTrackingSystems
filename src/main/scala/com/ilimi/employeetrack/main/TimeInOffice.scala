package com.ilimi.employeetrack.main

import com.ilimi.employeetrack.service.TrackingService
import com.ilimi.employeetrack.datageneration.DataGeneration
import com.ilimi.employeetrack.sqloperations.SqlOperations

object TimeInOffice {
  def main(args: Array[String]) {
    //data generation
    DataGeneration.dataGeneration
    //tracking service and loading data to cassandra table
    val sparkContext = SparkContextUtil.sparkcontext
    TrackingService.saveToCassandra(sparkContext,"fileName")

    //read from cassandra
    val readDataFromCassandra = TrackingService.readingDataFromCassandra(sparkContext)
    println(readDataFromCassandra.first())

    //expected arrival time
    val expectedArrivalTime = TrackingService.expectedArrivalTime(readDataFromCassandra)
    println(expectedArrivalTime.show())
    //employee absent
    val employeeAbsent = SqlOperations.employeeAbsent(sparkContext, readDataFromCassandra)

    //employee avg time per month
    val employeePerMonth = SqlOperations.averageTimePerMonth(sparkContext, readDataFromCassandra)

    //employee avg time per week
    val employeePerWeek = SqlOperations.averageTimePerWeek(sparkContext, readDataFromCassandra)

    println(employeePerWeek.show())

  }
}


