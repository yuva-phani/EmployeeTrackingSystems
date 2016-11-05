package com.ilimi.employeetrack.test

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.service.TrackingService
import com.ilimi.employeetrack.main.SparkContextUtil

class TestTrackingSer extends FlatSpec {
  val sparkContext = SparkContextUtil.sparkcontext
  //test data generation
  DataGeneration.dataGeneration

  TrackingService.saveToCassandra(sparkContext, "fileNameTest")

  //reading from casssandra
  val readFromCassandra = TrackingService.readingDataFromCassandra(sparkContext)

  //check expected time for empid2
  "Employee expected time" should "match" in {

    val expectedTime = TrackingService.expectedArrivalTime(readFromCassandra).rdd
    val expectedTimeRdd = expectedTime.map { x => (x(0), x(1)) }

    val keys = expectedTimeRdd.collect().toMap

    assert(keys.get("2").get == 34045)

  }

}