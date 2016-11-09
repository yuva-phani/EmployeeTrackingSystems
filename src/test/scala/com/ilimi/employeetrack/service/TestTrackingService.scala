package com.ilimi.employeetrack.service

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.datageneration.DataGeneration
import com.ilimi.employeetrack.utils.SparkContextUtil

object TestTrackingService extends FlatSpec{
   val sparkContext = SparkContextUtil.sparkcontext
  //test data generation

  DataGeneration.dataGeneration("fileNameTest",5,"startDateTest","endDateTest")
  TrackingService.saveToCassandra(sparkContext, "fileNameTest")

  //reading from casssandra
  val readFromCassandra = TrackingService.readingDataFromCassandra(sparkContext)

  //check expected time for empid2
  "Employee expected time" should "match" in {

    val expectedTime = TrackingService.expectedArrivalTime(readFromCassandra).rdd
    val expectedTimeRdd = expectedTime.map { x => (x(0), x(1)) }

    val keys = expectedTimeRdd.collect().toMap

    assert(keys.get("2").get == 34590)

  }
}