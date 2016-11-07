package com.ilimi.employeetrack.test

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.service.TrackingService
import com.ilimi.employeetrack.main.SparkContextUtil
import com.ilimi.employeetrack.sqloperations.SqlOperations._
import com.ilimi.employeetrack.utils.DateTimeUtils._
import com.ilimi.employeetrack.datageneration.DataGeneration

class TestSqlOperations extends FlatSpec {

  val sparkContext = SparkContextUtil.sparkcontext
  //test data generation
  DataGeneration.dataGeneration("fileNameTest",5,"startDateTest","endDateTest")

  TrackingService.saveToCassandra(sparkContext, "fileNameTest")

  //reading from casssandra
  val readFromCassandra = TrackingService.readingDataFromCassandra(sparkContext)

  //check avg time per week for empid 2
  "Avg time per Week" should "match" in {

    val averageTimeWeek = averageTimePerWeek(sparkContext, readFromCassandra).rdd

    val avgTime = averageTimeWeek.map { x => (x(0), x(1)) }
    val avgtimeMapWeek = avgTime.collect().toMap

    assert(avgtimeMapWeek.get("2").get == 59788.5)

  }

  //check avg time per month for empid 2
  "Avg time per month" should "match" in {

    val averageTimeMonth = averageTimePerMonth(sparkContext, readFromCassandra).rdd
    val avgTime = averageTimeMonth.map { x => (x(0), x(1)) }
    val avgtimeMapMonth = avgTime.collect().toMap

    assert(avgtimeMapMonth.get("2").get == 119577)

  }

  //check employee absent for empid 2 
  "Employee absent" should "match" in {

    val employeeAbsents = employeeAbsent(sparkContext, readFromCassandra).rdd
    val totalWeekDays = calculateWeekDays("2016-10-20", "2016-11-04")
    val absnt = employeeAbsents.map { x => (x.getAs[String](0), totalWeekDays.toLong - x.getAs[Long](1)) }
    val keys = absnt.collect().toMap
    assert(keys.getOrElse("2", 0) == 9L)

  }

}