package com.ilimi.employeetracker.service

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.toSparkContextFunctions
import com.ilimi.employeetracker.utils.DateTimeUtils.calculateWeekDays
import com.ilimi.employeetracker.utils.DateTimeUtils.epocDateTimeToTimeInEpoch
import com.ilimi.employeetracker.utils.DateTimeUtils.epocTimeToDay
import com.ilimi.employeetracker.utils.DateTimeUtils.epocTimeToDayWithMonth
import com.ilimi.employeetracker.utils.DateTimeUtils.epocTimeToDayWithWeek
import com.ilimi.employeetracker.utils.PropertyReader

//import sqlContext.implicits._

case class EmployeeTrackingSystem(empid: String, date: String, timeinoffice: Long, firstlogintime: Long)
case class GeneratedData(empid: String, logintimeinepochformat: Long, logouttimeinepochformat: Long)

object TrackingService  {

  val configuration = new SparkConf(true).set("spark.cassandra.connection.host", PropertyReader.getProperty("ipAddress")).setMaster(PropertyReader.getProperty("master"))
  val sc = new SparkContext("local", "test", configuration)

  //reading file from local path
  val timeInOffice = sc.textFile("tracking2.csv").map(_.split(",")).map { x => GeneratedData(x(0), x(3).toLong, x(4).toLong) }

  //time in office  day having empid,day,total time,firstlogin time
  val timeInOfficeWithPeriodDay = timeInOffice.map { x => (x.empid, epocTimeToDay(x.logintimeinepochformat), (x.logouttimeinepochformat - x.logintimeinepochformat), epocDateTimeToTimeInEpoch(x.logintimeinepochformat)) }

  ////time in office  week having empid,day,total time,firstlogin time value is zero
  val timeInOfficeWithPeriodWeek = timeInOffice.map { x => (x.empid, epocTimeToDayWithWeek(x.logintimeinepochformat), (x.logouttimeinepochformat - x.logintimeinepochformat), 0L) }

  //time in office  month having empid,day,total time,firstlogin time value is zero
  val timeInOfficeWithPeriodMonth = timeInOffice.map { x => (x.empid, epocTimeToDayWithMonth(x.logintimeinepochformat), (x.logouttimeinepochformat - x.logintimeinepochformat), 0L) }

  //combining all three RDDs
  val totalTimeInOffice = timeInOfficeWithPeriodDay ++ timeInOfficeWithPeriodWeek ++ timeInOfficeWithPeriodMonth

  //group by empid and date , summing total time for each emp with date and finding first login time i,e min time for each day
  val timeInOfficeWithPeriodGroupByEmpDate = totalTimeInOffice.groupBy { x => (x._1, x._2) }.mapValues(f => (f.map(_._1).head, f.map(_._2).head, f.map(_._3).sum, f.map(_._4).min)).map(f => f._2)

  //mapping data with case class EmployeeTrackingSystem
  val employeeTrackingSystemvalues = timeInOfficeWithPeriodGroupByEmpDate.map(f => EmployeeTrackingSystem(f._1, f._2, f._3, f._4))

  //saving data to cassandra
  employeeTrackingSystemvalues.saveToCassandra(PropertyReader.getProperty("keySpace"),PropertyReader.getProperty("table"))
   
  //reading data from Cassandra
  val employeeTrackingSystemReadFromCassandraTable = sc.cassandraTable[EmployeeTrackingSystem](PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))

  
  // ***************************** calculating expected Arrival time to office ***************************

  val selectingEmpidAndFirstLoginTime = employeeTrackingSystemReadFromCassandraTable.map { x => (x.empid, x.firstlogintime) }

  //group by empid and then sort according to list of first login time
  val sortFirstLogintimeListGroupByEmpid = selectingEmpidAndFirstLoginTime.groupBy(f => f._1).mapValues(f => (f.map(_._2).toList.sortBy(f => f).distinct))

  //calculating median logic 
  val expectedArrival = sortFirstLogintimeListGroupByEmpid.map(f => (f._1,
    
      //if size is even then (mid1+mid2)/2 else mid
    if (f._2.size % 2 == 0) (f._2((f._2.size / 2)) + f._2(((f._2.size ) / 2)-1))/2  else f._2(f._2.size / 2) //median login 
    ))

  //*********************************** End ****************************************************************   

  //********************************** Spark SQL Queries ****************************************************    

  //creating sql Context
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val df = employeeTrackingSystemReadFromCassandraTable.toDF()

  //creating temporary view
  df.createOrReplaceTempView("employeetrackingsystem")

  //creating UDF for WeekDays Count
  def weekdaysCount(fromDate: String, toDate: String): Int = calculateWeekDays("2016-08-01", "2016-10-07")

  //registering UDF 
  sqlContext.udf.register("weekdaysCount", weekdaysCount(_: String, _: String))

  //calculate Employee Absent
  val employeeAbsent = sqlContext.sql("select  empid,weekdaysCount(2016-08-01,2016-10-07)-count(date)as absent from employeetrackingsystem  where date like '%%%%-%%-%%' group by empid ")
  employeeAbsent.show()

  //calculate avg time per month in office
  val averageTimePerMonth = sqlContext.sql("select empid,avg(timeinoffice) as averagetimepermonth from employeetrackingsystem  where date like '%%%%-%%' group by empid ")
  averageTimePerMonth.show()

  //calculate avg time per week in office
  val averageTimePerWeek = sqlContext.sql("select empid,avg(timeinoffice) as averagetimeperweek from employeetrackingsystem  where date like '%%%%-%% %%w' group by empid ")
  averageTimePerWeek.show()

  //**************************************** End *******************************************************************  
}
