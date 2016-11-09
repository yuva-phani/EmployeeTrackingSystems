package com.ilimi.employeetrack.service

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext

import com.datastax.spark.connector._
import com.datastax.spark.connector.toSparkContextFunctions
import com.ilimi.employeetrack.utils.DateTimeUtils.calculateWeekDays
import com.ilimi.employeetrack.utils.DateTimeUtils.epocDateTimeToTimeInEpoch
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDay
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDayWithMonth
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDayWithWeek
import com.ilimi.employeetrack.utils.PropertyReader
import com.ilimi.employeetrack.sqloperations.SqlOperations
import java.util.Arrays
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer
import java.io.IOException
import java.sql.SQLException
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.ilimi.employeetrack.datageneration.DataGeneration
import com.ilimi.employeetrack.utils.SparkContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Floor
import com.ilimi.employeetrack.exception.CassandraReadException

//import sqlContext.implicits._

case class EmployeeTrackingSystem(empid: String, date: String, timeinoffice: Long, firstlogintime: Long)
case class GeneratedData(empid: String, logintimeinepochformat: Long, logouttimeinepochformat: Long)

object TrackingService extends App {

  val sc = SparkContextUtil.sparkcontext

  var employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem] = null

  def saveToCassandra(sc: SparkContext, fileName: String) {

    try {
      //reading file from local path
      val timeInOffice = sc.textFile(PropertyReader.getProperty(fileName)).map(_.split(",")).map { x => GeneratedData(x(0), x(1).toLong, x(2).toLong) }

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
      employeeTrackingSystemvalues.saveToCassandra(PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))
    } catch {

      case e: IllegalArgumentException => println("illegal arg. exception " + e.printStackTrace());
      case e: IllegalStateException    => println("illegal state exception " + e.printStackTrace());
      case e: IOException              => println("IO exception " + e.printStackTrace());
      case e: SQLException             => println("SQL exception " + e.printStackTrace());
      case unknown                     => println("Exception  " + unknown)

    }
  }
@throws(classOf[CassandraReadException])
  def readingDataFromCassandra(sc: SparkContext): CassandraTableScanRDD[EmployeeTrackingSystem] = {
    //reading data from Cassandra
    val read = sc.cassandraTable[EmployeeTrackingSystem](PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))
    if (read == null) {
      throw new CassandraReadException("not getting data from cassandra table")
    }
    return read
  }

  // ***************************** calculating expected Arrival time to office ***************************
  def expectedArrivalTime(employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem]): DataFrame = {
    val sqlContext = SparkContextUtil.sqlContext(sc)
    import sqlContext.implicits._
    val selectingEmpidAndFirstLoginTime = employeeTrackingSystemReadFromCassandraTable.map { x => (x.empid, x.firstlogintime) }

    //group by empid and then sort according to list of first login time
    val sortFirstLogintimeListGroupByEmpid = selectingEmpidAndFirstLoginTime.groupBy(f => f._1).mapValues(f => (f.map(_._2).toList.sortBy(f => f).distinct))

    //calculating median logic 
    val expectedArrivalEachEmp = sortFirstLogintimeListGroupByEmpid.map(f => (f._1,

      //if size is even then (mid1+mid2)/2 else mid
      if ((f._2.size - 1) % 2 == 0) (f._2(((f._2.size - 1) / 2)) + f._2(((f._2.size - 1) / 2) + 1)) / 2 else f._2(((f._2.size - 1) / 2) + 1) //median login 
      ))
    return expectedArrivalEachEmp.toDF()
  }
  //*********************************** End ****************************************************************   

} 

