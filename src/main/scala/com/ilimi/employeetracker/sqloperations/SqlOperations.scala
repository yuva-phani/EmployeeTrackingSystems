package com.ilimi.employeetracker.sqloperations
import com.ilimi.employeetracker.service.TrackingService
import com.datastax.spark.connector._
import com.datastax.spark.connector.toSparkContextFunctions
import com.ilimi.employeetracker.service.EmployeeTrackingSystem
import com.ilimi.employeetracker.utils.PropertyReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import com.ilimi.employeetracker.utils.DateTimeUtils.calculateWeekDays
object SqlOperations {
  
  
  
  val sparkContext=TrackingService.sc
  //Reading data from Cassandra
  val employeeTrackingSystemReadFromCassandraTable = sparkContext.cassandraTable[EmployeeTrackingSystem](PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))

  //creating sql Context
  val sqlContext = new SQLContext(sparkContext)
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
  

  //calculate avg time per month in office
  val averageTimePerMonth = sqlContext.sql("select empid,avg(timeinoffice) as averagetimepermonth from employeetrackingsystem  where date like '%%%%-%%' group by empid ")
 

  //calculate avg time per week in office
  val averageTimePerWeek = sqlContext.sql("select empid,avg(timeinoffice) as averagetimeperweek from employeetrackingsystem  where date like '%%%%-%% %%w' group by empid ")
 
  
  
  
}
