package com.ilimi.employeetrack.sqloperations
import com.ilimi.employeetrack.service.TrackingService
import com.datastax.spark.connector._
import com.datastax.spark.connector.toSparkContextFunctions
import com.ilimi.employeetrack.service.EmployeeTrackingSystem
import com.ilimi.employeetrack.utils.PropertyReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import com.ilimi.employeetrack.utils.DateTimeUtils.calculateWeekDays
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.ilimi.employeetrack.main.SparkContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame

object SqlOperations {

  def employeeAbsent(sparkContext: SparkContext, employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem]): DataFrame = {
    val sqlContext = SparkContextUtil.sqlContext(sparkContext)
    val df = SparkContextUtil.sqlcontextAndConvertToDataFrame(sparkContext, employeeTrackingSystemReadFromCassandraTable)
    import sqlContext.implicits._
    //creating temporary view
    df.createOrReplaceTempView("employeetrackingsystem")
    val employeeAbsent = sqlContext.sql("select  empid,count(date) as absent from employeetrackingsystem  where date like '%%%%-%%-%%' group by empid ").rdd
    val totalWeekDays = calculateWeekDays("2016-10-20", "2016-11-04")
    val absnt = employeeAbsent.map { x => (x.getAs[String](0), totalWeekDays.toLong - x.getAs[Long](1)) }
    absnt.toDF()

  }

  def averageTimePerMonth(sparkContext: SparkContext, employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem]): DataFrame = {

    val sqlContext = SparkContextUtil.sqlContext(sparkContext)
    val df = SparkContextUtil.sqlcontextAndConvertToDataFrame(sparkContext, employeeTrackingSystemReadFromCassandraTable)
    //creating temporary view
    df.createOrReplaceTempView("employeetrackingsystem")

    return sqlContext.sql("select empid,avg(timeinoffice) as averagetimepermonth  from employeetrackingsystem  where date like '____-__m' group by empid ")

  }
  def averageTimePerWeek(sparkContext: SparkContext, employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem]): DataFrame = {
    val sqlContext = SparkContextUtil.sqlContext(sparkContext)
    val df = SparkContextUtil.sqlcontextAndConvertToDataFrame(sparkContext, employeeTrackingSystemReadFromCassandraTable)

    //creating temporary view
    df.createOrReplaceTempView("employeetrackingsystem")
    return sqlContext.sql("select empid,avg(timeinoffice) as averagetimeperweek from employeetrackingsystem  where date like '%%%%-%% %%w' group by empid ")

  }

}