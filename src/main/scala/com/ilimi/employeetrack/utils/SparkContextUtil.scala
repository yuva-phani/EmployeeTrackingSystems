package com.ilimi.employeetrack.utils

import com.ilimi.employeetrack.utils.PropertyReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.ilimi.employeetrack.service.EmployeeTrackingSystem
import org.apache.spark.sql.DataFrame
object SparkContextUtil {

  
   def sqlContext(sparkcontext: SparkContext) = sql(sparkcontext)

  def sparkcontext: SparkContext = {
    val configuration = new SparkConf(true).set("spark.cassandra.connection.host", PropertyReader.getProperty("ipAddress")).setMaster(PropertyReader.getProperty("master"))
    val sc = new SparkContext("local", "test", configuration)
    return sc
  }

  def sqlcontextAndConvertToDataFrame(sparkContext: SparkContext, employeeTrackingSystemReadFromCassandraTable: CassandraTableScanRDD[EmployeeTrackingSystem]): DataFrame = {

    val sqlContext = sql(sparkContext)
    import sqlContext.implicits._
    val df = employeeTrackingSystemReadFromCassandraTable.toDF()
    return df
  }
  private def sql(sparkContext: SparkContext): SQLContext = {

    val sqlContext = new SQLContext(sparkContext)
    return sqlContext

  }

}
