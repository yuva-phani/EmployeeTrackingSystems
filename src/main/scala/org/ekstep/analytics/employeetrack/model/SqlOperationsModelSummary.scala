package org.ekstep.analytics.employeetrack.model

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.employeetrack.exception.SqlOperationException
import org.ekstep.analytics.employeetrack.models._
import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.models.SqlOperation
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.calculateWeekDays
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocDateTimeToTimeInEpoch
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDay
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDayWithMonth
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDayWithWeek
import org.ekstep.analytics.employeetrack.utils.SparkContextUtil
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.DerivedEvent

import com.datastax.spark.connector._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.toSparkContextFunctions
import scala.tools.nsc.transform.Flatten

@throws(classOf[SqlOperationException])
object SqlOperationsModelSummary extends IBatchModelTemplate[EmployeeTrackingSystemWithExpectedTimeInOffice, EmployeeTrackingSystemWithExpectedTimeInOffice, SqlOperation, SqlOperation] with Serializable {

  val className = "org.ekstep.analytics.employeetrack.model.SqlOperationsModelSummary"
  override def name: String = "SqlOperationsModelSummary"

  override def preProcess(data: RDD[EmployeeTrackingSystemWithExpectedTimeInOffice], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EmployeeTrackingSystemWithExpectedTimeInOffice] = {

    return data
  }

  override def algorithm(timeInOffice: RDD[EmployeeTrackingSystemWithExpectedTimeInOffice], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[SqlOperation] = {
    println("inside algorithm first")
    val sqlContext = SparkContextUtil.sqlContext(sc)
    import sqlContext.implicits._
    val df = timeInOffice.toDF

    //creating temporary view
    df.createOrReplaceTempView("employeetrackingsystem")
    val employeeAbsent = sqlContext.sql("select  empid,count(date) as absent from employeetrackingsystem  where date like '%%%%-%%-%%' group by empid ").rdd

    if (employeeAbsent == null) {
      throw new SqlOperationException("sqlOperation exception");
    }

    val totalWeekDays = calculateWeekDays("2016-10-20", "2016-11-04")
    val absnt = employeeAbsent.map { x => (x.getAs[String](0), totalWeekDays.toLong - x.getAs[Long](1)) }
    //absnt.toDF()

    val sqlResult = sqlContext.sql("select empid,avg(timeinoffice) as averagetimepermonth  from employeetrackingsystem  where date like '____-__m' group by empid ").rdd.map { x => (x.getAs[String](0), x.getAs[Long](1)) }

    //return sqlResult
    val sqlResult1 = sqlContext.sql("select empid,avg(timeinoffice) as averagetimeperweek from employeetrackingsystem  where date like '%%%%-%% %%w' group by empid ").rdd.map { x => (x.getAs[String](0), x.getAs[Long](1)) }

    val joins = sqlResult.join(sqlResult1)
    
    
    joins.foreach(f=>println(f))
    println("After first join...........................")
    val joins2 = absnt.join(joins).map { case (a, (b, (c, d))) => (a, b, c, d) }
   
   
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx......................"+joins2)
     joins2.foreach(f=>println(f._1))
    val mapingCaseclass = joins2.map(f => SqlOperation(f._1, f._2, f._3, f._4))
    
    println(mapingCaseclass+"             ............mapingCaseclass")
    println("inside algorithm second")
    return mapingCaseclass
  }

  override def postProcess(employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra: RDD[SqlOperation], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[SqlOperation] = {
    println("post process first")
    return employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra

  }

}
