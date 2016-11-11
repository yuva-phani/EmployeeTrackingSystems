package org.ekstep.analytics.employeetrack.model

import org.ekstep.analytics.framework.IBatchModelTemplate

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.employeetrack.utils.PropertyReader
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocDateTimeToTimeInEpoch
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDay
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDayWithMonth
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils.epocTimeToDayWithWeek
import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.utils.PropertyReader
import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.models.Event3

import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystem
object TimeInOfficeSummaryModel extends IBatchModelTemplate[Event3, Event3, EmployeeTrackingSystemWithExpectedTimeInOffice, EmployeeTrackingSystemWithExpectedTimeInOffice] with Serializable {

  val className = "org.ekstep.analytics.employeetrack.model.TimeInOfficeSummaryModel"
  override def name: String = "TimeInOfficeSummaryModel"
  override def preProcess(data: RDD[Event3], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Event3] = {

    return data
  }

  override def algorithm(timeInOffice: RDD[Event3], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EmployeeTrackingSystemWithExpectedTimeInOffice] = {

    //time in office  day having empid,day,total time,firstlogin time
    val timeInOfficeWithPeriodDay = timeInOffice.map { x => (x.empId, epocTimeToDay(x.loginTime), (x.logoutTime - x.loginTime), epocDateTimeToTimeInEpoch(x.loginTime)) }

    ////time in office  week having empid,day,total time,firstlogin time value is zero
    val timeInOfficeWithPeriodWeek = timeInOffice.map { x => (x.empId, epocTimeToDayWithWeek(x.loginTime), (x.logoutTime - x.loginTime), 0L) }

    //time in office  month having empid,day,total time,firstlogin time value is zero
    val timeInOfficeWithPeriodMonth = timeInOffice.map { x => (x.empId, epocTimeToDayWithMonth(x.loginTime), (x.logoutTime - x.loginTime), 0L) }

    //combining all three RDDs
    val totalTimeInOffice = timeInOfficeWithPeriodDay ++ timeInOfficeWithPeriodWeek ++ timeInOfficeWithPeriodMonth

    //group by empid and date , summing total time for each emp with date and finding first login time i,e min time for each day
    val timeInOfficeWithPeriodGroupByEmpDate = totalTimeInOffice.groupBy { x => (x._1, x._2) }.mapValues(f => (f.map(_._1).head, f.map(_._2).head, f.map(_._3).sum, f.map(_._4).min)).map(f => f._2)

    //mapping data with case class EmployeeTrackingSystem
    val employeeTrackingSystemvalues = timeInOfficeWithPeriodGroupByEmpDate.map(f => EmployeeTrackingSystem(f._1, f._2, f._3, f._4))

    val selectingEmpidAndFirstLoginTime = employeeTrackingSystemvalues.map { x => (x.empid, x.firstlogintime) }

    //group by empid and then sort according to list of first login time
    val sortFirstLogintimeListGroupByEmpid = selectingEmpidAndFirstLoginTime.groupBy(f => f._1).mapValues(f => (f.map(_._2).toList.sortBy(f => f).distinct))

    //calculating median logic 
    val expectedArrivalEachEmp = sortFirstLogintimeListGroupByEmpid.map(f => (f._1,

      //if size is even then (mid1+mid2)/2 else mid
      if ((f._2.size - 1) % 2 == 0) (f._2(((f._2.size - 1) / 2)) + f._2(((f._2.size - 1) / 2) + 1)) / 2 else f._2(((f._2.size - 1) / 2) + 1) //median login 
      ))
    // return expectedArrivalEachEmp
    val tmp = employeeTrackingSystemvalues.map(x => (x.empid, (x.date, x.firstlogintime, x.timeinoffice)))
    val employeeTrackingSystemvaluesWithExpectedTime = tmp.join(expectedArrivalEachEmp)

    val employeeTrackingSystemvaluesWithExpectedTimeFlatten = employeeTrackingSystemvaluesWithExpectedTime.map { case (a, ((b, c, d), e)) => (a, b, c, d, e) }

    val employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra = employeeTrackingSystemvaluesWithExpectedTimeFlatten.map(x => (EmployeeTrackingSystemWithExpectedTimeInOffice(x._1, x._2, x._3, x._4, x._5)))

    return employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra
  }

  override def postProcess(employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra: RDD[EmployeeTrackingSystemWithExpectedTimeInOffice], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EmployeeTrackingSystemWithExpectedTimeInOffice] = {

    //saving data to cassandra
    employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra.saveToCassandra(PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))

    return employeeTrackingSystemvaluesWithExpectedTimeFlattenToCassandra

  }

}


