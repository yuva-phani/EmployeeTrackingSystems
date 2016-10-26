package com.ilimi.employeetracker.datagenaration

import scala.util.Random
import java.io.FileWriter
import org.joda.time.DateTime
import scala.collection.mutable.ArrayBuffer
import org.joda.time.Period
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import org.joda.time.DateTimeConstants

object DataGen {

  var loginTime = ArrayBuffer[String]()
  var logoutTime = ArrayBuffer[String]()
  var empid = ArrayBuffer[String]()
  var totalEmpid = ArrayBuffer[String]()
  var totallogin = ArrayBuffer[String]()
  var totallogout = ArrayBuffer[String]()
  var loginTimeEmp = ArrayBuffer[String]()
  var logoutTimeEmp = ArrayBuffer[String]()

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] = {
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }

  def dateRanges(start: DateTime, end: DateTime): Array[DateTime] = {
    var array = ArrayBuffer[DateTime]()
    var weekday = start;

    if (start.getDayOfWeek() == DateTimeConstants.SATURDAY ||
      start.getDayOfWeek() == DateTimeConstants.SUNDAY) {
      weekday = weekday.plusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY);
    }

    while (weekday.isBefore(end)) {
      System.out.println(weekday);

      if (weekday.getDayOfWeek() == DateTimeConstants.FRIDAY)
        weekday = weekday.plusDays(3);
      else
        weekday = weekday.plusDays(1);
      // println(weekday)
    }
    array += weekday
    return array.toArray
  }

  def timeConversion(totalSeconds: Int): String = {

    val MINUTES_IN_AN_HOUR = 60;
    val SECONDS_IN_A_MINUTE = 60;

    val seconds = totalSeconds % SECONDS_IN_A_MINUTE;
    val totalMinutes = totalSeconds / SECONDS_IN_A_MINUTE;
    val minutes = totalMinutes % MINUTES_IN_AN_HOUR;
    val hours = totalMinutes / MINUTES_IN_AN_HOUR;
    return hours + ":" + minutes + ":" + seconds.toString();
  }
  val range = dateRanges(DateTime.now().minusDays(90), DateTime.now())

  def main(args: Array[String]) {

    for (employee <- 1 to 2) { // no of employees

      val listOfWorkingDaysPerEmployee = 45 to 90

      val WorkingDaysPerEmployee = Random.shuffle(listOfWorkingDaysPerEmployee.toList).head //picking no of days present to office each employee

      for (j <- 1 to WorkingDaysPerEmployee) {

        val k = 1

        val listOfLoginsLogoutsEachDay = 2 to 7 //no of login/logouts each day

        val loginsLogoutsEachDay = Random.shuffle(listOfLoginsLogoutsEachDay.toList).head

        val timeSequencePerDay = 25200 to 86399
        var timeSlice = Random.shuffle(timeSequencePerDay.toList).take(2 * loginsLogoutsEachDay)
        timeSlice = timeSlice.sorted
        val k2 = (1 to 2 * loginsLogoutsEachDay - 1 by 2)

        for (k1 <- k2) {

          loginTime += range(j).toYearMonthDay().toString() + " " + timeConversion(timeSlice(k1 - 1))

          logoutTime += range(j).toYearMonthDay().toString() + " " + timeConversion(timeSlice(k1))

          totallogin = loginTime ++ totallogin

          totallogout = logoutTime ++ totallogout

        }

        empid = ArrayBuffer.fill((totallogin.length))(employee.toString())

      }

      totalEmpid = totalEmpid ++ empid

      loginTimeEmp = loginTimeEmp ++ totallogin

      logoutTimeEmp = logoutTimeEmp ++ totallogout
    }

    val addingColumns = totalEmpid zip loginTimeEmp zip logoutTimeEmp

    val mapping = addingColumns.map { case (((a, b), c)) => (a, b, c) }

    val content = mapping.toList

    var sb = new StringBuilder();

    for (z <- 1 to content.size - 1) {
      sb.append(content(z)._1);
      sb.append(",");
      sb.append(content(z)._2)
      sb.append(",");
      sb.append(content(z)._3)
      sb.append('\n')
    }

    val bwr = new BufferedWriter(new FileWriter(new File("//home//yuva//Desktop//timeinoffice.csv")));

    //write contents of StringBuffer to a file
    bwr.write(sb.toString());

    //flush the stream
    bwr.flush();

    //close the stream
    bwr.close();
  }

}