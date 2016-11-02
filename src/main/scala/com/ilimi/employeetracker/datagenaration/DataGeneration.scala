package com.ilimi.DataGenerations

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.joda.time.DateMidnight
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants

object DataGeneration extends App {

  def main(args: Array[String]) {
    // Generate Date ranges between two dates
    val range = dateRanges(DateTime.now().minusDays(90), DateTime.now())
    //Login time Sequence In Seconds
    val loginTimeSequencePerDay = 32400 to 43200
    //Logout Time Sequence In Seconds
    val logoutTimeSequencePerDay = 57600 to 68400

    val bwr = new BufferedWriter(new FileWriter(new File("//home//yuva//Desktop//timeinoffice9088.csv")));
    for (employee <- 1 to 100) {
      //Working days for each employee
      val listOfWorkingDaysPerEmployee = 45 to 80
      //Select working days using random function
      val WorkingDaysPerEmployee = Random.shuffle(listOfWorkingDaysPerEmployee).head
      // Generate Employee,Login and logout times in Epoch time format
      val loginLogoutsInSeconds = range.map(x => (employee, (x.getMillis / 1000) + Random.shuffle(loginTimeSequencePerDay.toList).head, (x.getMillis / 1000) + Random.shuffle(logoutTimeSequencePerDay.toList).head))
      //Select list of working days for each employee 
      val selectListOfWorkingDaysEachEmployee = Random.shuffle(loginLogoutsInSeconds.toList).take(WorkingDaysPerEmployee)
      //Writing data to file
      selectListOfWorkingDaysEachEmployee.map { x =>
        val line = x.productIterator.mkString(",")
        bwr.write(line)
        bwr.write("\n")
      }
    }
    bwr.close();
    println("Writing to CSV file Done!!")
  }

  //Generate Date sequences with out weekends in Date range
  def dateRanges(start: DateTime, end: DateTime): Array[DateMidnight] = {
    var array = ArrayBuffer[DateMidnight]()
    var weekday = start;

    if (start.getDayOfWeek() == DateTimeConstants.SATURDAY ||
      start.getDayOfWeek() == DateTimeConstants.SUNDAY) {
      weekday = weekday.plusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY);

    }

    while (weekday.isBefore(end)) {

      if (weekday.getDayOfWeek() == DateTimeConstants.FRIDAY) {
        weekday = weekday.plusDays(3);

      } else {
        weekday = weekday.plusDays(1);

      }
      array += weekday.toDateMidnight()
    }

    return array.toArray
  }

}