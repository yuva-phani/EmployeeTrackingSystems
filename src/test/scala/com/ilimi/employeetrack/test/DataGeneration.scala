package com.ilimi.employeetrack.test

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.joda.time.DateMidnight
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants
import java.io.BufferedReader
import java.io.IOException
import java.sql.SQLException
import java.io.FileNotFoundException

object DataGeneration {

  def dataGeneration {
    var bwr: BufferedWriter = null
    if (!new java.io.File("timeinofficetest.csv").exists) {
      try {
        bwr = new BufferedWriter(new FileWriter(new File("timeinofficetest.csv")));
        // Generate Date ranges between two dates
        val range = dateRanges(DateTime.now().minusDays(10), DateTime.now())
        //Login time Sequence In Seconds
        val loginTimeSequencePerDay = 32400 to 43200
        //Logout Time Sequence In Seconds
        val logoutTimeSequencePerDay = 57600 to 68400

        val random = new scala.util.Random()
        random.setSeed(100L)
        for (employee <- 1 to 5) {
          //Working days for each employee
          val listOfWorkingDaysPerEmployee = 45 to 80
          //Select working days using random function
          val WorkingDaysPerEmployee = random.shuffle(listOfWorkingDaysPerEmployee).head

          // Generate Employee,Login and logout times in Epoch time format
          val loginLogoutsInSeconds = range.map(x => (employee, (x.getMillis / 1000) + random.shuffle(loginTimeSequencePerDay.toList).head, (x.getMillis / 1000) + random.shuffle(logoutTimeSequencePerDay.toList).head))
          //Select list of working days for each employee 
          val selectListOfWorkingDaysEachEmployee = random.shuffle(loginLogoutsInSeconds.toList).take(WorkingDaysPerEmployee)
          //Writing data to file
          selectListOfWorkingDaysEachEmployee.map { x =>
            val line = x.productIterator.mkString(",")
            bwr.write(line)
            bwr.write("\n")
          }
        }

      } catch {
        case e: IOException           => println("IO exception " + e.printStackTrace())
        case e: FileNotFoundException => println("file not found exception " + e.printStackTrace())
        case unknown                  => println("Exception  " + unknown)
      } finally {
        bwr.close()
        println("Writing to CSV file Done!!")

      }
    } else {

      println("file already present in location !!!")
    }
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