package org.ekstep.analytics.employeetrack.datageneration

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
import org.ekstep.analytics.employeetrack.utils.DateTimeUtils._
import org.ekstep.analytics.employeetrack.utils.PropertyReader

object DataGeneration {
  var bwr: BufferedWriter = null
  def dataGeneration(fileName: String, employeeCount: Int, startDate: String, endDate: String){

    if (!new java.io.File(PropertyReader.getProperty(fileName)).exists) {
      try {
        bwr = new BufferedWriter(new FileWriter(new File(PropertyReader.getProperty(fileName))));

        // Generate Date ranges between two dates
        val range = dateRanges(new DateTime(PropertyReader.getProperty(startDate)), new DateTime(PropertyReader.getProperty(startDate)))

        //Login time Sequence In Seconds
        val loginTimeSequencePerDay = 32400 to 43200

        //Logout Time Sequence In Seconds
        val logoutTimeSequencePerDay = 57600 to 68400

        val random = new scala.util.Random()
        random.setSeed(100L)
        for (employee <- 1 to employeeCount) {

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

}