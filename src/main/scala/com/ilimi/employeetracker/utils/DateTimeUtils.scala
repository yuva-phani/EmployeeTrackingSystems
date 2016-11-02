package com.ilimi.employeetracker.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.joda.time.DateTime
import org.joda.time.Months
import org.joda.time.Weeks
import org.joda.time.format.DateTimeFormat
import com.ilimi.employeetracker.utils.PropertyReader
import java.util.Date
import com.ilimi.employeetracker.utils.PropertyReader

object DateTimeUtils {

 
//extracting time in seconds from date and time in string format
  def strToOnlyTimeInSeconds(x: String): Long = {
    val parseFormat = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat") + " " + PropertyReader.getProperty("onlyTimeFormat"))
    val printFormat = new SimpleDateFormat(PropertyReader.getProperty("onlyTimeFormat"))
    val date = parseFormat.parse(x)
    val time = printFormat.format(date).toString()
    val tokens = time.split(":")
    val hours = Integer.parseInt(tokens(0))
    val minutes = Integer.parseInt(tokens(1))
    val seconds = Integer.parseInt(tokens(2))
    val duration = 3600 * hours + 60 * minutes + seconds
    return duration
  }

  //Calculate number of week days
  def calculateWeekDays(fromDate: String, toDate: String): Int = {
    val df = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat"))
    val date1 = df.parse(fromDate)
    val date2 = df.parse(toDate)
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()
    cal1.setTime(date1)
    cal2.setTime(date2)
    var numberOfDays = 1;
    while (cal1.before(cal2)) {
      if ((Calendar.SATURDAY != cal1.get(Calendar.DAY_OF_WEEK))
        && (Calendar.SUNDAY != cal1.get(Calendar.DAY_OF_WEEK))) {
        numberOfDays += 1;
      }
      cal1.add(Calendar.DATE, 1)
    }
    return numberOfDays
  }

  //Converting Epoch time format to only day not including time
  def epocTimeToDate(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    return formatter.format(calendar.getTime()).toString()
  }

 //converting epoch time to only Day
  def epocTimeToDay(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    return formatter.format(calendar.getTime()).toString()
  }
  
//extracting only time from Date and Time in epoch format
  def epocDateTimeToTimeInEpoch(x: Long): Long = {
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    val formatter = new SimpleDateFormat("YYY-MM-dd HH:mm:ss")
    val date = formatter.format(calendar.getTime()).toString()
    return strToOnlyTimeInSeconds(date)

  }
//Converting epoch time to year and week
  def epocTimeToDayWithWeek(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
    val yearMonth = epocTimeToDayWithMonth(x)
    return yearMonth + " " + weekOfYear + "w"
  }
  
  //Converting epoch time to year and month
  def epocTimeToDayWithMonth(x: Long): String = {
    val formatter = new SimpleDateFormat("YYYY-MM");
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000)
    return formatter.format(calendar.getTime()).toString()
  }
  
  //Generate Date sequences with out weekends in Date range
  def dateRanges(start: DateTime, end: DateTime): Array[DateMidnight] = {
    var array = ArrayBuffer[DateMidnight]()
    var weekday = start;
    if (start.getDayOfWeek() == DateTimeConstants.SATURDAY ||
      start.getDayOfWeek() == DateTimeConstants.SUNDAY) {
      weekday = weekday.plusWeeks(1).withDayOfWeek(DateTimeConstants.MONDAY)
    }
    while (weekday.isBefore(end)) {
      if (weekday.getDayOfWeek() == DateTimeConstants.FRIDAY) {
        weekday = weekday.plusDays(3)
      } else {
        weekday = weekday.plusDays(1)
      }
      array += weekday.toDateMidnight()
    }
    return array.toArray
  }

}

}
