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

  def strDateTotimeInSeconds(x: String): Long = {
    DateTimeFormat.forPattern(PropertyReader.getProperty("onlyDayFormat") + " " + PropertyReader.getProperty("onlyTimeFormat")).parseDateTime(x).getMillis() / 1000
  }

  def strDateToDay(x: String): String = {
    DateTimeFormat.forPattern(PropertyReader.getProperty("onlyDayFormat") + " " + PropertyReader.getProperty("onlyTimeFormat")).parseDateTime(x).toYearMonthDay().toString()
  }

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

  //no of weeks between two dates

  def noOfWeeksBetweenTwoDates(fromDate: String, toDate: String): Int = {
    val dateTime1 = new DateTime(fromDate);
    val dateTime2 = new DateTime(toDate);
    val weeks = Weeks.weeksBetween(dateTime1, dateTime2).getWeeks();
    return weeks
  }

  def noOfMonthsBetweenTwoDates(fromDate: String, toDate: String): Int = {
    val dateTime1 = new DateTime(fromDate);
    val dateTime2 = new DateTime(toDate);
    return Months.monthsBetween(dateTime1, dateTime2).getMonths();
  }

  def epocTimeToDate(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    return formatter.format(calendar.getTime()).toString()
  }

  def epocTimeToTime(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyTimeFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);

    return formatter.format(calendar.getTime()).toString()
  }
  def epocTimeToDay(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    return formatter.format(calendar.getTime()).toString()
  }

  def epocDateTimeToTimeInEpoch(x: Long): Long = {

    // val date = new Date(x);
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);

    val formatter = new SimpleDateFormat("YYY-MM-dd HH:mm:ss");
    val date = formatter.format(calendar.getTime()).toString()
    return strToOnlyTimeInSeconds(date)

  }

  def epocTimeToDayWithWeek(x: Long): String = {
    val formatter = new SimpleDateFormat(PropertyReader.getProperty("onlyDayFormat").toString());
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
    val yearMonth = epocTimeToDayWithMonth(x)
    return yearMonth + " " + weekOfYear + "w"
  }
  def epocTimeToDayWithMonth(x: Long): String = {
    val formatter = new SimpleDateFormat("YYYY-MM");
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(x * 1000);
    return formatter.format(calendar.getTime()).toString()
  }

}