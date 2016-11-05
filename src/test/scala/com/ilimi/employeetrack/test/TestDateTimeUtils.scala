package com.ilimi.employeetrack.test

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.utils.DateTimeUtils.strToOnlyTimeInSeconds
import com.ilimi.employeetrack.utils.DateTimeUtils.calculateWeekDays
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDate
import com.ilimi.employeetrack.utils.DateTimeUtils.epocDateTimeToTimeInEpoch
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDayWithWeek
import com.ilimi.employeetrack.utils.DateTimeUtils.epocTimeToDayWithMonth
class TestDateTimeUtils extends FlatSpec {

  "Date and Time from String" should "match with time in seconds" in {

    val stringToSeconds = strToOnlyTimeInSeconds("2016-11-02 02:12:33")

    assert(stringToSeconds == 7953)
  }

  "Week days " should "match with count" in {

    val weekdays = calculateWeekDays("2016-11-01", "2016-11-03")

    assert(weekdays == 3)
  }

  "epoch time " should "match with day" in {

    val epochTimeToDay = epocTimeToDate(1478110779L)

    assert(epochTimeToDay == "2016-11-02")
  }

  "Date and time" should "match with time in epoch" in {

    val epocDateTimeToTime = epocDateTimeToTimeInEpoch(1478110779L)
    assert(epocDateTimeToTime == 85779)

  }
  "epoch time to day with week" should "match" in {

    val epocTimeToDayWeek = epocTimeToDayWithWeek(1478110779)
    assert(epocTimeToDayWeek == "2016-11m 45w")

  }

  "epoch time to day with month" should "match" in {

    val epocTimeToDayMonth = epocTimeToDayWithMonth(1478110779)

    assert(epocTimeToDayMonth == "2016-11m")
  }

}