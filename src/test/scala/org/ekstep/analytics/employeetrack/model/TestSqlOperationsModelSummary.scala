package org.ekstep.analytics.employeetrack.model

import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice

class TestSqlOperationsModelSummary extends SparkSpec(null) {

  "SqlOperationsModelSummary" should "print all employees summary" in {
    println("Hi...........")
    val rdd = loadFile[EmployeeTrackingSystemWithExpectedTimeInOffice]("src/test/resources/tset3.log");

    val rdd2 = SqlOperationsModelSummary.execute(rdd, None);

    val sql = rdd2.map(x => x.averagetimepermonth)

    sql.first() should be(9876456.0)
    val events = rdd2.collect
    events.length should be(1)

  }
}
