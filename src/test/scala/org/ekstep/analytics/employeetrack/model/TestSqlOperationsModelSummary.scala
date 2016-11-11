package org.ekstep.analytics.employeetrack.model

import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice

class TestSqlOperationsModelSummary extends SparkSpec(null) {

  "SqlOperationsModelSummary" should "print all employees summary" in {
    println("Hi...........")
    val rdd = loadFile[EmployeeTrackingSystemWithExpectedTimeInOffice]("src/test/resources/tset3.log");
    rdd.foreach { x => println(x) }

    val rdd2 = SqlOperationsModelSummary.execute(rdd, None);
    
    rdd2.foreach { x => println(x) }
    
    //val x=rdd2.map { x => x.empid }
    
    
    //rdd2.collect()
  
  }

}