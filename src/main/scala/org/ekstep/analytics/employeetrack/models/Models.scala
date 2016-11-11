package org.ekstep.analytics.employeetrack.models

import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.AlgoOutput

class Models extends Serializable {} 
  

case class EmployeeTrackingSystem(empid: String, date: String, timeinoffice: Long, firstlogintime: Long)

case class EmployeeTrackingSystemWithExpectedTimeInOffice(empid: String, date: String, timeinoffice: Long, firstlogintime: Long,expectedtimeinoffice:Long) extends Input with AlgoInput with AlgoOutput with Output
case class GeneratedData(empid: String, logintimeinepochformat: Long, logouttimeinepochformat: Long)
@scala.beans.BeanInfo
case class Event3(empId: String, loginTime: Long, logoutTime: Long) extends AlgoInput with Input {}
@scala.beans.BeanInfo
case class SqlOperation(empid:String,empabsent:Long,averagetimeperweek:Long,averagetimepermonth:Long) extends AlgoOutput with Output{}