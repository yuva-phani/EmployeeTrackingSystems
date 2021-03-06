package org.ekstep.analytics.employeetrack.summarizer
import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.utils.PropertyReader
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.employeetrack.summarizer
import org.ekstep.analytics.employeetrack.model.TimeInOfficeSummaryModel
import org.ekstep.analytics.employeetrack.summarizer.TimeInOfficeSummarizer
class TestTimeOfficeSummarizer extends SparkSpec(null) {

  "TestTimeOffice" should "execute the job and shouldn't throw any exception" in {

    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/test.log"))))), None, None, "org.ekstep.analytics.employeetrack.model.TimeInOfficeSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> true.asInstanceOf[AnyRef])))), Option(10), Option("TimeInOfficeSummarizer"), Option(false))
    TimeInOfficeSummarizer.main(JSONUtils.serialize(config))(Option(sc));
  }
  
}