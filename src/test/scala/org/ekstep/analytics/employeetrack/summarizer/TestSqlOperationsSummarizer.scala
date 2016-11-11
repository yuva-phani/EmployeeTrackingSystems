package org.ekstep.analytics.employeetrack.summarizer

import org.ekstep.analytics.employeetrack.models.EmployeeTrackingSystemWithExpectedTimeInOffice
import org.ekstep.analytics.employeetrack.utils.PropertyReader
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.employeetrack.summarizer
import org.ekstep.analytics.employeetrack.model.SqlOperationsModelSummary
import org.ekstep.analytics.employeetrack.summarizer.SqlOperationsSummarizer
import com.datastax.spark.connector._

class TestSqlOperationsSummarizer extends SparkSpec(null) {

  "SqlOperations" should "execute the job and shouldn't throw any exception" in {

    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/test.log"))))), None, None, "org.ekstep.analytics.employeetrack.model.TimeInOfficeSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> true.asInstanceOf[AnyRef])))), Option(10), Option("SqlOperationsSummarizer"), Option(false))
    SqlOperationsSummarizer.main(JSONUtils.serialize(config))(Option(sc));
  }

}