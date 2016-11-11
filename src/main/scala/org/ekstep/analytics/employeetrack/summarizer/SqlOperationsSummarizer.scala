package org.ekstep.analytics.employeetrack.summarizer

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.employeetrack.model.SqlOperationsModelSummary

object SqlOperationsSummarizer extends Application with IJob {

  implicit val className = "org.ekstep.analytics.employeetrack.summarizer.SqlOperationsSummarizer"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    JobLogger.log("Started executing Job")
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobDriver.run("batch", config, SqlOperationsModelSummary);
    JobLogger.log("Job Completed.")
  }
}