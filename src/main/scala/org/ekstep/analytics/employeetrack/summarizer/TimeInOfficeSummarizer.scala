package org.ekstep.analytics.employeetrack.summarizer

import org.apache.spark.SparkContext

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger

import org.ekstep.analytics.employeetrack.model.TimeInOfficeSummaryModel

object TimeInOfficeSummarizer extends Application with IJob {

  implicit val className = "org.ekstep.analytics.employeetrack.summarizer.TimeInOffice"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    JobLogger.log("Started executing Job")
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobDriver.run("batch", config, TimeInOfficeSummaryModel);
    JobLogger.log("Job Completed.")
  }
}