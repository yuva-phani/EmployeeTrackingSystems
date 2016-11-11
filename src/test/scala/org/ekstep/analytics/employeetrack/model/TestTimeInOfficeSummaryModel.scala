package org.ekstep.analytics.employeetrack.model

import org.ekstep.analytics.employeetrack.models.Event3

class TestTimeInOfficeSummaryModel extends SparkSpec(null) {
  it should "store data into cassandra table" in {
    val rdd = loadFile[Event3]("src/test/resources/test.log");
    val rdd2 = TimeInOfficeSummaryModel.execute(rdd, None);
    val events = rdd2.collect
    events.length should be(33)
  }
}