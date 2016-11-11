package org.ekstep.analytics.employeetrack.utils

import org.scalatest.FlatSpec
import org.ekstep.analytics.employeetrack.utils.PropertyReader.getProperty

class TestPropertyReader extends FlatSpec {

  "key value" should "match" in {

    val property = getProperty("master")

    assert(property == "local")

  }

}