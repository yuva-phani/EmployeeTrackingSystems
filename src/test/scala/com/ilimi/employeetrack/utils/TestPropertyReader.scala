package com.ilimi.employeetrack.utils

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.utils.PropertyReader.getProperty

class TestPropertyReader extends FlatSpec {

  "key value" should "match" in {

    val property = getProperty("master")

    assert(property == "local")

  }

}