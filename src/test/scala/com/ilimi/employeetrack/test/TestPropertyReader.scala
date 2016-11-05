package com.ilimi.employeetrack.test

import org.scalatest.FlatSpec
import com.ilimi.employeetrack.utils.PropertyReader.getProperty

class TestPropertyReader extends FlatSpec {

  "key value" should "match" in {

    val property = getProperty("master")

    assert(property == "local")

  }

}