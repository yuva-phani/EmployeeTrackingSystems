package org.ekstep.analytics.employeetrack.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import org.scalatest.BeforeAndAfterAll
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.employeetrack.models._

/**
 * @author Santhosh
 */
class SparkSpec(val file: String = "src/test/resources/timeinofficetest.log") extends BaseSpec with BeforeAndAfterAll {

    var events: RDD[Event3] = null;
    implicit var sc: SparkContext = null;

    override def beforeAll() {
      println("test..................")
        sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
        events = loadFile[Event3](file)
    }

    override def afterAll() {
        CommonUtil.closeSparkContext();
    }

    def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
        if (file == null) {
            return null;
        }
        sc.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
    }

}