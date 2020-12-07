import java.io._
import java.util.zip.{ZipEntry, ZipFile}

import org.json4s.NoTypeHints
import org.json4s.Xml.toJson
import org.json4s.native.Serialization
import org.json4s.native.Serialization.writePretty

import scala.collection.JavaConverters._
import scala.xml.{Node, NodeSeq, XML}

object Start extends App {

  zipEntries("airbase_v2_xmldata.zip").foreach(entry => {
    val (name, is) = entry
    if ("PL_meta.xml".equals(name)) {
      processXML(is)
    }
  })

  private def processXML(is: InputStream): Unit = {
    val f = javax.xml.parsers.SAXParserFactory.newInstance()
    f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    val p = f.newSAXParser()
    val doc = XML.withSAXParser(p).load(is)

    val stations = doc \\ "station"

    val allStationInfo: NodeSeq = stations.map(station => processStation(station)).map(data => {
      val (stName, eurCode, stationInfo: NodeSeq, tsv) = data
      val metaFileName = stName + "_" + eurCode + "_meta.json"
      val stationInfoJson = writePretty(toJson(stationInfo))(Serialization.formats(NoTypeHints))
      writeFile("output/" + metaFileName, stationInfoJson)
      val yearlyFileName = stName + "_" + eurCode + "_yearly.tsv"
      writeFile("output/" + yearlyFileName, tsv)
      stationInfo}).map { s => <stations_meta> {s} </stations_meta>} toSeq

    val countryName = (doc \\ "country_name").text
    val allStationInfoJson = writePretty(toJson(allStationInfo))(Serialization.formats(NoTypeHints))
    writeFile("output/" + "stations_" + countryName + "_meta.json", allStationInfoJson)

  }

  def processStation(station: Node) = {
    val stationInfo = station \ "station_info"
    val eurCode = (stationInfo \ "station_european_code").text
    val stName = (stationInfo \ "station_name").text

    val configs = station \ "measurement_configuration"
    val year = "2005"

    var tsv = "component_name\tcomponent_caption\tmeasurement_unit\tmeasurement_technique_principle\tYear 2005 mean\tYear 2005 median(P50)\n"

    tsv += configs.map(config => {
      val info = config \ "measurement_info"
      val component_name = (info \ "component_name").text
      val component_caption = (info \ "component_caption").text
      val measurement_unit = (info \ "measurement_unit").text
      val measurement_technique_principle = (info \ "measurement_technique_principle").text
      val statisticGroup = ((config \ "statistics")
        .filter(st => (st \ "@Year").text.equals(year)) \ "statistics_average_group")
        .filter(group => (group \ "@value").text.equals("day"))

      val yearlyMean = (statisticGroup \\ "statistic_result").filter(result => (result \ "statistic_name").text.equals("50 percentile")) \ "statistic_value"
      val annualMean = (statisticGroup \\ "statistic_result").filter(result => (result \ "statistic_name").text.equals("annual mean")) \ "statistic_value"

      component_name + "\t" + component_caption + "\t" + measurement_unit + "\t" + measurement_technique_principle + "\t" + yearlyMean.text + "\t" + annualMean.text
    }).mkString("\n")

    (stName, eurCode, stationInfo, tsv)
  }

  private def inputStreamToString(is: InputStream) = {
    val inputStreamReader = new InputStreamReader(is)
    val bufferedReader = new BufferedReader(inputStreamReader)
    Iterator continually bufferedReader.readLine takeWhile (_ != null) mkString ("\n")
  }


  def zipEntries(source: String) = {
    val zipFile = new ZipFile(source)
    val entries = zipFile.entries().asScala
    entries.map(entry => (entry.getName, getZipEntryInputStream(zipFile)(entry)))
  }

  def getZipEntryInputStream(zipFile: ZipFile)(entry: ZipEntry) = zipFile.getInputStream(entry)

  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
}
