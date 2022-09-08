package ru.mail.armmv.otus.dataengineer.hw01

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

//import com.github.plokhotnyuk.jsoniter_scala.macros._
//import com.github.plokhotnyuk.jsoniter_scala.core._

import io.circe.parser._
import io.circe.generic.auto._


case class Name(common: String, official: String)
case class Country(name: Name,
                   capital: Seq[String],
                   region: String,
                   area: Double)


object HW01Main {

  def main(args: Array[String]): Unit = {
    val outFile: String = args(0)

    val countriesJson: String = Source.fromResource("countries.json").getLines().mkString

    //implicit val codec: JsonValueCodec[Seq[Country]] = JsonCodecMaker.make
    //val countries = readFromArray(countriesJson.getBytes("UTF-8"))
    //println(countries)

    val countriesParsed = parse(countriesJson)
    val countries = countriesParsed.flatMap(_.as[List[Country]])
    //println(countries)

    val countriesList = countries.toOption.getOrElse(Seq.empty[Country])
    val result = countriesList.filter(_.region == "Africa").sortWith(_.area > _.area).take(10)
    //result.foreach(c => println(c))

    var resultJson: String = "["
    var isWas: Boolean = false
    result.foreach(c => {
      if (isWas) {
        resultJson = resultJson.concat(",")
      }
      val tmp = s"""\n  {\n    "name": "${c.name.official}",\n    "capital": "${c.capital.lift(0).getOrElse("<none>")}",\n    "area": ${c.area}\n  }"""
      resultJson = resultJson.concat(tmp)
      isWas = true
    })
    resultJson += "\n]"
    //println(resultJson)

    val file = new File(outFile)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(resultJson)
    bw.close()
  }
}