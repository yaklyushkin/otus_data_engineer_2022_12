package ru.mail.armmv.otus.dataengineer.hw02

import java.net.URI

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object HW02Main {

  def createDir(fileSystem: FileSystem, dir: String): Unit = {
    val path = new Path(dir)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def createOutFile(fileSystem: FileSystem, dir: String): FSDataOutputStream = {
    val outFilePath = new Path(dir.concat("/").concat("part-0000.csv"))
    if (fileSystem.exists(outFilePath)) {
      fileSystem.delete(outFilePath, false)
    }
    fileSystem.create(outFilePath)
  }

  def copyDir(fileSystem: FileSystem, dateDir: Path): Unit = {
    println(dateDir.getName)

    val outDir = outPath.concat("/").concat(dateDir.getName)
    createDir(fileSystem, outDir)
    val out = createOutFile(fileSystem, outDir)

    val files = fileSystem.listStatus(dateDir)
    files
      .filter(x => (x.getPath.getName.startsWith("part") && x.getPath.getName.endsWith(".csv")))
      .foreach (x => {
        println(x.getPath)
        val in = fileSystem.open(x.getPath)
        var b = new Array[Byte](1024)
        var numBytes = in.read(b)
        while (numBytes > 0) {
          out.write(b, 0, numBytes)
          numBytes = in.read(b)
        }
        in.close()
    })

    out.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.addResource(hdfsCoreSitePath)
    conf.addResource(hdfsHDFSSitePath)
    val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

    createDir(fileSystem, outPath)

    val dateDirs = fileSystem.listStatus(new Path(dataPath))
    dateDirs.foreach(x => copyDir(fileSystem, x.getPath))
  }

  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  private val dataPath: String = "/stage"
  private val outPath: String = "/ods"
}
