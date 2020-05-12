//import spark session and sparkcontext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

// Spark data manipulation libraries
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//Spark Machine Learning librarires
import org.apache.spark.ml.feature.{HashingTF,Tokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline

//import file from JAVA class
import java.io.File

// General purpose library
import scala.xml._
import scala.xml.XML
object main {
  def main(args: Array[String]): Unit = {
    //list all files in a directory
    val stackexchangeDirectory = "/media/harry/harry/torrent/stackexchange_data"
    // list only the folders directly under this directory (does not recurse)
    val folders: Array[File] = (new File(stackexchangeDirectory))
      .listFiles
//    folders.foreach(println)

    val xmlFile = ("/home/harry/Downloads/DATA/Posts.small.xml")
    val conf = new SparkConf().setMaster("local[*]").setAppName("xml handling")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val textFile = sc.textFile(xmlFile)
    val postsXml = textFile.map(_.trim)
      .filter(!_.startsWith("<?xml version=")).
      filter(_ != "<posts>").
      filter(_ != "</posts>")

    // concatenate title and body and remove all unnecessary tags and new line
    // characters from the body and all space duplications
    val postsRDD = postsXml.map { s =>
      val xml = XML.loadString(s)
      val id = (xml \ "@Id").text
      val tags = (xml \ "@Tags").text
      val title = (xml \ "@Title").text
      val body = (xml \ "@Body").text
      val bodyPlain = ("<\\S+>".r).replaceAllIn(body, " ")
      val text = (title + " " + bodyPlain).replaceAll("\n", " ").replaceAll("( )+", " ");

      Row(id, tags, text)
    }

    // create a data-frame
    val schemaString = "Id Tags Text"
    val schema = StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val postsDf = spark.createDataFrame(postsRDD, schema)

    // take a look at your data frame
//    postsDf.show()
    print(postsDf.show())



  }
}

