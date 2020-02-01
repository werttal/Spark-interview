import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.{SparkConf, SparkContext}


object Playground {

  def uniFormat(s: String): String =
    s.toLowerCase().replace("-", " ")  //make the keys uniform

  def main(args: Array[String]): Unit = {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Playground")
    val sc = new SparkContext(conf)
    //upload file to RDD object
    val jsonFile = sc.textFile("src/main/resources/sample-dataset.json")
    //create mapper obj to work with JSON format
    val mapper = new ObjectMapper();
    val myRdd = jsonFile
      //RDD line to JsonNode line
      .map(line => mapper.readTree(line))
      // converts JsonNode to (keyphrase , (keyphrase (as a copy), int (for count uniques))) tupple
      .map(obj => (obj.get("keyphrase").asText(), (obj.get("keyphrase").asText(), 0)))
      // counts the uniques
      .reduceByKey((a, b) => (a._1, a._2 + b._2))
      // change the similar keys to be the same (replace "-" with " ", and lower case) while keeping a copy of the original key
      .map(softUniques => (uniFormat(softUniques._1), softUniques._2))
      //pick the most common format for each key
      .reduceByKey((softUnique1, softUnique2) =>
        if (softUnique1._2 > softUnique2._2) {
          softUnique1
        }
        else {
          softUnique2
        }
      )
      //remove the count since we don't need it anymore
      .map(hardUnique => (hardUnique._1, hardUnique._2._1))
    //create a map to get the most popular key from the uniform key
    val keysMap: collection.Map[String, String] = myRdd.collectAsMap()

    //replace the original keyphrase the with the most common keyphrase
    val outputRdd = jsonFile
      .map((line: String) => {
        //this is a mutable object
        val jsonObject: ObjectNode = mapper.readTree(line).asInstanceOf[ObjectNode]
        //get the common key from the uniform key
        val correctKey: String = keysMap(uniFormat(jsonObject.get("keyphrase").asText()))
        //replace the key
        jsonObject.put("keyphrase", correctKey)
        //return an RDD string of the updated JSON line
        mapper.writeValueAsString(jsonObject)
      })

    // outputRdd.collect().foreach(println)
    outputRdd.saveAsTextFile("src/main/resources/output1.json")

    //the first and probably much less efficient solution to find the popular key of each group
    /*
    //  def isTheSame(x: String, y: String): Boolean =
    //    x.toLowerCase.replace("-", " ") == y.toLowerCase.replace("-", " ")

        val distinctWithCount = jsonFile
          .map(line => mapper.readTree(line))
          .map(obj => JsonLine(obj.get("keyphrase").asText(), obj.get("weight").asDouble(), obj.get("doc_id").asInt()))
          .map(ourObj => (ourObj.keyphrase, 1))
          .reduceByKey(_ + _)
          //.sample(true,0.0001)

        def seqOp(out: Map[String, Int], t: (String, Int)) = {
          val entry: Option[(String, Int)] = out.find(entry => isTheSame(entry._1, t._1))
          if (entry.isEmpty) {
            out + t
          }
          else {
            if(entry.get._2 < t._2) {
              out - entry.get._1 + t
            }
            else {
             out
           }
          }
        }

        def combOp (out1: Map[String, Int], out2: Map[String, Int]): Map[String, Int] = {
          out1.foldLeft(out2)(seqOp)
        }

       // distinctWithCount.foreach(println)
        val out = distinctWithCount.aggregate(Map.empty[String, Int])(seqOp,combOp)
        println(out)
        //distinctWithCount.sortBy(_._1).saveAsTextFile("src/main/resources/results")
    //    println("all distinct:" + distinctWithCount.count())
    */
  }
}

