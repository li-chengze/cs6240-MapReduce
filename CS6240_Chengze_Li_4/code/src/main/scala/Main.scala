import org.apache.log4j._
import org.apache.spark._
import Bz2WikiParser._
import scalax.io._


object Main {
  type node = (Double, List[String]);

  def formatResult(pair: (String, node)) : (String, Double) = {
    val pageName = pair._1;
    val pageWeight = pair._2._1;

    return (pageName, pageWeight);
  }

  def addDglToWeight(value: Any, dgl: Double) : node = {
    val value1 = value.asInstanceOf[node]
    val weight = value1._1;
    val newWeight = value1._1 + dgl;
    val formatted = BigDecimal(newWeight).setScale(10, BigDecimal.RoundingMode.HALF_UP).toDouble
    return (formatted, value1._2);
  }

  def collectOut(value1: Any, value2: Any) : Any = {
    if (value1.isInstanceOf[Double] && value2.isInstanceOf[Double]) {
      val sum = value1.asInstanceOf[Double] + value2.asInstanceOf[Double];
      return sum ;
    } else if (value1.isInstanceOf[Double] && value2.isInstanceOf[node]) { // means the value2 is node content
      val weight = value2.asInstanceOf[node]._1;
      val linkPages = value2.asInstanceOf[node]._2;
      return (weight + value1.asInstanceOf[Double], linkPages);
    } else if (value1.isInstanceOf[node] && value2.isInstanceOf[Double]){ // value1 is node content and value2 is received weight
      val weight = value1.asInstanceOf[node]._1;
      val linkPages = value1.asInstanceOf[node]._2;
      return (weight.toDouble + value2.asInstanceOf[Double], linkPages);

    }
  }
  def calculateOut(page: (String, node)): List[(String, Any)] = {
    val pageName = page._1;
    val content = page._2;
    val pageWeight = content._1;
    val linkPages = content._2;
    val linkPageNumbers = linkPages.length;
    var res = List[(String, Any)]();
    if(linkPageNumbers != 0) {
      for (linkPage <- linkPages) {
        res ::= (linkPage, pageWeight / linkPageNumbers);
      }
    } else {
      res ::= ("DANGLING", pageWeight);
    }

    val newNode = (0.toDouble, linkPages)
    res ::= (pageName, newNode);

    return res;
  }

  def constructNode (weight: Double, pageLinks: String): node = {
    var linkPages = List[String]();
    if (pageLinks.length != 2) {
      val content = pageLinks.substring(1, pageLinks.length - 1).split(", ");
      for (value <- content) {
        linkPages ::= value;
      }
    }

    return (weight, linkPages);
  }
  def mergeLinkPages(link1: String, link2: String): String = {
    if (link1.length == 2 && link2.length == 2) {
      return "[]";
    } else {
      val res = if (link1.length != 2) link1 else link2;
      return res;
    }
  }

  def extractNode(line: (String, String)) : List[(String, String)] = {
    val pageId = line._1;
    val linkPageListString = line._2;

    val linkPageArray = linkPageListString.substring(1, linkPageListString.length() - 1).split(", ");

    var res = List[(String, String)]();

    for (value <- linkPageArray) {
      res ::= (value, "[]");
    }

    res ::= line;

    return res;
  }
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Spark Page Rank").set("spark.network.timeout", "600s");
    val sc = new SparkContext(conf);
    val input = args(0)
    val output = args(1)

    // create the rdd from bz file
    val bz2Rdd = sc.textFile(input);

    val parsedRdd =
      bz2Rdd.map(Bz2WikiParser.process)
        .filter(line => !line.equals("unvalid"));


    // transform the parsed rdd to key value pair
    val keyValueRdd =
      parsedRdd.map(line => (line.split(":")(0), line.split(":")(1)))
          .flatMap(extractNode)
          .reduceByKey(mergeLinkPages).persist();

    // calculate the number of dangling node
    val dangling =
      keyValueRdd.filter(x => x._2 == "[]").count();

    // calculate the number of page
    val pageCount = keyValueRdd.count();

    // convert the value to node structure
    val nodeRdd =
      keyValueRdd
        .mapValues(value => constructNode(1f / pageCount, value));

    val initDgl = dangling.toDouble / pageCount;

    // unpersist the keyValueRdd
    keyValueRdd.unpersist();

    // iterative calculation of pagerank
    /** example output
      * (A,(0.0,List(C, B)))
        (B,(0.125,List(C)))
        (C,(0.375,List(D)))
        (DANGLING,0.25)
        (D,(0.25,List()))
      */
    var modifiedOutRdd : rdd.RDD[(String, node)] = nodeRdd;
    var dglTotal = initDgl;
    for (i <- 1 to 10) {
      val lastResultRdd = modifiedOutRdd;
      val lastDglTotal = dglTotal;

      val outRdd =
        lastResultRdd.flatMap(calculateOut)
          .reduceByKey(collectOut);

      modifiedOutRdd =
        outRdd.filter(pair => pair._1 != "DANGLING")
          .mapValues(value => addDglToWeight(value, lastDglTotal / pageCount));

      dglTotal =
        modifiedOutRdd.filter(pair => pair._2._2.isEmpty)
          .mapValues(value => value._1.asInstanceOf[Double])
          .values.sum;
    }

    val displayResult =
      modifiedOutRdd.map(formatResult)
        .collect()
        .sortBy(pair => pair._2)
        .reverse
        .take(100);

    sc.parallelize(displayResult).coalesce(1).saveAsTextFile(output);
  }
}
