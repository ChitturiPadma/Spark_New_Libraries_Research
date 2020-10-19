package com.expedia.demos

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

import scala.collection.mutable


object Spark_Ngrams {

  def generateNgrams(property_address:String):Array[String] = (3 to property_address.length).map{i => property_address.sliding(i).toList}.toArray.flatten

  def normalize(raw: String) = raw.replaceAll("[^a-zA-Z0-9]+","").toLowerCase

  //Does the actual N-Gram extraction from a string.
  def extractTokens(str: String, n: Int, stopWords: Set[String] = Set[String](), sep: String="_"): Array[String] = {
    //Lowercase and split on whitespace, and filter out empty tokens and stopwords.
    val toks = str.toLowerCase.replaceAll(sep,"").split("\\s+").map(normalize).filter(_ != "").filter(!stopWords.contains(_))

    //Produce the ngrams.
    //val ngrams = (for( i <- 1 to n) yield toks.sliding(i).map(p => p.mkString(sep))).flatMap(x => x)
   /* val ngrams =  toks.sliding(n).map(p => p.mkString(sep))

    ngrams.toSet*/
    toks
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark_Ngrams")
      .getOrCreate()

    val stopWords = Set("a", "about", "above", "above", "across", "after",
      "afterwards", "again", "against", "all", "almost",
      "alone", "along", "already", "also","although","always",
      "am","among", "amongst", "amongst", "amount",  "an", "and",
      "another", "any","anyhow","anyone","anything","anyway", "anywhere",
      "are", "around", "as",  "at", "back","be","became", "because",
      "become","becomes", "becoming", "been", "before", "beforehand",
      "behind", "being", "below", "beside", "besides", "between", "beyond",
      "bill", "both", "bottom","but", "by", "call", "can", "cannot",
      "cant", "co", "con", "could", "couldnt", "cry", "de", "describe",
      "detail", "do", "done", "down", "due", "during", "each", "eg",
      "eight", "either", "eleven","else", "elsewhere", "empty",
      "enough", "etc", "even", "ever", "every", "everyone", "everything",
      "everywhere", "except", "few", "fifteen", "fify", "fill", "find",
      "fire", "first", "five", "for", "former", "formerly", "forty",
      "found", "four", "from", "front", "full", "further", "get", "give",
      "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
      "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
      "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc",
      "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last",
      "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
      "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
      "move", "much", "must", "my", "myself", "name", "namely", "neither", "never",
      "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not",
      "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
      "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves",
      "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re",
      "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she",
      "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some",
      "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still",
      "such", "system", "take", "ten", "than", "that", "the", "their", "them",
      "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore",
      "therein", "thereupon", "these", "they", "thick", "thin", "third", "this",
      "those", "though", "three", "through", "throughout", "thru", "thus", "to",
      "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un",
      "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well",
      "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
      "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which",
      "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
      "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself",
      "yourselves")

    import spark.implicits._

    val ngrams_udf = udf(generateNgrams _)

    val extractTokens_udf = udf(extractTokens _)


    //val tokenizer = new Tokenizer().setInputCol("key_value").setOutputCol("tokens")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("key_value")
      .setOutputCol("tokens")
      .setPattern("\\W")
     // .setGaps(false)

   // println("Gaps: " + regexTokenizer.getGaps)


    val keychain_Df = spark.read.option("header", "true").option("delimiter", "\t").csv("/Users/pchitturi/Downloads/KeyChain_V2_Today.tsv")
      .select("keychain_id", "key_type", "key_value")
      //.withColumn("address_length", length($"key_value"))
    //  .withColumn("key_value_ngram", ngrams_udf(col("key_value")))
       // .withColumn("tokens", extractTokens_udf(col("key_value"))(lit(1),lit(stopWords),lit("_")))

   // keychain_Df.show(10, false)

    //val tokenized_keychain_Df = tokenizer.transform(keychain_Df)






   // tokenized_keychain_Df.show(10, false)

    val regexTokenized_KeyChainDf = regexTokenizer.transform(keychain_Df)

   // regexTokenized_KeyChainDf.show(20, false)

   // println("Count : " + keychain_Df.count())

   /*val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

   // keychain_Df.select("key_type").distinct().show(20, false)

    wordDataFrame.show(10, false) */

   val ngram = new NGram().setN(2).setInputCol("tokens").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(regexTokenized_KeyChainDf)
    //ngramDataFrame.show(false)

  //  keychain_Df.show(10, false)

   println( ngramDataFrame.select("tokens").first().getAs[mutable.WrappedArray[String]](0).toList)
  }

}
