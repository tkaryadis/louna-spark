(ns sparkdefinitive.ch12RDD1
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            louna.library.scalaInterop
            clojure.set
            clojure.string)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.rdds.api)
  (:import (org.apache.spark.sql.types DataTypes)
           [org.apache.spark.sql functions Column]
           (org.apache.spark.api.java.function Function)
           (org.apache.spark.api.java JavaRDD)
           (louna.rdds.api F1)))

(sparkdefinitive.init-settings/init)

(def spark (get-session))
(def sc (louna.state.settings/get-sc))


;;rdd from df   (.JavaRDD df)
;;df from rdd   (.toDF rdd)

;;spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

(-> (.range spark 10)
   (.toDF)
   (.javaRDD)
   (map_ (fn [r] (.getLong r 0)))
   (.collect)
   (prn))

;;val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple" .split(" ")
;;val words = spark.sparkContext.parallelize(myCollection, 2)  ;;2 partitions
;;words.setName("myWords")
;;words.name // myWords

;;RDD from memory collection

(def words (-> (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple"
                                     #" ")
               (seq->rdd 2)
               (.setName "myWords")))

(prn (.collect words))
(prn (.name words))

;;sparkContext readfiles,sparksession is better to used in general
;;spark.sparkContext.textFile("/some/path/TextFile")   => rdd of lines

;;spark.sparkContext.wholeTextFiles("/some/path/TextFile") => rdd with 2 members (filename,file_contents)

;;---------------------------------Transformations-------------------------------------------------------

;;words.distinct().count()

(-> words
    (.distinct)
    (.count)
    (prn))

;;def startsWithS(individual:String) = {
;  individual.startsWith("S")
;}
;;words.filter(word => startsWithS(word)).collect()

(-> words
    (filter_ (fn [word] (clojure.string/starts-with? word "S")))
    (.collect)
    (prn))

;;val words2 = words.map(word => (word, word(0), word.startsWith("S")))

(def words2
  (-> words
      (map_ (fn [word] [word (.charAt word 0) (clojure.string/starts-with? word "S")]))))

(prn (.collect words2))

;;words2.filter(record => record._3).take(5)

(-> words2
    (filter_ (fn [word] (get word 2)))
    (.take 5)
    (prn))


;;words.flatMap(word => word.toSeq).take(5)

(-> words
    (flatMap (fn [word] (seq word)))
    (.take 5)
    (prn))

;;words.sortBy(word => word.length() * -1).take(2)

(-> words
    (sortBy (fn [word] (* (.length word) -1)))
    (.take 2)
    (prn))

;;val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))  ;;TODO

#_(-> words
    (.randonSplit (double-array [0.5 0.5])))

;;spark.sparkContext.parallelize(1 to 20).reduce(_ + _)

(-> (range 1 20)
    (seq->rdd)
    (reduce_ +)
    (prn))

;;def wordLengthReducer(leftWord:String, rightWord:String): String = {
;  if (leftWord.length > rightWord.length)
;    return leftWord
;  else
;    return rightWord
;}
;
;words.reduce(wordLengthReducer)

(defn wordLengthReducer [lword rword]
  (if (> (.length lword) (.length rword))
    lword
    rword))

(-> words (reduce_ wordLengthReducer) (prn))


;;words.count()

(prn (.count words))

;;count takes alot of time,so we can say count with some acceptable error,and with timeout
;;val confidence = 0.95
;val timeoutMilliseconds = 400
;words.countApprox(timeoutMilliseconds, confidence)

;words.countApproxDistinct(0.05)
;words.countApproxDistinct(4, 10)

;;words.countByValue()
;;loads the distinct items in the driver program memory and counts them(only for small distict values that fit in memory)

;;words.countByValueApprox(1000, 0.95)   //same but timeout,and 95% right results

;;min,max,first,take,takeOrdered,top,takeSample
(def ardd (seq->rdd [1 2 3 4 5]))

;;max
(-> ardd
    (.max <)
    (prn))

;;min
(-> ardd
    (.min <)
    (prn))

(prn (.first ardd))                                         ;;take the firsr
(prn (.take ardd 3))                                        ;;take the 3 first

(prn (.takeOrdered ardd 3 >))                               ;;order and take the 3 first

(prn (.top ardd 3))                                              ;;take the 3 last

;val withReplacement = true   ;;true=>allow duplicates on result
;val numberToTake = 6
;val randomSeed = 100L        ;;a random number it can be any random number for example System.nanoTime.toInt
;words.takeSample(withReplacement, numberToTake, randomSeed)

(prn (.takeSample ardd true 3 100))                         ;;ardd insted of words used

;;RDD->File

;;words.saveAsTextFile("file:/tmp/bookTitle")

;;set compression codec
;;import org.apache.hadoop.io.compress.BZip2Codec
;words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])


;;words.saveAsObjectFile("/tmp/my/sequenceFilePath")  ;;save sequence files ?? (binary key-value hadoop related file)

;;if i need many actions in rdd i can cache it,so no repeat the transformations
;;MEMORY_ONLY
;;MEMORY_ONLY_SER
;;MEMORY_AND_DISK
;;MEMORY_AND_DISK_SER
;;DISK_ONLY
;;   px rdd.persist(StorageLevel.DISK_ONLY)
;;cache() = persist(StorageLevel.MEMORY_ONLY);
;;persist is lazy,its just says when you compute it keep the result

;;Checkpointing (i think its like persist but disk_only)
;;spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
;words.checkpoint()

;;words.cache()
;;words.getStorageLevel

(.cache ^JavaRDD words)

(prn (.getStorageLevel words))

;;pipe, partition members -> stdin -> command -> member of rdd
;;words.pipe("wc -l").collect()

;;("5" "5")  we have 2 partition here,with 5 words(lines) each
(-> words
    (.pipe "wc -l")
    (.collect)
    (prn))

;;in general spark is working per partition,and then i guess "reduces" the results

;;words.mapPartitions(part => Iterator[Int](1)).sum() // 2

(-> words
    (mapPartitions (fn [part] [1]))
    (reduce_ +)
    (prn))

;;every partition has an index(partition number id),here i use it
;def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
;  withinPartIterator.toList.map(
;    value => s"Partition: $partitionIndex => $value").iterator
;}
;words.mapPartitionsWithIndex(indexedFunc).collect()

;;for each partition we have an index,and an iterator with all the members (here 5 words per partition)
;;here we print a list,of index,word  (saying in what partition index each word is located)
(-> words
    (mapPartitionsWithIndex (fn [index iter]
                              (let [partition-seq (iterator-seq iter)]
                                (.iterator (map (fn [value]
                                                (str index " " value))
                                              partition-seq))))
                            true)
    (.collect)
    (prn))


;;TODO
;words.foreachPartition { iter =>
;  import java.io._
;  import scala.util.Random
;  val randomFileName = new Random().nextInt()
;  val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
;  while (iter.hasNext) {
;      pw.write(iter.next())
;  }
;  pw.close()
;}

;;with glom we collect each partition into an array
;;here we have 2 words in 2 partitions

;spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()
;// Array(Array(Hello), Array(World))

(-> (seq->rdd ["Hello" "World"] 2)
    (.glom)
    (.collect)
    (prn))

;;completed