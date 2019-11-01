(ns sparkdefinitive.ch13RDD2
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
           (org.apache.spark.api.java JavaRDD JavaPairRDD)
           (louna.rdds.api F1)
           (scala Tuple2)
           (java.util Arrays)))


(sparkdefinitive.init-settings/init)

(def spark (get-session))
(def sc (louna.state.settings/get-sc))

;;val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
;  .split(" ")
;val words = spark.sparkContext.parallelize(myCollection, 2)

(def words (-> (clojure.string/split "Spark The Definitive Guide : Big Data Processing Made Simple"
                                     #" ")
               (seq->rdd 2)
               (.setName "myWords")))

(prn (.collect words))
(prn (.name words))


;;Making pairs,pairs should be of type Tuple2.
;words.map(word => (word.toLowerCase, 1))

(-> words
    (mapToPair (fn [word] [(clojure.string/lower-case word) 1]))
    (.collect)
    (prn))

;;==

(-> words
    (map_ (fn [word] (t (clojure.string/lower-case word) 1))) ;;t => (Tuple2. k v)
    (.collect)
    (prn))

;;==

(-> words
    (keyBy (fn [word] (clojure.string/lower-case word)))    ;;pairs [lowercase(word) word]  ;;!!!!value before
    (mapValues (fn [value] 1))                              ;;pairs [lowercase(word) 1]
    (.collect)
    (prn))

;;words.keyBy(word => word.toLowerCase.toSeq(0).toString)
;;keyword.mapValues(word => word.toUpperCase).collect()

(def keywords (-> words
                  (keyBy (fn [word] (clojure.string/lower-case (subs word 0 1))))    ;;pairs [subs(word) word]
                  ))
(-> keywords
    (mapValues (fn [value] (clojure.string/upper-case value)))    ;;pairs [first_char uppercase(word)]
    (.collect)
    (prn))

;;keyword.flatMapValues(word => word.toUpperCase).collect()  ;;BUG

;;if [k1 v1] and v1 can be made a list of (v1.1 v1.2 v1.3)  (here 3 parts for example)
;;flattmap will add 3 new pairs [k1 v1.1] [k1 v1.2] [k1 v1.3]
;;to work we need the result of the fn to be a iterable object for example list,vector etc
(-> keywords
    (flatMapValues (fn [value] (seq (clojure.string/upper-case value))))
    (.collect)
    (prn))

;;keyword.keys.collect()
;keyword.values.collect()

(-> keywords .keys .collect prn)                            ;;get the keys
(-> keywords .values .collect prn)                          ;;get the values


;keyword.lookup("s")

;;return a "list" of the values that have as key the "s"
;;scala.collection.convert.Wrappers$SeqWrapper = that list object
(prn (.lookup keywords "s"))


;;sampleByKey  TODO (page 233)
;;val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
;  .collect()
;import scala.util.Random
;val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
;words.map(word => (word.toLowerCase.toSeq(0), word))
;  .sampleByKey(true, sampleMap, 6L)
;  .collect()

;;another method
;;words.map(word => (word.toLowerCase.toSeq(0), word))
;;.sampleByKeyExact(true, sampleMap, 6L).collect()


;;;-------Aggregations---------------


;;val chars = words.flatMap(word => word.toLowerCase.toSeq)
;val KVcharacters = chars.map(letter => (letter, 1))


(def cRDD (-> words (flatMap (fn [word] (seq (clojure.string/lower-case word))))))
(def cKV (-> cRDD (mapToPair (fn [letter] [letter 1]))))

(prn (.collect cRDD))
(prn (.collect cKV))


;;val timeout = 1000L //milliseconds
;val confidence = 0.95
;KVcharacters.countByKey()
;KVcharacters.countByKeyApprox(timeout, confidence)



;;returns a map (org.apache.spark.api.java.JavaUtils$SerializableMapWrapper)
;;{key number_the_key_appears}
;;for example if rdd= ([\e 2] [\e 5] [\e 4]) => i will get [\e 3]

(prn (.countByKey cKV))
(prn (.countByKeyApprox cKV 1000 0.95))                     ;;timeout confidence


;;def addFunc(left:Int, right:Int) = left + right


(defn addFunc [left right] (+ left right))


;;KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
;KVcharacters.reduceByKey(addFunc).collect()
;nums.aggregate(0)(maxFunc, addFunc)


;;groupby + after apply the reduce function on the seq value(map)
(-> cKV
    (.groupByKey)  ;;pair= Tuple2 [k,iterable]  (1 seq per distinct key with all the values)
    (map_ (fn [row] (t (._1 row) (reduce addFunc (._2 row)))))
    (.collect)
    (prn))

;;reduceByKey= groupByKey + apply reduce function seq value
;;this is better approach,for memory reasons,for example if a key has 1 million values we dont need
;;to first make them a sequence and then reduce them,its better to reduce them in the process
(-> cKV
    (reduceByKey addFunc)
    (.collect)
    (prn))

;;val nums = sc.parallelize(1 to 30, 5)
;;def maxFunc(left:Int, right:Int) = math.max(left, right)

(defn maxFunc [left right] (max left right))
(def nums (seq->rdd (range 1 30) 5))

;;TODO (pages 235-235 semi-skipped)
;;Low level aggregation functions,we do the aggregation thinking about partitions also
;;we dont use them in general

;;aggregate
;;apply first function maxFunc inside each partition
;;and the second function to the results (it is done on the driver => maybe memory problem)
;;0 is the neutral number
;;nums.aggregate(0)(maxFunc, addFunc)

;;like the above aggregate but dont do all the second job on driver
;;val depth = 3
;nums.treeAggregate(0)(maxFunc, addFunc, depth)

;;like aggregate but by key,not by partition
;;KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()

;;....


;;----------------coGroups-----------------------------------------

;;2-3max pair RDDS are joined,result = pairRDD [common_key tuple3([value1],[value2],[value3])]

;;import scala.util.Random
;val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
;val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
;val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
;val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
;charRDD.cogroup(charRDD2, charRDD3).take(5)

(def distinctChars (-> words
                       (flatMap (fn [word] (seq (clojure.string/lower-case word))))
                       (.distinct)))

(def charRDD (-> distinctChars
                 (mapToPair (fn [c] [c (rand)]))))

(def charRDD2 (-> distinctChars
                 (mapToPair (fn [c] [c (rand)]))))

(def charRDD3 (-> distinctChars
                 (mapToPair (fn [c] [c (rand)]))))

(-> charRDD
    (.cogroup charRDD2 charRDD3)
    (.take 5)
    (prn))

;;-----------------------------Joins----------------------------------

;;val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
;val outputPartitions = 10
;KVcharacters.join(keyedChars).count()
;KVcharacters.join(keyedChars, outputPartitions).count()


(def keyedChars (mapToPair distinctChars (fn [c] [c (rand)])))
(def outPutPartitions 10)

;;cKV pairRDD [letter 1]
;;keyedChars [letter 0.55]
;;join result  [letter [1 0.55]]  ;;for example  [1 0.55]=Tuple
(-> cKV
    (.join keyedChars)
    (.collect)
    (prn))

;;i can do other types of joins also
;fullOuterJoin
;leftOuterJoin
;rightOuterJoin
;cartesian

;;--------------zips-------------------------------------------

;;val numRange = sc.parallelize(0 to 9, 2)
;words.zip(numRange).collect()

(def numRange (seq->rdd (range 0 10) 2))

(prn (.count numRange))
(prn (.count words))

;;like in clojure but an RDD of pairs (not map)
;;2 RDDS must have same number of members and same number of partitions
;;there is also zip with index etc
(prn (.collect (.zip words numRange)))

;;-------------------------partitions-----------------------------
;;The main reason to use low level RDDs is if you want to use a custom Partitioner
;;custom partitioners are not availiable in datasets
;;custom partitioners can improve the alot the transformations
;;the main goal o custom partitioning is to make partition sizes equal => more parallelism

;;The best is to go from df->rdd(just to use custom partitioner)->df again   (when this is needed)

;;shuffle of data means moving data between partitions

;;coalesce()    ;;only for less partitions,no shuffle of data => fast
;;repartition() ;;+ or - , partitions,shuffle data can occur

;;words.coalesce(1).getNumPartitions
;;words.repartition(10) // gives us 10 partitions

(prn (.getNumPartitions (.coalesce words 1)))
(prn (.getNumPartitions (.repartition words 10)))

;;val df = spark.read.option("header", "true").option("inferSchema", "true")
;  .csv("/data/retail-data/all/")
;val rdd = df.coalesce(10).rdd

;df.printSchema()

(def df (-> spark
            (.read)
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.csv (str (settings/get-base-path) "/data/retail-data/all/"))))

(def rdd (.rdd (.coalesce df 10)))

(.printSchema df)



;;TODO (241-242 pages has some more)
;;Spark has two built-in Partitioners
;;1)HashPartitioner for discrete values
;;2)RangePartitioner. These two work for discrete  values and continuous values
;;Spark’s Structured APIs will already use these,we can use them in RDDS also


;;--------------------------Serialization-----------------------------------------

;;Spark sends objects and functions to nodes,so they must be serializable
;;Also serialization is used to save RDD's on disk
;;for example

;;class SomeClass extends Serializable {
;;    var someValue = 0
;;    def setSomeValue(i:Int) = {
;;    someValue = i
;     this
;     }
;}

;;sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))

;;In order for spark nodes to make new objects,class must be Serializable

;;Spark uses Java serialization by default but (for shuffle of simple types Kryo is used by default)
;;Kryo library (version 2) can be 10x faster,but dont support all object types,and the classes must be
;;declared before use

;;option("spark.serializer"  "org.apache.spark.serializer.KryoSerializer")  to use the Kryo

;;Register my classes to use with Kryo

;;val conf = new SparkConf().setMaster(...).setAppName(...)
;conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
;val sc = new SparkContext(conf)