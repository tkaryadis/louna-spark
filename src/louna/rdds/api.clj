(ns louna.rdds.api
  (:require louna.state.settings)
  (:import (org.apache.spark.api.java.function FlatMapFunction PairFunction Function Function2 VoidFunction)
           (org.apache.spark.api.java JavaRDD)
           (scala Tuple2)))

;;-----------------------functions-----------------------------

(defrecord F1 [f]
  Function
  (call [this x]
    (f x)))

(defrecord F2 [f]
  Function2
  (call [this x y]
    (f x y)))

(defrecord VF [f]
  VoidFunction
  (call [this x]
    (f x)))

(defrecord FlatMap [f]
  FlatMapFunction
  (call [this x]
    (.iterator (f x))))

;;-------------------tranformations----------------------------

;;f argument must produce an iterable sequence for example seq,vector,list
;;that will be flatted
(defn flatMap [rdd f]
  (.flatMap ^JavaRDD rdd (FlatMap. f)))

(defn map_ [rdd f]
  (.map ^JavaRDD rdd (F1. f)))

;;reduce operation on the partitions is not deterministic
(defn reduce_ [rdd f]
  (.reduce rdd (F2. f)))

(defn foreach [rdd f]
  (.foreach rdd (VF. f)))

(defn filter_ [rdd f]
  (.filter rdd (F1. f)))

(defn sortBy [rdd f]
  (.sortBy rdd (F1. f) true 1))

(defn mapPartitions [rdd f]
  (.mapPartitions rdd (FlatMap. f)))

(defn mapPartitionsWithIndex [rdd f preservesPartitioning]
  (.mapPartitionsWithIndex rdd (F2. f) preservesPartitioning))

;;-----------------transformations Pair related-------------------

(defrecord Pair [f]
  PairFunction
  (call [this x]
    (let [vector-pair (f x)]
      (Tuple2. (first vector-pair) (second vector-pair)))))

(defn t [k v]
  (Tuple2. k v))

;;TODO
;;map_ to (Tuple. a1 a2)  dont produce a JavaPairRDD (rdd with tutples produce)so i have to use mapToPair for it
(defn mapToPair [rdd f]
  (.mapToPair rdd (Pair. f)))

(defn reduceByKey [rdd f]
  (.reduceByKey rdd (F2. f)))

(defn keyBy [rdd f]
  (.keyBy rdd (F1. f)))

(defn mapValues [pair-rdd f]
  (.mapValues pair-rdd (F1. f)))

(defn flatMapValues [pair-rdd f]
  (.flatMapValues pair-rdd (F1. f)))

;;-----------Create RDDS----------------------

(defn seq->rdd
  ([col]
   (.parallelize (louna.state.settings/get-sc) col))
  ([col numpartitions]
   (.parallelize (louna.state.settings/get-sc) col numpartitions)))
