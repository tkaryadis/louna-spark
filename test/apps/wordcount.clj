(ns apps.wordcount
  (:require louna.state.settings)
  (:use louna.rdds.api))

(louna.state.settings/set-local-session)
(louna.state.settings/set-log-level "ERROR")
(louna.state.settings/set-base-path "/home/white/IdeaProjects/louna/")

;;counts=rdd of pairs(tuple2) [word occurencies]
(def counts1
  (-> (louna.state.settings/get-sc)
      (.textFile (str (louna.state.settings/get-base-path) "/data/wordcount")) ;;[line1 line2 ...]
      (flatMap (fn [x] (clojure.string/split x #" ")))            ;;[word1 word2]  (line became [word..] and flat after)
      (mapToPair (fn [x] [x 1]))                  ;;[[word1 1] [word2 1]]
      (reduceByKey (fn [v1 v2] (+ v1 v2)))))         ;;if same key add the second => [word sum]


;;print as collection of vectors
;;print them as collection of strings
;;save to text file
(do (prn (map (fn [t] [(._1 t) (._2 t)]) (.collect counts))) ;;print to stdout
    (prn (map (fn [t] (.toString t)) (.collect counts)))
    (.saveAsTextFile counts "/home/white/IdeaProjects/louna/out"))  ;;;;creates a directory out,with contents inside




