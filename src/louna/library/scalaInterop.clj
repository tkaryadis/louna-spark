(ns louna.library.scalaInterop)

(defn clj->scala [seq]
  (let [scala-buf (scala.collection.mutable.ListBuffer.)]
    (doall (map #(.$plus$eq scala-buf %) (flatten [seq])))
    scala-buf))