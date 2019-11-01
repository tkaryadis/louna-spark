(ns louna.datasets.schema
  (:import (org.apache.spark.sql.types StructType Metadata DataTypes StructField)))

(def schema-types
  {:binary    DataTypes/BinaryType
   :boolean   DataTypes/BooleanType
   :byte      DataTypes/ByteType
   :date      DataTypes/DateType
   :double    DataTypes/DoubleType
   :float     DataTypes/FloatType
   :integer   DataTypes/IntegerType
   :long      DataTypes/LongType
   :null      DataTypes/NullType
   :short     DataTypes/ShortType
   :string    DataTypes/StringType
   :timestamp DataTypes/TimestampType})

(defn array-type
  "create spark array type"
  ([element-type nullable?]
   (if (keyword? element-type)
     (DataTypes/createArrayType (get schema-types element-type) nullable?)
     (DataTypes/createArrayType element-type nullable?)))
  ([element-type] (array-type element-type true)))


;;structField=metadata of Column
;;structType=array of structFiels = header
;;default string-type ,null true and no metadata
(defn build-schema [cols-info]
  "posible col-info = [[name0] [name1 type1] [name2 type2 false] [name3 type3 false metadata]]"
  (let [structfields
        (reduce (fn [fields col-info]
                  (conj fields (StructField. (first col-info)
                                             (if (>= (count col-info) 2)
                                               (get schema-types (second col-info) (second col-info))
                                               DataTypes/StringType)
                                             (if (>= (count col-info) 3)
                                               (nth col-info 2)
                                               true)
                                             (Metadata/empty))))
                []
                cols-info)]
    (StructType. (into-array structfields))))