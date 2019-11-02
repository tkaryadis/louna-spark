(ns louna-tests.udaf
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            louna.datasets.schema
            louna.datasets.sql)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.datasets.udf)
  (:import (org.apache.spark.sql.types DataTypes)
           (java.util ArrayList)
           (org.apache.spark.sql.expressions UserDefinedAggregateFunction MutableAggregationBuffer)
           (org.apache.spark.sql Row SparkSession)
           (louna-tests.udafAvg MyAvg)))

(settings/set-local-session)
(settings/set-log-level "ERROR")
(settings/set-base-path "/home/white/IdeaProjects/louna-spark/")

;;Proxy doesn't worked,not seriliazeble exception

;;To create a udaf
;;1)class MyUdaf extends UserDefinedAggregateFunction
;;2)MutableAggregationBuffer buffer its argument that spark is using,when calling
;;  my udaf,in that buffer i store temporary results or take the final result
;;  its spark object like array for example buffer.getLong(0)  //to get the first if its type is long
;;3)Implement those methods
;;  1)inputSchema()
;;    schema of the input arguments
;;    StructType(header) with structFields(columns) type of the Column where the
;;    aggregation will be done
;;  2)bufferSchema()
;;    schema of the temporary results,while aggregating
;;    StructType with the field(Column) schema
;;  3)dataType represents the return DataType
;;    the return Datatype,here we dont need schema,just the return type
;;  4)deterministic()
;;    true if udaf always same result(with same args),false otherwise
;;  5)initialize(MutableAggregationBuffer buffer)
;;    set the first value of the aggregation buffer
;;  6)update(MutableAggregationBuffer buffer, Row input)
;;    Row input is everytime what is left so far in the Collumn that we see as Row
;;    we take for example the first value and change the buffer
;;  7)merge(MutableAggregationBuffer buffer1, Row buffer2)
;;    merge the 2,by changing the buffer1
;;  8)evaluate will generate the final result of the aggregation
;;    for example if the final result will be the 1 element of the buffer
;;    return buffer.getLong(0)


;;Here the example will be to calculate the average of a long collumn
;;I read,longs,and the buffer holds 2 numbers,the sum and the count
;;when i finish i return sum/count
;;  1)inputSchema()  returns StructType
;;    inputSchema object will be on member of the class,and this method will return it
;;    louna.schema/build-schema([["inputColumn" :long]])
;;  2)bufferSchema()  returns StructType
;;    buffer schema object will be on member of the class,and this method will return it
;;    louna.schema/build-schema([["sum" :long] ["count" :long]])
;;    (the 2 numbers we need the sum and the count)
;;  3)dataType()
;;    return DataTypes.DoubleType;  (we will return the average in the end=> double type)
;;  4)deterministic()
;;    return true;
;;  5)initialize(MutableAggregationBuffer buffer)
;;    buffer.update(0, 0L);     //at 0 position ,which is the sum put 0
;     buffer.update(1, 0L);     //at 1 position,which is the count put 0
;;    *our buffer has only 2 values
;;  6)update(MutableAggregationBuffer buffer, Row input)
;;    add the number on the sum
;;    add 1 on count
;;  7)merge(MutableAggregationBuffer buffer1, Row buffer2)
;;    sum1=sum1+sum2;
;;    count1=count1+count2;s
;;  8)evaluate will generate the final result of the aggregation
;;    return buffer.getLong(0)/buffer.getLong(1)  (sum/count)

(def df (louna.datasets.sql/seq->df [["michael" 3000]
                  ["andy" 4500]
                  ["justin" 3500]
                  ["berta" 4000]]
                 [["name"] ["salary" :long]]))


(defudaf myAverage (MyAvg.))

(q df
   [((myAverage ?salary) ?av)]
   .show)