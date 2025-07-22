(ns sql2dsql.core
  (:require
   [clojure.data.json :as json]
   [clojure.string :as string])
  (:import
   [com.example.pgquery PgQuery]))

(defn parse-sql [query]
  (-> query
      PgQuery/parse
      (json/read-str :key-fn keyword)))

(defn star? [column-ref]
  (not (empty? (filter :A_Star (:fields column-ref)))))

(defn only-star? [target-list]
  (and (= (count target-list) 1)
       (-> (first target-list)
           :ResTarget
           :val
           :ColumnRef
           star?)))

(defn process-join [data]
  (loop [stmt data
         acc {}]
    (let [[jointype l r quals] stmt]
      (if (vector? l)
        (recur l
               (let [alias (ffirst r)
                     table (second (first r))]
                 (assoc-in acc [jointype alias] {:table table
                                                 :on quals})))
        (let [alias (ffirst r)
              table (second (first r))]
          (assoc (assoc-in acc [jointype alias] {:table table
                                                 :on quals})
                 :from l))))))

(defn join? [x]
  (and (vector? x)
       (or (= (first x) :left-join)
           (= (first x) :right-join)
           (= (first x) :join))))

(defmulti stmt->dsql (fn [x & [opts]] (ffirst x)))

(defmethod stmt->dsql :ExplainStmt [x & [opts]]
  (let [explain-stmt (:ExplainStmt x)]
    (merge {:explain (into {} (mapv #(stmt->dsql % opts) (:options explain-stmt)))}
           (stmt->dsql (:query explain-stmt) opts))))

(defmethod stmt->dsql :DefElem [x & [opts]]
  (let [def-elem (:DefElem x)]
    [(keyword (:defname def-elem)) true]))



; todo: distinct / distinct on
(defmethod stmt->dsql :SelectStmt [x & [opts]]
  (let [select-stmt (:SelectStmt x)]
    (cond->
     {:select
      (let [target-list (:targetList select-stmt)]
                (if (only-star? target-list)
                  :*
                  (into {} (map-indexed (fn [i x] (stmt->dsql x (assoc opts :column (inc i))))
                                        target-list))))}
     (:fromClause select-stmt) (merge (let [from-clause (:fromClause select-stmt)
                                             result (if (= 1 (count from-clause))
                                                      (stmt->dsql (first from-clause) opts)
                                                      (let [q (mapv (fn [x] (stmt->dsql x opts))
                                                                    from-clause)]
                                                        (into {} q)))]
                                         (if (join? result)
                                           (process-join result)
                                           {:from result})))
      (:whereClause select-stmt) (assoc :where
                                        (stmt->dsql (:whereClause select-stmt) (assoc opts :whereClause? true)))
      (:groupClause select-stmt) (assoc :group-by
                                        (into {}
                                              (map-indexed (fn [i x] (stmt->dsql x (assoc opts :column (inc i))))
                                                           (:groupClause select-stmt))))
      (:sortClause select-stmt) (assoc :order-by
                                       (let [sort-clause (:sortClause select-stmt)]
                                         (if (= 1 (count sort-clause))
                                           (stmt->dsql (first sort-clause) opts)
                                           (let [q (mapv (fn [x] (stmt->dsql x opts))
                                                         sort-clause)]
                                             (into {} q)))))
      (:limitCount select-stmt) (assoc :limit
                                       (stmt->dsql (:limitCount select-stmt))))))

(defmethod stmt->dsql :ResTarget [x & [opts]]
  (let [res-target (:ResTarget x)]
    (stmt->dsql (:val res-target) (assoc opts :name (:name res-target)))))

(defn get-column-name [column-ref & [opts]]
  (if (> (count (:fields column-ref)) 1)
    (or (some-> (:name opts) keyword)
        (keyword (str "column-" (:column opts))))
    (or
     (some-> (:name opts) keyword)
     (-> column-ref
         :fields
         first
         :String
         :sval
         keyword))))

(defmethod stmt->dsql :ColumnRef [x & [opts]]
  (cond
    (or (:whereClause? opts) (:join? opts) (:order-by? opts) (:isA_Expr? opts))
      (keyword (string/join "." (map #(-> % :String :sval) (:fields (:ColumnRef x)))))
    :else
      (let [column-ref (:ColumnRef x)
            column-name (get-column-name column-ref opts)]
        [column-name (keyword (string/join "." (map #(-> % :String :sval) (:fields column-ref))))])
    )
  )

(def operators {"->>" :jsonb/->>
                "#>>" :jsonb/#>>
                "~~*" :ilike})

(defmethod stmt->dsql :A_Expr [x & [opts]]
  (let [opts (assoc opts :isA_Expr? true)
        lexpr (stmt->dsql (-> x :A_Expr :lexpr) opts)
        rexpr  (stmt->dsql (-> x :A_Expr :rexpr) opts)
        opname (-> x :A_Expr :name first :String :sval)
        op (or (get operators opname) (keyword opname))
        expr [op lexpr rexpr]]
    (if-let [column-name (some-> (:name opts) keyword)]
      [column-name expr]
      expr)))

(defmethod stmt->dsql :RangeVar [x & [opts]]
  (if-let [aliasname (-> x :RangeVar :alias :aliasname)]
    {(keyword aliasname) (keyword (-> x :RangeVar :relname))}
    (keyword (-> x :RangeVar :relname))))

(defmethod stmt->dsql :ParamRef [x & [opts]]
  (if-let [param (nth (:params opts) (dec (-> x :ParamRef :number)) nil)]
    (if (:name opts)
      [(keyword (:name opts)) [:pg/param param]]
      [:pg/param param])
    (throw (Exception. (str "No parameter found: " (dec (-> x :ParamRef :number)))))))

(defn parse-arr [arr]
  (mapv (fn [k] (let [x (string/trim k)]
                  (try
                    (Integer/parseInt x)
                    (catch Exception _s
                      (keyword (string/replace x #"\"" ""))))))
        (string/split arr #",")))

(defmethod stmt->dsql :A_Const [x & [opts]]
  ;; todo: need to check all [x]val
  (let [aconst (:A_Const x)]
    (cond
      (:ival aconst) (:ival (:ival aconst))
      (:fval aconst) (:fval (:fval aconst))
      (:sval aconst) (let [sval (:sval (:sval aconst))]
                       (if-let [[_ arr] (re-matches #"\{(.*)\}" sval)]
                         (parse-arr arr)
                         (if (:isFuncArg? opts)
                           [:pg/sql (str \' sval \')]
                           sval
                           )
                         )
                       )
      :else
      (throw (Exception. (str "Unsupported A_Const: " aconst))))))

(defmethod stmt->dsql :FuncCall [x & [opts]]
  (let [opts (assoc opts :isFuncArg? true)
        funcname (-> x :FuncCall :funcname first :String :sval keyword)
        columnname (or (some-> opts :name keyword)
                       funcname)]
    (if (-> x :FuncCall :agg_star)
      (if (= funcname :count)
        [columnname ^:pg/fn [:pg/count*]]
        [columnname ^:pg/fn [funcname "*"]])
      (into [funcname] (mapv #(stmt->dsql % opts) (-> x :FuncCall :args)))
      )
    )
  )

;"jsonb_build_object( 'id' , p.id )"
;(with-meta [:jsonb_build_object "id" [:column- :p.id]] {:pg/fn true})
;
;(stmt->dsql {:FuncCall
;             {:funcname [{:String {:sval "jsonb_build_object"}}],
;              :args
;              [{:A_Const {:sval {:sval "id"}, :location 41}}
;               {:ColumnRef {:fields [{:String {:sval "p"}} {:String {:sval "id"}}], :location 48}}],
;              :funcformat "COERCE_EXPLICIT_CALL",
;              :location 21}})

(defmethod stmt->dsql :SQLValueFunction [x & [opts]]
  (let [f (keyword (string/replace (-> x :SQLValueFunction :op) "SVFOP_" ""))]
    (if (:name opts)
      [(keyword (:name opts)) f]
      f)))

(defmethod stmt->dsql :JoinExpr [x & [opts]]
  (let [next-opts (assoc opts :join? true)
        jointype (case (-> x :JoinExpr :jointype)
                   "JOIN_LEFT" :left-join
                   "JOIN_RIGHT" :right-join
                   :join)
        l (stmt->dsql (-> x :JoinExpr :larg) next-opts)
        r (stmt->dsql (-> x :JoinExpr :rarg) next-opts)
        quals (stmt->dsql (-> x :JoinExpr :quals) next-opts)]
    [jointype l r quals]))

(defmethod stmt->dsql :BoolExpr [x & [opts]]
  (let [boolop (case (-> x :BoolExpr :boolop)
                 "AND_EXPR" :and
                 "OR_EXPR" :or
                 :undefined-op)
        args (map (fn [arg] (stmt->dsql arg opts)) (-> x :BoolExpr :args))]
    (vec (conj args boolop))))

(defmethod stmt->dsql :NullTest [x & [opts]]
  (let [arg (stmt->dsql (-> x :NullTest :arg) opts)]
    [:is arg nil]))

(defmethod stmt->dsql :SortBy [x & [opts]]
  (let [sortby (-> x :SortBy)]
    (stmt->dsql (:node sortby) (assoc opts :order-by? true))))

(defmethod stmt->dsql :TypeCast [x & [opts]]
  (let [type-cast (:TypeCast x)
        arg (stmt->dsql (:arg type-cast) opts)
        typname (string/join  "." (map #(-> % :String :sval) (-> type-cast :typeName :names)))]
    
    [:pg/cast arg (keyword typname)]))

(defmethod stmt->dsql :default [x & [opts]]
  (prn "default" x)
  :???)

(defn ->dsql [sql & params]
  (mapv (fn [{stmt :stmt}] (stmt->dsql stmt {:params params})) (:stmts (parse-sql sql))))

(comment
  (->dsql "select * from patient where id = '1'")
)