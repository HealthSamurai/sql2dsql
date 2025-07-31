(ns sql2dsql.core
  (:require
    [clojure.data.json :as json]
    [clojure.string :as string])
  (:import
    [com.example.pgquery PgQuery]))

;; =======================================================================
;; 1. PARSE SQL
;; =======================================================================

(defn parse-sql [query]
  (-> query
      PgQuery/parse
      (json/read-str :key-fn keyword)))

;; =======================================================================
;; 2. MULTIMETHOD
;; =======================================================================

(defmulti stmt->dsql (fn [x & [_]]
                       (if (map? x) (ffirst x) :unknown)))

;; =======================================================================
;; 3. HELPERS
;; =======================================================================

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

;; =======================================================================
;; 4. MAIN METHODS
;; =======================================================================

;; ============================
;; EXPLAIN STATEMENTS
;; ============================

(defmethod stmt->dsql :ExplainStmt [x & [opts]]
  (let [explain-stmt (:ExplainStmt x)]
    (merge {:explain (into {} (mapv #(stmt->dsql % opts) (:options explain-stmt)))}
           (stmt->dsql (:query explain-stmt) opts))))

;; ============================
;; DEF ELEMENTS
;; ============================

(defmethod stmt->dsql :DefElem [x & [_]]
  (let [def-elem (:DefElem x)]
    [(keyword (:defname def-elem)) true]))

(defmethod stmt->dsql :DefElem [x & [_]]
  (let [elem (:DefElem x) kind (:defname elem)]
    (cond
      (= kind "schema") [:schema (-> elem :arg :String :sval)]
      (= kind "new_version") [:version (-> elem :arg :String :sval)]
      :else [(keyword kind) true])))

;; ============================
;; SUBLINKS
;; ============================

(defmethod stmt->dsql :SubLink [x & [opts]]
  (let [subselect (-> x :SubLink :subselect)
        base-result (stmt->dsql subselect opts)]
    (merge {:ql/type :pg/sub-select}
           (dissoc base-result :ql/type))))

;; ============================
;; SELECT STATEMENTS
;; ============================

(defn handle-target-list [target-list & [opts]]
    (if (only-star? target-list)
      :*
      (let [target-results (map-indexed (fn [i x] (stmt->dsql x (assoc opts :column (inc i)))) target-list)]
        (if (coll? (first target-results))
          (into {} target-results)
          (first target-results)))))

(defn on-distinct [distinct-on target-list & [opts]]
  (if (empty? (first distinct-on))
    {:select-distinct (handle-target-list target-list opts)}
    {:select
     (let [target (handle-target-list target-list opts)
           meta_ {:distinct-on (into [] (map (fn [x] (stmt->dsql x (assoc opts :not-include-col-name? true))) distinct-on))}]
       (with-meta target {:pg/projection meta_}))}))

(defn select-base [select-stmt & [opts]]
  (if (:distinctClause select-stmt)
    (on-distinct (:distinctClause select-stmt) (:targetList select-stmt) opts)
    {:select (handle-target-list (:targetList select-stmt) opts)}))

(defn vl-get-size [lst]
  (-> lst :List :items count))

(defn vl-transform [lst & [opts]]
  (into [] (map #(stmt->dsql % opts) (-> lst :List :items))))

(defn handle-values-lists [valuesLists & [opts]]
  (let [max-length (reduce max (map vl-get-size valuesLists))
        keys_ (mapv (comp keyword #(str "k" %)) (range 1 (inc max-length)))]
    {:ql/type :pg/values
     :keys keys_
     :values (into [] (map (fn [x] (zipmap keys_ (vl-transform x opts))) valuesLists))}))

(defn process-from-clause [select-stmt & [opts]]
  (let [from-clause (:fromClause select-stmt)
        result (if (= 1 (count from-clause))
                 (stmt->dsql (first from-clause) opts)
                   (into {} (mapv (fn [x] (stmt->dsql x opts)) from-clause)))]
    (if (join? result)
      (process-join result)
      {:from result})))

(defn process-group-by-clause [select-stmt & [opts]]
  (into {} (map-indexed
             (fn [i x] (stmt->dsql x (assoc opts :column (inc i) :group-by? true)))
             (:groupClause select-stmt))))

(defn process-sort-clause [select-stmt & [opts]]
  (let [sort-clause (:sortClause select-stmt)]
    (if (= 1 (count sort-clause))
      (stmt->dsql (first sort-clause) opts)
      (let [q (mapv (fn [x] (stmt->dsql x opts))
                    sort-clause)]
        (into {} q)))))

(defn get-op-type [op all]
  (case op
    "SETOP_UNION" (if all :union-all :union)
    "SETOP_INTERSECT" :intersect
    "SETOP_EXCEPT" :except
    (throw (Exception. ^String (str "Unknown set operation: " op)))))

(defn process-op [select-stmt & [opts]]
  (let [op-key (get-op-type (:op select-stmt) (:all select-stmt))
        l-arg (if (:SelectStmt (:larg select-stmt))
                (:larg select-stmt)
                {:SelectStmt (:larg select-stmt)})
        r-arg (if (:SelectStmt (:rarg select-stmt))
                (:rarg select-stmt)
                {:SelectStmt (:rarg select-stmt)})
        l-result (stmt->dsql l-arg opts)
        r-result (stmt->dsql r-arg opts)
        ;; The left side provides the base structure
        left-select (if (map? l-result) (:select l-result) nil)
        left-from (if (map? l-result) (:from l-result) nil)
        ;; Extract the table name from the right side's FROM clause
        right-table-name (cond
                           (and (map? r-result) (keyword? (:from r-result))) (:from r-result)
                           (and (map? r-result) (map? (:from r-result))) (first (keys (:from r-result)))
                           :else :right)
        ;; Add :ql/type :pg/sub-select to the right side
        right-val (if (map? r-result)
                    (assoc r-result :ql/type :pg/sub-select)
                    r-result)]
    (cond-> {}
            left-select (assoc :select left-select)
            left-from (assoc :from left-from)
            true (assoc op-key {right-table-name right-val}))))

(defn select-body->dsql [select-stmt opts]
  (cond->
    (select-base select-stmt opts)
    (:fromClause select-stmt) (merge (process-from-clause select-stmt opts))
    (:whereClause select-stmt) (assoc :where (stmt->dsql (:whereClause select-stmt) (assoc opts :not-include-col-name? true)))
    (:groupClause select-stmt) (assoc :group-by (process-group-by-clause select-stmt opts))
    (:sortClause select-stmt) (assoc :order-by (process-sort-clause select-stmt opts))
    (:limitCount select-stmt) (assoc :limit (stmt->dsql (:limitCount select-stmt)))))

(defn process-with-clause [select-stmt & [opts]]
  (let [with-map (into {}
                       (map (fn [cte]
                              [(keyword (:ctename (:CommonTableExpr cte)))
                               (stmt->dsql (:ctequery (:CommonTableExpr cte)) opts)])
                            (get-in select-stmt [:withClause :ctes])))
        select-body (select-body->dsql select-stmt opts)]
    {:ql/type :pg/cte
     :with with-map
     :select select-body}))

(defmethod stmt->dsql :SelectStmt [x & [opts]]
  (let [select-stmt (:SelectStmt x)]
    (cond
      (:valuesLists select-stmt) (handle-values-lists (:valuesLists select-stmt) (assoc opts :val-lists? true))
      (:withClause select-stmt) (process-with-clause select-stmt x opts)
      (not= (:op select-stmt) "SETOP_NONE") (process-op select-stmt opts)
      :else (select-body->dsql select-stmt opts))))

;; ============================
;; RES TARGETS
;; ============================

(defmethod stmt->dsql :ResTarget [x & [opts]]
  (let [res-target (:ResTarget x)
        name (:name res-target)
        val (:val res-target)]
    (if (nil? name)
      (stmt->dsql val opts)
      [(keyword name) (stmt->dsql val (assoc opts :as-stmt? true))])))

;; ============================
;; COLUMN REFERENCES
;; ============================

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
          keyword) )))

(defmethod stmt->dsql :ColumnRef [x & [opts]]
  (cond
    (or (:not-include-col-name? opts) (:join? opts) (:order-by? opts)
        (:func-arg? opts) (:as-stmt? opts) (:not-include-col-name? opts))
      (keyword (string/join "." (map #(-> % :String :sval) (:fields (:ColumnRef x)))))
    :else
      (let [column-ref (:ColumnRef x)
            column-name (get-column-name column-ref opts)]
        [column-name (keyword (string/join "." (map #(-> % :String :sval) (:fields column-ref))))])))

;; ============================
;; A_Expr EXPRESSIONS
;; ============================

(def operators {"->>" :jsonb/->>
                "#>>" :jsonb/#>>
                "~~*" :ilike
                "~~" :like})

(defn get-op-name [x]
  (if (= (-> x :A_Expr :kind) "AEXPR_IN")
    "in"
    (-> x :A_Expr :name first :String :sval)))

(defmethod stmt->dsql :A_Expr [x & [opts]]
  (let [opts (assoc opts :not-include-col-name? true)
        lexpr (stmt->dsql (-> x :A_Expr :lexpr) opts)
        rexpr  (stmt->dsql (-> x :A_Expr :rexpr) opts)
        opname (get-op-name x)
        op (or (get operators opname) (keyword opname))
        expr [op lexpr rexpr]]
    (if-let [column-name (some-> (:name opts) keyword)]
      [column-name expr]
      expr)))

;; ============================
;; LIST EXPRESSIONS
;; ============================

(defmethod stmt->dsql :List [x & [opts]]
  (into [:pg/list] (map #(stmt->dsql % opts) (:items (:List x)))))

;; ============================
;; RANGE VARIABLES
;; ============================

(defmethod stmt->dsql :RangeVar [x & [_]]
  (if-let [aliasname (-> x :RangeVar :alias :aliasname)]
    {(keyword aliasname) (keyword (-> x :RangeVar :relname))}
    (keyword (-> x :RangeVar :relname))))

;; ============================
;; PARAMETER REFERENCES
;; ============================

(defmethod stmt->dsql :ParamRef [x & [opts]]
  (if-let [param (nth (:params opts) (dec (-> x :ParamRef :number)) nil)]
    (if (:name opts)
      [(keyword (:name opts)) [:pg/param param]]
      [:pg/param param])
    (throw (Exception. ^String (str "No parameter found: " (dec (-> x :ParamRef :number)))))))

;; ============================
;; A_CONSTANTS
;; ============================

(defn parse-arr [arr]
  (mapv (fn [k] (let [x (string/trim k)]
                  (try
                    (Integer/parseInt x)
                    (catch Exception _s
                      (keyword (string/replace x #"\"" ""))))))
        (string/split arr #",")))

(defmethod stmt->dsql :A_Const [x & [opts]]
  (let [aconst (:A_Const x)]
    (cond
      (:boolval aconst) (:boolval (:boolval aconst))
      (:ival aconst) (if-let [r (-> aconst :ival :ival)] r 0)
      (:fval aconst) (:fval (:fval aconst))
      (:sval aconst) (let [sval (:sval (:sval aconst))]
                       (if-let [[_ arr] (re-matches #"\{(.*)\}" sval)]
                         (parse-arr arr)
                         (if (or (:func-arg? opts) (:val-lists? opts) (:type-cast? opts))
                           [:pg/sql (str \' sval \')]
                           (keyword sval))))
      :else
      (throw (Exception. ^String (str "Unsupported A_Const: " aconst))))))

;; ============================
;; FUNCTION CALLS
;; ============================

(defn func-call-on-distinct [func-name args & [opts]]
  (let [arguments (mapv #(stmt->dsql % opts) args)
        arg-set (into [:pg/columns] arguments)
        name (if-let [name (:name opts)] (keyword name) func-name)]
    (if (:as-stmt? opts)
      [func-name [:distinct arg-set]]
      [name [func-name [:distinct arg-set]]])))

(defn func-call-on-agg_star [func-name & [opts]]
  (if (:as-stmt? opts)
    (if (= func-name :count)
        [:pg/count*]
        [func-name "*"])
    (if (= func-name :count)
      [:count [:pg/count*]]
      [func-name (with-meta [func-name "*"] {:pg/fn true})])))

(defn func-call-on-no-args [func-name & [_]]
  (with-meta [func-name] {:pg/fn true}))

(defn func-call-default [x func-name & [opts]]
  (let [args (mapv #(stmt->dsql % opts) (-> x :FuncCall :args))
        func (with-meta (into [func-name] args) {:pg/fn true})
        name (if-let [name (:name opts)] (keyword name) func-name)]
    (cond
      (:as-stmt? opts) func
      (:index-expr? opts) (into [:pg/call] func)
      :else [name func])))

(defmethod stmt->dsql :FuncCall [x & [opts]]
  (let [opts (assoc opts :func-arg? true)
        func-name (-> x :FuncCall :funcname first :String :sval keyword)
        args (-> x :FuncCall :args)]
    (cond
      (-> x :FuncCall :agg_distinct) (func-call-on-distinct func-name args opts)
      (-> x :FuncCall :agg_star) (func-call-on-agg_star func-name opts)
      (empty? args) (func-call-on-no-args func-name opts)
      :else (func-call-default x func-name opts))))

;; ============================
;; SQL VALUE FUNCTIONS
;; ============================

(defmethod stmt->dsql :SQLValueFunction [x & [opts]]
  (let [f (keyword (string/replace (-> x :SQLValueFunction :op) "SVFOP_" ""))]
    (if (:name opts)
      [(keyword (:name opts)) f]
      f)))

;; ============================
;; JOIN EXPRESSIONS
;; ============================

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

;; ============================
;; BOOLEAN EXPRESSIONS
;; ============================

(defmethod stmt->dsql :BoolExpr [x & [opts]]
  (let [boolop (case (-> x :BoolExpr :boolop)
                 "AND_EXPR" :and
                 "OR_EXPR" :or
                 :undefined-op)
        args (map (fn [arg] (stmt->dsql arg opts)) (-> x :BoolExpr :args))]
    (vec (conj args boolop))))

;; ============================
;; NULL TESTS
;; ============================

(defmethod stmt->dsql :NullTest [x & [opts]]
  (let [arg (stmt->dsql (-> x :NullTest :arg) opts)]
    [:is arg nil]))

;; ============================
;; SORT BY EXPRESSIONS
;; ============================

(defmethod stmt->dsql :SortBy [x & [opts]]
  (let [sortby (-> x :SortBy)]
    (stmt->dsql (:node sortby) (assoc opts :order-by? true))))

;; ============================
;; TYPE CASTS
;; ============================

(defmethod stmt->dsql :TypeCast [x & [opts]]
  (let [type-cast (:TypeCast x)
        arg (stmt->dsql (:arg type-cast) (assoc opts :type-cast? true))
        typname (string/join  "." (map #(-> % :String :sval) (-> type-cast :typeName :names)))]
    [:pg/cast arg (keyword typname)]))

;; ============================
;; RANGE SUBSELECTS
;; ============================

(defmethod stmt->dsql :RangeSubselect [x & [opts]]
  (let [x (:RangeSubselect x) subquery (:subquery x)]  ;; alias (:alias x) ; develop alias logic
    (assoc (stmt->dsql subquery opts) :alias :to-be-implemented)))

;; ============================
;; CREATE STATEMENTS
;; ============================

(defn handle-table-elements [table-elems & [opts]]
  (let [constraints (filter #(:Constraint %) table-elems)
        columns (filter #(:ColumnDef %) table-elems)]
    (cond->
      {}
      (not (empty?  constraints)) (assoc :constraint  (into {} (map #(stmt->dsql % opts) constraints)))
      (not (empty? columns)) (assoc :columns (into {} (map #(stmt->dsql % opts) columns))))))

(def strategy-methods {"PARTITION_STRATEGY_RANGE" :range
                       "PARTITION_STRATEGY_HASH" :hash})

;; now can only handle range partitioning
(defn handle-partition [name part-spec part-bound & [opts]]
  (let [method (get strategy-methods (:strategy part-spec))]
    {:partition-of name
     :partition-by {:method method
                    :expr (-> part-spec :partParams first :PartitionElem :name keyword)}
     :for (case method
            :range  {:from (stmt->dsql (-> part-bound :lowerdatums first) opts)
                     :to (stmt->dsql (-> part-bound :upperdatums first) opts)}
            :hash {:modulus (:modulus part-bound) })}))

(def rel-persistence {"p" {:permanent true}
                      "t" {:temporary true}
                      "u" {:unlogged true}})

(defmethod stmt->dsql :CreateStmt [x & [opts]]
  (let [create-stmt (:CreateStmt x)
        rel_persistence (get rel-persistence (-> create-stmt :relation :relpersistence))]
    (cond->
      {:ql/type :pg/create-table :table-name (-> create-stmt :relation :relname keyword)}
      (:if_not_exists create-stmt) (assoc :if-not-exists true)
      (:unlogged rel_persistence) (assoc :unlogged true)
      (:temporary rel_persistence) (assoc :temporary true)
      (:tableElts create-stmt) (merge (handle-table-elements (:tableElts create-stmt) opts))
      (:partspec create-stmt) (merge (handle-partition
                                       (-> create-stmt :inhRelations first :RangeVar :relname)
                                       (:partspec create-stmt)
                                       (:partbound create-stmt) opts)))))

;; ============================
;; COLUMN DEFINITION CLAUSE
;; ============================

(defn handle-int [intgr]
  (let [val (-> intgr :Integer :ival)]
    (if (= val -1)
      "[]"
      (str "[" val "]"))))

(defn get-col-type [col-type]
  (let [type (string/join "." (map #(-> % :String :sval) (:names col-type)))
        array-bounds (:arrayBounds col-type)]
    (if array-bounds
      (reduce str type (map handle-int array-bounds))
      type)))

(defmethod stmt->dsql :ColumnDef [x & [opts]]
  (let [col (:ColumnDef x)
        col-name (keyword (:colname col))
        col-type (get-col-type (:typeName col))
        col-clause (:collClause col)
        constraints (map #(stmt->dsql % opts) (:constraints col))]
    {col-name
     (if (nil? col-clause)
       (reduce into [col-type] constraints)
       [col-type "COLLATE" (-> col-clause :collname first :String :sval)])}))

;; ============================
;; CONSTRAINTS
;; ============================

(def actions_ {"c" "CASCADE"
               "a" false})

(defn on-foreign [con & [_]]
  (let [args (string/join (map #(-> % :String :sval) (:pk_attrs con)))]
    (cond->
      [(str "REFERENCE " (-> con :pktable :relname) "(" args ")")]
      (get actions_ (:fk_upd_action con)) (conj (str "ON UPDATE " (get actions_ (:fk_upd_action con))))
      (get actions_ (:fk_del_action con)) (conj (str "ON DELETE " (get actions_ (:fk_del_action con)))))))

(defmethod stmt->dsql :Constraint [con & [opts]]
  (let [con (:Constraint con)]
    (if-let [con-type (:contype con)]
      (case con-type
        "CONSTR_PRIMARY" (if (:keys con)
                           {:primary-key (into [] (map #(-> % :String :sval keyword) (:keys con)))}
                           ["PRIMARY KEY"])
        "CONSTR_NOTNULL" ["NOT NULL"]
        "CONSTR_DEFAULT" [:DEFAULT  (stmt->dsql (:raw_expr con) opts)]
        "CONSTR_UNIQUE" ["UNIQUE"]
        "CONSTR_FOREIGN" (on-foreign con opts)
        :else (throw (Exception. ^String (str "Unimplemented"))))
      (throw (Exception. ^String (str "No constraint type provided"))))))

;; ============================
;; CREATE EXTENSION STATEMENTS
;; ============================

(defmethod stmt->dsql :CreateExtensionStmt [x & [opts]]
  (let [ce-stmt (:CreateExtensionStmt x)]
    (cond->
      {:ql/type :pg/create-extension :name (-> ce-stmt :extname keyword)}
      (:if_not_exists ce-stmt) (assoc :if-not-exists true)
      (:options ce-stmt) (merge (into {} (map #(stmt->dsql % opts)) (:options ce-stmt))))))

;; ============================
;; CREATE TABLE AS STATEMENTS
;; ============================

(defmethod stmt->dsql :CreateTableAsStmt [x & [opts]]
  (let [create-table-as-stmt (:CreateTableAsStmt x)
        name (-> create-table-as-stmt :into :rel :relname keyword)
        rel_persistence (get rel-persistence (-> create-table-as-stmt :relation :relpersistence))]
    (cond->
      {:ql/type :pg/create-table-as :table name}
      (:if_not_exists create-table-as-stmt) (assoc :if-not-exists true)
      (:unlogged rel_persistence) (assoc :unlogged true)
      (:query create-table-as-stmt) (assoc :select (merge
                                                          {:ql/type :pg/select}
                                                          (stmt->dsql (:query create-table-as-stmt) opts))))))

;; ============================
;; INDEX STATEMENTS
;; ============================

(defmethod stmt->dsql :IndexStmt [x & [opts]]
  (let [idx-stmt (:IndexStmt x)]
    (cond->
      {:ql/type :pg/index
       :index (-> idx-stmt :idxname keyword)
       :on (-> idx-stmt :relation :relname keyword)}
      (:if_not_exists idx-stmt) (assoc :if-not-exists true)
      (:unique idx-stmt) (assoc :unique true)
      (:options idx-stmt) (assoc :schema (-> idx-stmt :options first :DefElem :arg :String :sval))
      (:accessMethod idx-stmt) (assoc :using (-> idx-stmt :accessMethod keyword))
      (:indexParams idx-stmt) (assoc
                                :expr
                                (into [] (map #(stmt->dsql % (assoc opts :index-expr? true)) (:indexParams idx-stmt))))
      (:whereClause idx-stmt) (assoc :where
                                     (stmt->dsql
                                       (:whereClause idx-stmt)
                                       (assoc opts :not-include-col-name? true))))))

;; ============================
;; INDEX ELEMENT
;; ============================

(defmethod stmt->dsql :IndexElem [index-stmt & [opts]]
  (if-let [name (-> index-stmt :IndexElem :name)]
    (keyword name)
    (stmt->dsql (-> index-stmt :IndexElem :expr) (assoc opts :not-include-col-name? true))))


;; ============================
;; DEFAULT HANDLER
;; ============================

(defmethod stmt->dsql :default [_ & [_]] :???)

(defn ->dsql [sql & params]
  (mapv (fn [{stmt :stmt}] (stmt->dsql stmt {:params params})) (:stmts (parse-sql sql))))

(comment
  (->dsql "select * from patient where id = '1'")
  )