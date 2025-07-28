(ns sql2dsql.core-test
  (:require
    [clojure.test :refer :all]
    [sql2dsql.core :refer [->dsql]]))

(defn parse [sql & params]
  (try
    (-> (apply ->dsql (cons sql params))
        first)
    (catch Exception e
      (println (str "Error parsing SQL: " sql))
      (println (str "Exception: " (.getMessage e)))
      nil)))

(defmacro test-sql
  ([num sql expected]
   `(test-sql ~num ~sql [] ~expected))
  ([num sql params expected]
   `(testing (str "Test " ~num ": " ~sql)
      (let [result# (apply parse ~sql ~params)
            passed?# (= result# ~expected)]
        (do-report
          {:type (if passed?# :pass :fail)
           :message (str "\nTest " ~num " failed!"
                         "\nExpected: " ~expected
                         "\nActual:   " result#)
           :expected ~expected
           :actual result#})))))

(defmacro test-sql-meta
  [num sql params expected]
  `(testing (str "Test " ~num ": " ~sql)
     (let [result# (apply parse ~sql ~params)
           passed?# (= result# ~expected)
           meta-actual# (meta (:select result#))
           meta-expected# (meta (:select ~expected))]
       (do-report
         {:type (if passed?# :pass :fail)
          :message (str "\nTest " ~num " failed!"
                        "\nExpected: " ~expected
                        "\nActual:   " result#)
          :expected ~expected
          :actual result#})
       (when passed?#
         (let [meta-passed?# (= meta-actual# meta-expected#)]
           (do-report
             {:type (if meta-passed?# :pass :fail)
              :message (str "\nTest " ~num " metadata failed!"
                            "\nExpected meta: " meta-expected#
                            "\nActual meta:   " meta-actual#)
              :expected meta-expected#
              :actual meta-actual#}))))))

(deftest select-tests
  (testing "Basic SELECT queries"

    (test-sql 1
              "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100;"
              ["u-1"]
              {:select :*
               :from :user
               :where ^:pg/op [:= :user.id [:pg/param "u-1"]]
               :limit 100})

    (test-sql 2
              "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100"
              ["u'-1"]
              {:select :*
               :from :user
               :where ^:pg/op [:= :user.id [:pg/param "u'-1"]]
               :limit 100})

    (test-sql 3
              "SELECT a, b, c FROM \"user\""
              {:select {:a :a, :b :b, :c :c}
               :from :user})

    (test-sql 4
              "SELECT a, $1 b, c FROM \"user\""
              ["deleted"]
              {:select {:a :a
                        :b [:pg/param "deleted"]
                        :c :c}
               :from :user})

    (test-sql 5
              "SELECT a, $1 b, $2 c FROM \"user\""
              ["deleted" 9]
              {:select {:a :a
                        :b [:pg/param "deleted"]
                        :c [:pg/param 9]}
               :from :user})

    (test-sql 6
              "SELECT a, CURRENT_TIMESTAMP b, $1 c FROM \"user\""
              [9]
              {:select {:a :a
                        :b :CURRENT_TIMESTAMP
                        :c [:pg/param 9]}
               :from :user}))

  (testing "GROUP BY queries"

    (test-sql 7
              "SELECT * FROM patient GROUP BY name, same LIMIT 10"
              {:select :*
               :from :patient
               :group-by {:name :name, :same :same}
               :limit 10}))

  (testing "JOIN queries"

    (test-sql 8
              "SELECT count(*) FROM dft"
              []
              {:select {:count [:pg/count*]}
               :from :dft})

    (test-sql 9
              "SELECT count(*) as count FROM dft
               LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
               WHERE d.id is NULL"
              ["front"]
              {:select {:count [:pg/count*]}
               :from :dft
               :left-join {:d {:table :document
                               :on [:and
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]})
    )

  (testing "EXPLAIN queries"

    (test-sql 10
              "EXPLAIN ANALYZE
               SELECT count(*) as count
               FROM dft
               LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
               WHERE d.id is NULL"
              ["front"]
              {:explain {:analyze true}
               :select {:count [:pg/count*]}
               :from :dft
               :left-join {:d {:table :document
                               :on [:and
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]}))

  (testing "Complex WHERE clauses"

    (test-sql 11
              "SELECT id, resource
               FROM healthcareservices
               WHERE (
                 resource #>> '{name}' ILIKE $1
                 OR resource #>> '{type,0,coding,0,code}' ILIKE $1
                 OR resource #>> '{type,0,coding,1,code}' ILIKE $1)
               AND (
                 resource #>> '{name}' ILIKE $2 OR
                 resource #>> '{type,0,coding,0,code}' ILIKE $2 OR
                 resource #>> '{type,0,coding,1,code}' ILIKE $2
               )
               ORDER BY id"
              ["a" "b"]
              {:select {:id :id, :resource :resource}
               :from :healthcareservices
               :where [:and
                       [:or
                        [:ilike [:jsonb/#>> :resource [:name]] [:pg/param "a"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 0 :code]] [:pg/param "a"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 1 :code]] [:pg/param "a"]]]
                       [:or
                        [:ilike [:jsonb/#>> :resource [:name]] [:pg/param "b"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 0 :code]] [:pg/param "b"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 1 :code]] [:pg/param "b"]]]]
               :order-by :id})

    (test-sql 12
              "SELECT count(*) FROM oru WHERE ( resource #>> '{message,datetime}' )::timestamp > now() - interval '1 week' and id ILIKE $1"
              ["%Z%.CV"]
              {:select {:count [:pg/count*]}
               :from :oru
               :where [:and
                       [:>
                        [:pg/cast [:jsonb/#>> :resource [:message :datetime]] :pg_catalog.timestamp]
                        [:- [:now] [:pg/cast [:pg/sql "'1 week'"] :pg_catalog.interval]]]
                       [:ilike :id [:pg/param "%Z%.CV"]]]}))

  (testing "Function calls"

    (test-sql 13
              "SELECT p.resource || jsonb_build_object('id', p.id) as pr, resource || jsonb_build_object('id', id) as resource
               FROM oru
               LEFT JOIN practitioner p ON practitioner.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, requester, provider, 0, identifier, value}'
               LEFT JOIN organization org ON organization.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, contact, phone, 0, phone}'
               WHERE id ILIKE $1
               ORDER BY id
               LIMIT 5"
              ["%Z38886%"]
              {:select {:pr [:|| :p.resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :p.id]]
                        :resource [:|| :resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :id]]}
               :left-join {:org {:table :organization
                                 :on [:=
                                      :organization.id
                                      [:jsonb/#>> :p.resource [:patient_group :order_group 0 :order :contact :phone 0 :phone]]]}
                           :p {:table :practitioner
                               :on [:=
                                    :practitioner.id
                                    [:jsonb/#>>
                                     :p.resource
                                     [:patient_group :order_group 0 :order :requester :provider 0 :identifier :value]]]}}
               :from :oru
               :where [:ilike :id [:pg/param "%Z38886%"]]
               :order-by :id
               :limit 5})

    (test-sql 14
              "SELECT AVG(salary) as avg_sal FROM abc"
              {:select {:avg_sal [:avg :salary]}
               :from :abc}))

  (testing "DISTINCT queries"

    (test-sql 15
              "SELECT DISTINCT test FROM best"
              {:select-distinct {:test :test}
               :from :best})

    (test-sql 16
              "SELECT DISTINCT id as id , resource as resource , txid as txid FROM best"
              {:select-distinct {:id :id
                                 :resource :resource
                                 :txid :txid}
               :from :best}))

  (testing "DISTINCT ON queries"

    (test-sql-meta 17
                   "SELECT DISTINCT ON ( id , txid ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id :txid]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 18
                   "SELECT DISTINCT ON ( id ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 19
                   "SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [[:jsonb/#>> :resource [:id]]]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best}))

  (testing "SELECT ALL"

    (test-sql 20
              "SELECT ALL id as id , resource as resource , txid as txid FROM best"
              {:select {:id :id
                        :resource :resource
                        :txid :txid}
               :from :best}))

  (testing "GROUP BY with DISTINCT"

    (test-sql 21
              "SELECT department_id, COUNT(DISTINCT job_title) as count_job_titles FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count_job_titles [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}})

    (test-sql 22
              "SELECT department_id, COUNT(DISTINCT job_title) FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}}))

  (testing "VALUES clause"

    (test-sql 23
              "(VALUES (1, 'Alice'), (2, 'Grandma'), (3, 'Bob'))"
              {:ql/type :pg/values
               :keys [:k1 :k2]
               :values [{:k1 1 :k2 [:pg/sql "'Alice'"]}
                        {:k1 2 :k2 [:pg/sql "'Grandma'"]}
                        {:k1 3 :k2 [:pg/sql "'Bob'"]}]}))

  (testing "FETCH FIRST"

    (test-sql 24
              "SELECT * FROM employees FETCH FIRST 5 ROWS ONLY"
              {:select :*
               :from :employees
               :limit 5}))

  (testing "WITH clause (CTEs)"

    (test-sql 25
              "WITH recent_hires AS ( SELECT * FROM employees WHERE hire_date > CURRENT_DATE - INTERVAL '30 days')
               SELECT * FROM recent_hires"
              {:ql/type :pg/cte
               :with {:recent_hires {:select :*
                                     :from :employees
                                     :where [:> :hire_date
                                             [:- :CURRENT_DATE [:pg/cast [:pg/sql "'30 days'"] :pg_catalog.interval]]]}}
               :select {:select :*
                        :from :recent_hires}})

    (test-sql 26
              "WITH dept_avg AS (
                 SELECT department_id as dept_id, AVG(salary) as avg_sal
                 FROM employee
                 GROUP BY department_id
               ),
               high_earners AS (
                 SELECT *
                 FROM employee
                 WHERE salary > (SELECT AVG(salary) FROM dept_avg WHERE dept_avg.dept_id = employee.department_id)
               )
               SELECT * FROM high_earners"
              {:ql/type :pg/cte
               :with {:dept_avg {:select {:dept_id :department_id
                                          :avg_sal ^:pg/fn [:avg :salary]}
                                 :from :employee
                                 :group-by {:department_id :department_id}}
                      :high_earners {:select :*
                                     :from :employee
                                     :where [:> :salary
                                             {:ql/type :pg/sub-select
                                              :select {:avg ^:pg/fn [:avg :salary]}
                                              :from :dept_avg
                                              :where [:= :dept_avg.dept_id :employee.department_id]}]}}
               :select {:select :*
                        :from :high_earners}}))

  (testing "Set operations"

    (test-sql 27
              "SELECT name FROM employees UNION SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union {:contractors {:ql/type :pg/sub-select
                                     :select {:name :name}
                                     :from :contractors}}})

    (test-sql 28
              "SELECT name FROM employees INTERSECT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :intersect {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}})

    (test-sql 29
              "SELECT name FROM employees EXCEPT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :except {:contractors {:ql/type :pg/sub-select
                                      :select {:name :name}
                                      :from :contractors}}})

    (test-sql 30
              "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union-all {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}}))

  (testing "Additional edge cases"
    (test-sql 31
              "SELECT COUNT(*) as total, MAX(salary) as max_sal, MIN(salary) as min_sal FROM employees"
              {:select {:total [:pg/count*]
                        :max_sal [:max :salary]
                        :min_sal [:min :salary]}
               :from :employees})

    (test-sql 32
              "SELECT id, name AS employee_name FROM users WHERE active = true"
              {:select {:id :id, :employee_name :name}
               :from :users
               :where [:= :active true]})

    (test-sql 33
              "SELECT * FROM employees WHERE department_id IN (1, 2, 3)"
              {:select :*
               :from :employees
               :where [:in :department_id [:pg/list 1 2 3]]})

    (test-sql 34
              "SELECT * FROM employees WHERE name LIKE 'John%'"
              {:select :*
               :from :employees
               :where [:like :name "John%"]})

    (test-sql 35
              "SELECT CAST(salary AS TEXT) as salary_text FROM employees"
              {:select {:salary_text [:pg/cast :salary :text]}
               :from :employees})

    (test-sql 36
              "SELECT EXISTS(SELECT a as a FROM employees WHERE salary > 100000) as has_high_earners"
              {:select {:has_high_earners
                        {:ql/type :pg/sub-select,
                         :select {:a :a},
                         :from :employees,
                         :where [:> :salary 100000]}}})))

(deftest create-table-tests
  (testing "CREATE TABLE queries"
    (test-sql 0
              "CREATE TABLE \"MyTable\" ( \"a\" integer )"
              {:ql/type :pg/create-table
               :table-name :MyTable
               :columns {:a {:type "pg_catalog.int4" }}})

    (test-sql 1
              "CREATE TABLE projects (project_id INT PRIMARY KEY,\n    start_date DATE,\n    end_date DATE\n)"
              {:ql/type :pg/create-table
               :table-name :projects
               :columns {:project_id {:type "pg_catalog.int4" :primary-key true}
                         :start_date {:type "date"}
                         :end_date {:type "date"}}})

    (test-sql 2
              "CREATE TABLE IF NOT EXISTS mytable
              ( \"id\" text PRIMARY KEY , \"filelds\" jsonb , \"match_tags\" text[] , \"dedup_tags\" text[] )"
              {:ql/type :pg/create-table
               :table-name :mytable
               :if-not-exists true
               :columns {:id {:type "text", :primary-key true}
                         :filelds {:type "jsonb"}
                         :match_tags {:type "text"}
                         :dedup_tags {:type "text"}}})

    (test-sql 3
              "CREATE TABLE IF NOT EXISTS patient_000 partition of patient ( PRIMARY KEY (\"id\", \"partition\"))
              for values from (0) to (1001) partition by range ( partition )"
              {:ql/type :pg/create-table,
               :table-name :patient_000,
               :constraint {:primary-key [:id :partition]}
               :if-not-exists true,
               :partition-of "patient",
               :partition-by {:method :range,
                              :expr :partition},
               :for {:from 0, :to 1001}})

    (test-sql 4
              "CREATE TABLE mytable ( PRIMARY KEY (\"id\", \"partition\") )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :constraint {:primary-key [:id :partition]}})

    (test-sql 5
              "CREATE TABLE mytable
              ( \"id\" uuid , \"partition\" int , \"resource\" jsonb , PRIMARY KEY (\"id\", \"partition\") )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :constraint {:primary-key [:id :partition]}
               :columns {:id {:type "uuid"}, :partition {:type "pg_catalog.int4"}, :resource {:type "jsonb"}}})

    (test-sql 6
              "CREATE TABLE mytable
              ( \"id\" uuid not null , \"version\" uuid not null ,
              \"cts\" timestamptz not null DEFAULT current_timestamp ,
              \"ts\" timestamptz not null DEFAULT current_timestamp ,
              \"status\" resource_status not null , \"partition\" int not null ,
              \"resource\" jsonb not null )"
              {:ql/type :pg/create-table
               :table-name "mytable"
               :columns {:id        [:uuid "not null"]
                         :version   [:uuid "not null"]
                         :cts       [:timestamptz "not null" :DEFAULT :current_timestamp]
                         :ts        [:timestamptz "not null" :DEFAULT :current_timestamp]
                         :status    [:resource_status "not null"]
                         :partition [:int "not null"]
                         :resource  [:jsonb "not null"]}})

    (test-sql 7
              "CREATE UNLOGGED TABLE IF NOT EXISTS mytable
              ( \"id\" text PRIMARY KEY , \"filelds\" jsonb , \"match_tags\" text[] , \"dedup_tags\" text[] )"
              {:ql/type :pg/create-table
               :table-name "mytable"
               :if-not-exists true
               :unlogged true
               :columns {:id          {:type "text" :primary-key true}
                         :filelds     {:type "jsonb"}
                         :match_tags  {:type "text[]"}
                         :dedup_tags  {:type "text[]"}}})

    (test-sql 8
              "CREATE TABLE mytable ( \"a\" integer NOT NULL DEFAULT 8 )"
              {:ql/type :pg/create-table
               :table-name :mytable
               :columns {:a {:type "integer" :not-null true :default 8}}})

    (test-sql 9
              "CREATE TABLE IF NOT EXISTS part partition of whole for values from (0) to (400) partition by range ( partition )"
              {:ql/type :pg/create-table
               :table-name "part"
               :if-not-exists true
               :partition-by {:method :range :expr :partition}
               :partition-of "whole"
               :for {:from 0 :to 400}}))

    (test-sql 10
              "CREATE INDEX IF NOT EXISTS users_id_idx ON users USING GIN ( ( resource->'a' ) , ( resource->'b' ) ) WHERE user.status = ?" "active"
              {:ql/type :pg/index
               :index :users_id_idx
               :if-not-exists true
               :concurrently true
               :on :users
               :using :GIN
               :expr [[:resource-> :a] [:resource-> :b]]
               :tablespace :mytbs
               :with {:fillfactor 70}
               :where ^:pg/op[:= :user.status "active"]})
    (test-sql 11
              "CREATE TABLE mytable ( \"ts\" timestamp NOT NULL DEFAULT current_timestamp , \"meta_status\" resource_status NOT NULL DEFAULT [\"?\" \"created\"] )"
              {:ql/type :pg/create-table
               :table-name "mytable"
               :columns {:ts          {:type "timestamp"       :default :current_timestamp :not-null true}
                         :meta_status {:type "resource_status" :default "created"          :not-null true}}})

  (test-sql 12
            "CREATE EXTENSION jsonknife"
            {:ql/type :pg/create-extension
             :name "jsonknife"})

  (test-sql 13
            "CREATE EXTENSION IF NOT EXISTS jsonknife SCHEMA ext"
            {:ql/type :pg/create-extension
             :name "jsonknife"
             :schema "ext"
             :if-not-exists true})

  (test-sql 14
            "CREATE TABLE table1 AS SELECT 1"
            {:ql/type :pg/create-table-as
             :table "table1"
             :select {:ql/type :pg/select
                      :select 1}})
  (test-sql 15
            "CREATE UNIQUE INDEX IF NOT EXISTS sdl_src_dst ON sdl_src_dst ( ( src ) , ( dst ) )"
            {:ql/type :pg/index
             :index   :sdl_src_dst
             :unique  true
             :on      :sdl_src_dst
             :expr    [:src :dst]}))