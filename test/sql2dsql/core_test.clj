(ns sql2dsql.core-test
  (:require
   [clojure.test :refer :all]
   [sql2dsql.core :refer [->dsql]]))

(defn parse [sql & params]
  (-> (apply ->dsql (cons sql params))
      first))

(defn assertRight? [expected-dsql ^String sql & params]
  (let [res-dsql (parse sql params)]
    (if (= res-dsql expected-dsql)
      (let [meta_actual (meta (:select res-dsql))           ;; works only with directly meta at select stmts
            meta_exected (meta (:select expected-dsql))]
        (if (= meta_actual meta_exected)
          true
          (do
            (println "--> Test failed: difference at meta")
            (println "    The actual meta: " meta_actual)
            (println "    The expected meta: " meta_exected)
            (println "")
            false
            )
          )
        )
      (do
        (println "--> Test failed")
        (println "    The actual result: " res-dsql)
        (println "    The expected result: " expected-dsql)
        (println "")
        false
        )
      )
    )
  )

(deftest select-tests
  (testing "select"
    (is (= (parse "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100;" "u-1")
           {:select :*
            :from :user
            :where ^:pg/op [:= :user.id [:pg/param "u-1"]]
            :limit 100}))

    (is (= (parse "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100" "u'-1")
           {:select :*
            :from :user
            :where ^:pg/op [:= :user.id [:pg/param "u'-1"]]
            :limit 100}))

    (is (= (parse "SELECT a, b, c FROM \"user\"")
           {:select {:a :a
                     :b :b
                     :c :c}
            :from :user}))

    (is (= (parse "SELECT a, $1 b, c FROM \"user\"" "deleted")
           {:select {:a :a
                     :b [:pg/param "deleted"]
                     :c :c}
            :from :user}))

    (is (= (parse "SELECT a, $1 b, $2 c FROM \"user\"" "deleted" 9)
           {:select {:a :a
                     :b [:pg/param "deleted"]
                     :c [:pg/param 9]}
            :from :user}))

    (is (= (parse "SELECT a, CURRENT_TIMESTAMP b, $1 c FROM \"user\"" 9)
           {:select {:a :a
                     :b :CURRENT_TIMESTAMP
                     :c [:pg/param 9]}
            :from :user}))

    (is (= (parse "SELECT * FROM patient GROUP BY name, same LIMIT 10")
           {:select :*
            :from :patient
            :group-by {:name :name
                       :same :same}
            :limit 10}))


    (is (= (parse
            "SELECT count(*) as count FROM dft
             LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
             WHERE d.id is NULL"
            "front")
           {:select {:count [:pg/count*]}
            :from :dft
            :left-join {:d {:table :document
                            :on [:and
                                 ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                 ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
            :where ^:pg/op [:is :d.id nil]}))

    (is (= (parse
            "EXPLAIN ANALYZE
             SELECT count(*) as count
             FROM dft
             LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
             WHERE d.id is NULL"
            "front")
           {:explain {:analyze true}
            :select {:count [:pg/count*]}
            :from :dft
            :left-join {:d {:table :document
                            :on [:and
                                 ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                 ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
            :where ^:pg/op [:is :d.id nil]}))

    (is (= (parse
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
            "a"
            "b")
           {:select {:id :id
                     :resource :resource}
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
            :order-by :id}))

    (is (= (parse
            "SELECT count(*) FROM oru WHERE ( resource #>> '{message,datetime}' )::timestamp > now() - interval '1 week' and id ILIKE $1"
            "%Z%.CV")
           {:select {:count [:pg/count*]}
            :from :oru
            :where [:and
                    [:>
                     [:pg/cast [:jsonb/#>> :resource [:message :datetime]] :pg_catalog.timestamp]
                     [:- [:now] [:pg/cast "1 week" :pg_catalog.interval]]]
                    [:ilike :id [:pg/param "%Z%.CV"]]]}))
    )
  (testing "select FuncCall case"
    ; includes FuncCall assumption
    (is (= (parse "SELECT p.resource || jsonb_build_object('id', p.id) as pr, resource || jsonb_build_object('id', id) as resource
                   FROM oru
                   LEFT JOIN practitioner p ON practitioner.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, requester, provider, 0, identifier, value}'
                   LEFT JOIN organization org ON organization.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, contact, phone, 0, phone}'
                   WHERE id ILIKE $1
                   ORDER BY id
                   LIMIT 5"
                  "%Z38886%")
           {:select {:pr [:|| :p.resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :p.id]],
                     :resource [:|| :resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :id]]},
            :left-join {:org {:table :organization,
                              :on [:=
                                   :organization.id
                                   [:jsonb/#>> :p.resource [:patient_group :order_group 0 :order :contact :phone 0 :phone]]]},
                        :p {:table :practitioner,
                            :on [:=
                                 :practitioner.id
                                 [:jsonb/#>>
                                  :p.resource
                                  [:patient_group :order_group 0 :order :requester :provider 0 :identifier :value]]]}},
            :from :oru,
            :where [:ilike :id [:pg/param "%Z38886%"]],
            :order-by :id,
            :limit 5}))
    )

  (testing "select Distinct"
    (is (assertRight?
          {:select-distinct {:test :test}
           :from :best}
          "SELECT DISTINCT test FROM best"))

    (is (assertRight?
          {:select-distinct
           {:id       :id
            :resource :resource
            :txid     :txid}
           :from :best}
          "SELECT DISTINCT id as id , resource as resource , txid as txid FROM best"))
    )

  (testing "select Distinct on"
    (is (assertRight?
          {:select
                    ^{:pg/projection {:distinct-on [:id :txid]}}
                    {:id       :id
                     :resource :resource
                     :txid     :txid}
           :from :best}
          "SELECT DISTINCT ON ( id , txid ) id as id , resource as resource , txid as txid FROM best"))

    (is (assertRight?
          {:select
                    ^{:pg/projection {:distinct-on [:id]}}
                    {:id       :id
                     :resource :resource
                     :txid     :txid}
           :from :best}
          "SELECT DISTINCT ON ( id ) id as id , resource as resource , txid as txid FROM best"))

    (is (assertRight?
          {:select
                    ^{:pg/projection {:distinct-on [[:jsonb/#>> :resource [:id]]]}}
                    {:id       :id
                     :resource :resource
                     :txid     :txid}
           :from :best}
          "SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"))
    )

  (testing "select ALL"
    (is (assertRight?
          {:select
                 {:id       :id
                  :resource :resource
                  :txid     :txid}
           :from :best}
          "SELECT ALL id as id , resource as resource , txid as txid FROM best"))
    )

  ;(testing "select into"                             not supported in dsql
  ;  (is (assertRight?
  ;        {}
  ;        "SELECT employee_id, name, salary INTO high_earner FROM employee WHERE salary > 8000")))

  (testing "select group distinct"                          ;; always func(...) as func
    (is (assertRight?
          {:select    {:department_id :department_id
                       :count_job_titles         [:count [:distinct [:pg/columns :job_title]]]}
           :from      :employee
           :group-by  {:department_id :department_id}}
          "SELECT department_id, COUNT(DISTINCT job_title) as count_job_titles FROM employee GROUP BY department_id"))
    (is (assertRight?
          {:select    {:department_id :department_id
                       :count         [:count [:distinct [:pg/columns :job_title]]]}
           :from      :employee
           :group-by  {:department_id :department_id}}
          "SELECT department_id, COUNT(DISTINCT job_title) FROM employee GROUP BY department_id"))
    )

  ;(testing "select ... group by ... having"          not supported in dsql
  ;  (is (assertRight?
  ;        {}
  ;        "SELECT department_id, AVG(salary) AS avg_salary FROM employee
  ;        WHERE status = 'active' GROUP BY department_id HAVING AVG(salary) > 50000"))
  ;  )

  ;(testing "select window"                           not supported in dsql
  ;  (is (assertRight?
  ;        {}
  ;        "SELECT employee_id, salary, RANK() OVER emp_win AS salary_rank FROM employees
  ;        WINDOW emp_win AS (PARTITION BY department_id ORDER BY salary DESC)"))
  ;  )

  (testing "select valueList"
    (is (assertRight?
          {:ql/type :pg/values
           :keys [:k1 :k2]
           :values [{:k1 1 :k2 [:pg/sql "'Alice'"]}
                    {:k1 2 :k2 [:pg/sql "'Grandma'"]}
                    {:k1 3 :k2 [:pg/sql "'Bob'"]}]}
          "(VALUES (1, 'Alice'), (2, 'Grandma'), (3, 'Bob'))"))

    (is (assertRight?
          {:select :*
           :from   {:ql/type :pg/values :keys [:k1 :k2] :values [{:k1 1 :k2 [:pg/sql "'Alice'"]}
                                                                 {:k1 2 :k2 [:pg/sql "'Bob'"]}]
                    :alias  {:t {:columns [:id :name]}}}}
          "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)"))
    )

  ;(testing "select limit_offset"                     not supported in dsql
  ;  (is (assertRight?
  ;        {}
  ;        "SELECT * FROM employees ORDER BY hire_date OFFSET 5")))

  ;(testing "select limit_option"                     not supported in dsql
  ;  (is (assertRight?
  ;        {}
  ;        "SELECT * FROM employees FETCH FIRST 5 ROWS ONLY")))

  (testing "select locking_clause"
    (is (assertRight?
          {:select :*
           :from :employees
           :for :update}
          "SELECT * FROM employees FOR UPDATE"))
    )

  (testing "select with_clause"
    (is (assertRight?
          {}
          "WITH recent_hires AS ( SELECT * FROM employees WHERE hire_date > CURRENT_DATE - INTERVAL '30 days')
          SELECT * FROM recent_hires"))
    )

  (testing "select set operation"
    (is (assertRight?
          {}
          "SELECT name FROM employees UNION SELECT name FROM contractors"))
    (is (assertRight?
          {}
          "SELECT name FROM employees INTERSECT SELECT name FROM contractors"))
    (is (assertRight?
          {}
          "SELECT name FROM employees EXCEPT SELECT name FROM contractors"))
    )

  (testing "select all"                                     ;; op retain duplicates
    (is (assertRight?
          {}
          "SELECT name FROM employees UNION ALL SELECT name FROM contractors"))
    )



  )
