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
      true
      (let []
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
          {:select [:distinct :test]
           :from :best}
          "SELECT DISTINCT( test ) FROM best"))

    (is (assertRight?
          {:select
                 ^{:pg/projection :distinct}
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
                    ^{:pg/projection {:distinct-on [[:#>> :resource [:id]]]}}
                    {:id       :id
                     :resource :resource
                     :txid     :txid}
           :from :best}
          "SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"))
    )

  (testing "select ALL"
    (is (assertRight?
          {:select
                 ^{:pg/projection :all}
                 {:id       :id
                  :resource :resource
                  :txid     :txid}
           :from :best}
          "SELECT ALL id as id , resource as resource , txid as txid FROM best"))

    )
  )
