(ns sql2dsql.core
  (:require
   [cheshire.core :as json]
   [clojure.pprint :as p]
   [clojure.string :as s]
   [compojure.core :refer [defroutes GET POST]]
   [compojure.route :as route]
   [org.httpkit.server :as httpkit]
   [ring.middleware.defaults :refer [site-defaults wrap-defaults]]
   [sql2dsql.transpiler :as transpiler])
  (:import
   (sql2dsql.pgquery PgQueryLibInterface))
  (:gen-class))

(def port (or (some-> (System/getenv "PORT") parse-long) 3000))
(def lib-path (or (System/getenv "PGQUERY_PATH") "libpg_query.dylib"))
(def ->dsql (transpiler/make (PgQueryLibInterface/load lib-path)))

(defn parse [stmt params]
  (try
    (binding [*print-meta* true]
      (s/join "" (map #(with-out-str (p/pprint %)) (->dsql stmt params))))
    (catch Exception e
      (str "Error processing statement: " stmt " with params: " params " Error: " (.getMessage e)))))

(defroutes app
  (GET "/" []
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (slurp "resources/public/index.html")})

  (POST "/to-dsql" request
    (try
      (let [body (-> request :body slurp (json/parse-string true))]
        (if-let [sql (:sql body)]
          {:status 200
           :headers {"Content-Type" "text/plain"}
           :body (parse sql (:params body))}
          {:status 200
           :headers {"Content-Type" "text/plain"}
           :body ""}))
      (catch Exception e
        {:status 409
         :headers {"Content-Type" "text/plain"}
         :body (.getMessage e)})))

  (route/resources "/")

  (route/not-found (fn [request]
                     {:status 404
                      :headers {"Content-Type" "application/json"}
                      :body (json/generate-string
                             {:error (str "Route not found - " (:uri request))})})))

(defn start-server []
  (println (str "Server will start on port " port))
  (println "Using library path:" lib-path)
  (try
    (httpkit/run-server
     (wrap-defaults app
                    (assoc-in site-defaults [:security :anti-forgery] false))
     {:port port :join? false})
    (catch Exception e
      (println "Failed to initialize parser:" (.getMessage e))
      )))

(defn -main [& _] (start-server))

(comment

  (def stop (start-server))
  (stop)

)