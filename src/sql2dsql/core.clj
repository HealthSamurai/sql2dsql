(ns sql2dsql.core
  (:require [cheshire.core :as json]
            [org.httpkit.server :as httpkit]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [sql2dsql.transpiler :refer [->dsql]])
  (:gen-class))

(def ^String lib-path_ nil)

(defroutes app-routes
           (GET "/" []
             {:status 200
              :headers {"Content-Type" "text/html"}
              :body (slurp "resources/public/index.html")})

           (POST "/to-dsql" request
             (try
                 {:status 200 :body (->dsql lib-path_ (-> request :body slurp (json/parse-string true)))}
               (catch Exception e
                 {:status 400 :body (str "Error: " (.getMessage e))})))

           (route/resources "/")

           (route/not-found (fn [request]
                                {:status 404
                                 :body (str "Error: Route not found - " (:uri request))})))


(def app
  (wrap-defaults app-routes
                 (assoc-in site-defaults [:security :anti-forgery] false)))

(defn -main [& args]
  (do
    (println "Starting server on port 3000...")
    (if (empty? args)
      (httpkit/run-server app {:port 3000 :join? false})
      (let [lib-path (first args)]
        (println "Using library path:" lib-path)
        (alter-var-root #'lib-path_ (constantly lib-path))
        (httpkit/run-server app {:port 3000 :join? false})))))
