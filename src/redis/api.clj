(ns redis.api
  (:require [redis.client :as client]
            [redis.commands :as cmd]))

(defn client
  [opts]
  @(client/client opts))

(defn ops
  [_client]
  (sort (keys cmd/commands)))

(defn doc
  [_client op]
  (cmd/commands op))

(defn invoke
  [client & [:as ops]]
  (let [num-ops (count ops)]
    (when (client/send! client ops)
      (cond->> num-ops
        true (range)
        true (mapv (fn [_] (client/receive! client)))
        (= 1 num-ops) (first)))))

(comment
  (with-open [c (client {:host "localhost"
                         :port 6379})]
    (invoke c
            {:op :get
             :request
             {:key "bax"}}
            {:op :set
             :request
             {:key "bax"
              :value "hello"}}
            {:op :get
             :request
             {:key "bax"}}))
  nil)
