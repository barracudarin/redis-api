(ns redis.api
  (:require [redis.client.api :as client]
            [redis.commands :as cmd]))

(defn client
  [opts]
  (client/client opts))

(defn close!
  [{:keys [conn] :as _client}]
  (.close conn))

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
  (def c (client {:host "localhost"
                  :port 6379}))
  (doc {} :hello)
  (invoke c {:op :hello
             :request
             {:arguments
              {:protover (str 3)}}})
  (invoke c {:op :hello})
  (invoke c
          {:op :get
           :request
           {:key "bax"}}
          {:op :set
           :request
           {:key "bax"
            :value (str (rand-int 10))}}
          {:op :get
           :request
           {:key "bax"}})

  (close! c)
  nil)
