(ns redis.client
  (:require [aleph.tcp :as tcp]
            [clojure.string :as str]
            [gloss.io :as io]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [redis.commands :as c]
            [redis.protocol :as p]))

(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect (s/map #(io/encode protocol %) out) s)
    (s/splice out (io/decode-stream s protocol))))

(defn client
  [{:keys [host port protocol]}]
  (d/chain (tcp/client {:host host, :port port}) #(wrap-duplex-stream p/codec %)))

(defn simple-string
  [_client value]
  value)

(defn bulk-string
  [client value]
  (let [length (parse-long value)]
    (when (pos? length)
      @(s/take! client))))

(defn error
  [_client value]
  (let [[prefix message] (str/split value #"\s" 2)]
    (throw (ex-info message {:prefix (str/lower-case prefix)}))))

(declare receive!)

(defn map-response
  [client value]
  (->> value
       (parse-long)
       (range)
       (reduce (fn [acc _]
                 (assoc acc (keyword (receive! client)) (receive! client)))
               {})))

(defn list-response
  [client value]
  (->> value
       (parse-long)
       (range)
       (mapv (fn [_] (receive! client)))))

(defn number
  [_client value]
  (parse-long value))

(def dispatch-table
  {\+ simple-string
   \$ bulk-string
   \% map-response
   \* list-response
   \- error
   \: number})

(defn send!
  [client ops]
  @(s/put! client (str/join (map (comp p/-serialize c/build) ops))))

(defn receive!
  [client]
  (when-let [line @(s/take! client)]
    (let [f (dispatch-table (first line))]
      (prn line)
      (if-not f
        (throw (ex-info "unknown dispatch byte" {:response line}))
        (f client (str/join (rest line)))))))
