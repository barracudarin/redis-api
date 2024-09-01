(ns redis.client.protocol.shared
  (:require [clojure.string :as str]
            [redis.client.codec :as codec]))

(defn read-simple-string
  [_client value]
  value)

(defn read-bulk-string
  [client value]
  (let [length (parse-long value)]
    (when (pos? length)
      (codec/read-frame! client))))

(defn write-bulk-string
  [s]
  (format "$%s\r\n%s\r\n" (count s) s))

(defn read-error
  [_client value]
  (let [[prefix message] (str/split value #"\s" 2)]
    (throw (ex-info message {:prefix (str/lower-case prefix)}))))

(defn read-list
  [client value]
  (->> value
       (parse-long)
       (range)
       (mapv (fn [_] (codec/load-frame! client)))))

(defn read-number
  [_client value]
  (parse-long value))

(defn write-keyword
  [f kw]
  (f (subs (str kw) 1)))

(defn write-seqable
  [prefix f seq]
  (format "%s%s\r\n%s" prefix (count seq) (str/join (map f seq))))

(defn write-number
  [n]
  (format ":%s\r\n" (pr-str n)))

(defn decode-fn
  [dispatch-table]
  (fn [client frame]
    (prn frame)
    (if-let [f (dispatch-table (first frame))]
      (f client (subs frame 1))
      (throw (ex-info "unknown dispatch byte" {:response frame})))))
