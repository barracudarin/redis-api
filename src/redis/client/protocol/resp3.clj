(ns redis.client.protocol.resp3
  (:require [clojure.string :as str]
            [redis.client.codec :as codec]
            [redis.client.protocol.resp2 :as resp2]
            [redis.client.protocol.shared :as shared])
  (:import [clojure.lang BigInt Keyword PersistentHashSet]))

(defprotocol RespVersion3
  (-write [frame]))

(defn read-map
  [client value]
  (->> value
       (parse-long)
       (range)
       (reduce (fn [acc _]
                 (let [k (keyword (codec/load-frame! client))
                       v (codec/load-frame! client)]
                   (assoc acc k v)))
               {})))

(defn read-bool
  [_client value]
  (= "t" value))

(defn read-bigint
  [_client value]
  (bigint value))

(defn read-set
  [client value]
  (set (shared/read-list client value)))

(defn read-bulk-error
  [client value]
  (shared/read-error client (shared/read-bulk-string client value)))

(defn read-verbatim-string
  [client value]
  (let [[_encoding data] (str/split (shared/read-bulk-string client value) #":" 2)]
    data))

(defn read-push
  [client value]
  ;; TODO:
  ;;
  ;; Pushed data may precede or follow any of RESP's data types but never inside them.
  ;; That means a client won't find push data in the middle of a map reply, for example.
  ;; It also means that pushed data may appear before or after a command's reply, as well as
  ;; by itself (without calling any command).
  ;;
  ;; Clients should react to pushes by invoking a callback that implements their handling of the pushed data.
  (shared/read-list client value))

(defn read-double
  [_client value]
  (case value
    "inf"  Double/POSITIVE_INFINITY
    "-inf" Double/NEGATIVE_INFINITY
    "nan"  Double/NaN
    (Double/valueOf value)))

(defn write-bigint
  [i]
  (format "(%s\r\n" (str i)))

(defn write-bool
  [b]
  (format "#%s\r\n" (if b "t" "f")))

;; TODO: there has to be a way to make write-sequable work
(defn write-map
  [m]
  (let [entries (mapcat identity m)]
    (format "%%%s\r\n%s" (count m) (str/join (map -write entries)))))

(defn write-nil
  [_]
  "_\r\n")

(extend String                  RespVersion3 {:-write shared/write-bulk-string})
(extend java.util.regex.Pattern RespVersion3 {:-write (comp shared/write-bulk-string str)})
(extend Number                  RespVersion3 {:-write shared/write-number})
(extend BigInt                  RespVersion3 {:-write write-bigint})
(extend BigInteger              RespVersion3 {:-write write-bigint})
(extend Boolean                 RespVersion3 {:-write write-bool})
(extend nil                     RespVersion3 {:-write write-nil})
(extend java.util.Map           RespVersion3 {:-write write-map})
(extend Keyword                 RespVersion3 {:-write (partial shared/write-keyword -write)})
(extend PersistentHashSet       RespVersion3 {:-write (partial shared/write-seqable "~" -write)})
(extend java.util.List          RespVersion3 {:-write (partial shared/write-seqable "*" -write)})

(def readers
  (merge resp2/readers
         {\$ shared/read-bulk-string
          \* shared/read-list
          \% read-map
          \_ (fn [& _args] nil)
          \# read-bool
          \( read-bigint
          \, read-double
          \! read-bulk-error
          \= read-verbatim-string
          \~ read-set
          \> read-push}))

(def resp3
  {:version 3
   :encode -write
   :decode (shared/decode-fn readers)})
