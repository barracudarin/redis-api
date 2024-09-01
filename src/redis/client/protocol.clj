(ns redis.client.protocol
  (:require [clojure.string :as str]
            [gloss.core :as gloss]
            [redis.client.codec :as codec])
  (:import [clojure.lang Keyword PersistentHashSet]))

(defprotocol RespVersion2
  (-write-v2 [frame]))

(defprotocol RespVersion3
  (-write-v3 [frame]))

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

(defn write-bool
  [b]
  (format "#%s\r\n" (if b "t" "f")))

;; TODO: there has to be a way to make write-sequable work
(defn write-map
  [f m]
  (let [entries (mapcat identity m)]
    (format "%%%s\r\n%s" (count m) (str/join (map f entries)))))

(defn write-nil
  [_]
  "_\r\n")

(extend String         RespVersion2 {:-write-v2 write-bulk-string})
(extend Number         RespVersion2 {:-write-v2 write-number})
(extend Keyword        RespVersion2 {:-write-v2 (partial write-keyword -write-v2)})
(extend java.util.List RespVersion2 {:-write-v2 (partial write-seqable "*" -write-v2)})

(extend String            RespVersion3 {:-write-v3 write-bulk-string})
(extend Number            RespVersion3 {:-write-v3 write-number})
(extend Boolean           RespVersion3 {:-write-v3 write-bool})
(extend nil               RespVersion3 {:-write-v3 write-nil})
(extend java.util.Map     RespVersion3 {:-write-v3 (partial write-map -write-v3)})
(extend Keyword           RespVersion3 {:-write-v3 (partial write-keyword -write-v3)})
(extend PersistentHashSet RespVersion3 {:-write-v3 (partial write-seqable "~" -write-v3)})
(extend java.util.List    RespVersion3 {:-write-v3 (partial write-seqable "*" -write-v3)})

(def versions
  (let [v2-readers {\+ read-simple-string
                    \- read-error
                    \: read-number
                    \$ read-bulk-string
                    \* read-list}
        v3-readers (merge v2-readers
                          {\% read-map})
        -from (fn [m client frame]
                (if-let [f (m (first frame))]
                  (f client (subs frame 1))
                  (throw (ex-info "unknown dispatch byte" {:response frame}))))]
    {2 {:version 2
        :encode -write-v2
        :decode (fn [client frame] (-from v2-readers client frame))}
     3 {:version 3
        :encode -write-v3
        :decode (fn [client frame] (-from v3-readers client frame))}}))


(comment
  (require '[clojure.edn :as edn])
  (require '[dev.utils :as utils])
  (require '[manifold.stream :as s])

  (def port 12323)

  (defn handler
    [f]
    (fn [s _info]
      (s/connect (s/map f s) s)))

  (defn test-handler
    [input]
    (prn input)
    input)

  (def protocol
    (gloss/compile-frame
     (gloss/finite-frame :uint32 (gloss/string :utf-8))
     pr-str
     edn/read-string))

  (def server (utils/start-server (gloss/string :ascii :delimiters ["\r\n"]) (handler test-handler) port))
  (def client @(utils/client resp2 "localhost" 6379))

  @(s/put! client "+OK\r\n")
  @(s/take! client)

  (.close server)

  nil)
