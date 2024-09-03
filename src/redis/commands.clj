(ns redis.commands
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn command->keyword
  [cmd]
  (-> cmd
      (str/replace #" " "/")
      (str/lower-case)
      (keyword)))

(defn keyword->cmd
  [keyword]
  (-> keyword
      (str)
      (subs 1)
      (str/upper-case)
      (str/split #"/")
      (vec)))

(defn arg-spec
  [{:keys [type arguments]}]
  (let [;; treat the default case (i.e. the command-root map) as a block
        type (or type "block")]
    (case type
      "block" (into [:map] (map (fn [{:keys [name optional] :as arg}]
                                  [(keyword name) {:optional (boolean optional)} (arg-spec arg)]))
                    arguments)
      "oneof" (into [:or] (comp
                           (map #(assoc % :optional true))
                           (map #(do {:type "block" :arguments [%]}))
                           (map arg-spec))
                    arguments)
      "string" string?
      "integer" number?
      "double" float?
      "key" string?
      "pattern" [:fn #(instance? % java.util.regex.Pattern)]
      "unix-time" number?
      "pure-token" boolean?
      nil)))

;; https://raw.githubusercontent.com/redis/redis-doc/master/commands.json
(def commands
  (-> "commands.json"
      (io/resource)
      (io/reader)
      (json/read :key-fn command->keyword)
      (update-vals (fn [m] (assoc m :arg-spec (arg-spec m))))))

;; string: a string argument.
;; integer: an integer argument.
;; double: a double-precision argument.
;; key: a string that represents the name of a key.
;; pattern: a string that represents a glob-like pattern.
;; unix-time: an integer that represents a Unix timestamp.
;; pure-token: an argument is a token, meaning a reserved keyword, which may or may not be provided. Not to be confused with free-text user input.
;; oneof: the argument is a container for nested arguments. This type enables choice among several nested arguments (see the XADD example below).
;; block: the argument is a container for nested arguments. This type enables grouping arguments and applying a property (such as optional) to all (see the XADD example below).

(def simple-types
  #{"string" "integer" "double" "key" "pattern" "unix-time"})

(defn value
  [{:keys [name optional] :as arg} request]
  (when request
    (let [v (request (keyword name))]
      (when (and (not optional) (nil? v))
        (throw (ex-info "missing required argument" arg)))
      v)))

(defn expand-arg
  [{:keys [type token arguments] :as arg} request]
  (when-let [val (value arg request)]
    (cond-> []
      (and token (not (= "pure-token" type)))
      (conj token)

      (simple-types type)
      (conj val)

      (= "block" type)
      (concat (mapcat #(expand-arg % val) arguments))

      (and (= "pure-token" type) (true? val))
      (conj token)

      (and (= "oneof" type) (not= 1 (count val)))
      (do (throw (ex-info "too many values given for oneof field" {:arg arg :val val})))

      (= "oneof" type)
      (concat (remove nil? (mapcat #(expand-arg (assoc % :optional true) val) arguments))))))

(defn build
  [{:keys [op request]}]
  (->> op
       (commands)
       (:arguments)
       (into (keyword->cmd op) (mapcat #(expand-arg % request)))))

(comment
  (def xadd {:op :xadd
             :request
             {:key "key"
              :nomkstream true
              :trim
              {:strategy "MAXLEN"
               :operator "~"
               :threshold "foo"
               :count 1}
              :id-selector "*"
              :data
              [{:field "foo"
                :value "bar"}]}})

  (def hello
    {:op :hello
     :request
     {:arguments
      {:protover 1
       :clientname "foo"
       :auth
       {:username "username"
        :password "password"}}}})

  nil)
