(ns common.core
  (:require
    [cheshire.core :as json]))

;;;;;;;;;;;;;;;;;;; Util functions ;;;;;;;;;;;;;;;;;;;

;;;;;; Input pre-processing functions ;;;;;;

(defn process-stdin
  "Read lines from the stdin and calls the handler"
  [handler]
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (handler line)))

(defn parse-json
  "Parse the received input as json"
  [input]
  (try
    (json/parse-string input true)
    (catch Exception e
      nil)))

;;;;;; Output Generating functions ;;;;;;

(defn generate-json
  "Generate json string from input"
  [input]
  (if (map? input)
    [(json/generate-string input)]
    (mapv json/generate-string input)))

(defn printerr
  "Print the received input to stderr"
  [& input]
  (binding [*out* *err*]
    (apply prn input)))

(defn printout
  "Print the received input to stdout"
  [input]
  (doseq [i input]
    (println i)))

(defn reply
  ([src dest body]
   {:src src
    :dest dest
    :body body}))
