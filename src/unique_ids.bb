#!/usr/bin/env bb
(ns echo
  (:require
   [common.core :refer :all]))

(def node-id (atom ""))
(def next-message-id (atom 0))

(defn process-request
  [input]
  (let [body (:body input)
        r-body {:msg_id (swap! next-message-id inc)
                :in_reply_to (:msg_id body)}]
    (case (:type body)
      "init"
      (do
        (reset! node-id (:node_id body))
        (reply @node-id
               (:src input)
               (assoc r-body :type "init_ok")))
      "generate"
      (reply @node-id
             (:src input)
             (assoc r-body
                    :type "generate_ok"
                    :id (str @node-id "/" @next-message-id))))))


(defn -main
  "Read transactions from stdin and send output to stdout"
  [& args]
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))


(-main)
