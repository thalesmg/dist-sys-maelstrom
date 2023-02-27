#!/usr/bin/env bb
(ns broadcast
  (:require
   [common.core :refer :all]))

(def node-id (atom ""))
(def next-message-id (atom 0))
(def seen (atom []))
(def topology (atom {}))

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
      "broadcast"
      (do
        (swap! seen conj (:message body))
        (reply @node-id
               (:src input)
               (assoc r-body
                      :type "broadcast_ok")))
      "read"
      (reply @node-id
             (:src input)
             (assoc r-body
                    :type "read_ok"
                    :messages @seen))
      "topology"
      (do
        (reset! topology (:topology body))
        (reply @node-id
               (:src input)
               (assoc r-body
                      :type "topology_ok"))))))


(defn -main
  "Read transactions from stdin and send output to stdout"
  [& args]
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))


(-main)
