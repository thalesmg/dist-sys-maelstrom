#!/usr/bin/env bb
(ns broadcast
  (:require
   [common.core :refer :all]))

(def node-id (atom ""))
(def message-id (atom 0))
(def seen (atom #{}))
(def topology (atom {}))
(def pending
  "{broadcast-id {:msg msg
                  :to #{node-id}}}"
  (atom {}))

(defn next-message-id
  []
  (swap! message-id inc))

(defn retry-pending-loop
  []
  (future
   (loop []
     (doseq [[[broadcast-id neighbor] broadcast-msg] @pending]
       (send! (assoc-in broadcast-msg
                        [:body :msg_id] (next-message-id))))
     (Thread/sleep 1000)
     (recur))))

(defn process-request
  [input]
  (let [body (:body input)
        src (:src input)
        r-body {:msg_id (next-message-id)
                :in_reply_to (:msg_id body)}]
    (printerr {:input input})
    (case (:type body)
      "init"
      (do
        (reset! node-id (:node_id body))
        (reply @node-id
               src
               (assoc r-body :type "init_ok")))

      "broadcast"
      (let [broadcast-id (or (:broadcast_id body)
                             [@node-id (next-message-id)])
            gossip? (:broadcast_id body)
            neighbors (set (get @topology (keyword @node-id) []))
            message (:message body)
            new? (not (@seen message))]
        (when new?
          (swap! seen conj message)
          (doseq [neighbor neighbors
                  :when (not= neighbor src)]
            (let [broadcast-msg (reply @node-id
                                       neighbor
                                       (assoc body
                                              :broadcast_id broadcast-id))]
              (swap! pending assoc [broadcast-id neighbor] broadcast-msg)
              (send! broadcast-msg))))
        (if gossip?
          (reply @node-id
                 src
                 (assoc r-body
                        :type "broadcast_ok"
                        :broadcast_id broadcast-id))
          (reply @node-id
                 src
                 (assoc r-body
                        :type "broadcast_ok"))))

      "broadcast_ok"
      (let [broadcast-id (:broadcast_id body)]
        (swap! pending dissoc [broadcast-id src])
        [])

      "read"
      (reply @node-id
             src
             (assoc r-body
                    :type "read_ok"
                    :messages @seen))

      "topology"
      (do
        (reset! topology (:topology body))
        (reply @node-id
               src
               (assoc r-body
                      :type "topology_ok"))))))


(defn -main
  "Read transactions from stdin and send output to stdout"
  [& args]
  (retry-pending-loop)
  (process-stdin #(-> %
                      ;; (inspect :in-raw)
                      parse-json
                      ;; (inspect :in)
                      process-request
                      ;; (inspect :out-rep)
                      generate-json
                      printout)))


(-main)
