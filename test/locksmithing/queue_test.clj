(ns locksmithing.queue-test
  (:refer-clojure :exclude [pop!])
  (:require [clojure.test :refer [deftest is]]
            [locksmithing.queue :refer [queue pop! push!]]))

(deftest concurrent-enqueues
  (let [q (queue)]
    (->> (range 10000) (pmap #(push! q %)) dorun)
    (let [values (->> (repeatedly #(pop! q))
                      (take-while (complement nil?)))]
      (is (= (sort values) (range 10000))))))
