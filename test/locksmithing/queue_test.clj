(ns locksmithing.queue-test
  (:refer-clojure :exclude [pop])
  (:require [clojure.test :refer [deftest is]]
            [locksmithing.queue :refer [queue pop push]]))

(deftest concurrent-enqueues
  (let [q (queue)]
    (->> (range 1e4)
         (pmap (partial push q))
          dorun)
    (let [elements (->> (repeatedly #(pop q))
                        (take-while (complement nil?)))]
      (is (= (sort elements) (range 1e4))))))
