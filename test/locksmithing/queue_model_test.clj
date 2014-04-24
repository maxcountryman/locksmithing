(ns locksmithing.queue-model-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [deftest is]]
            [knossos.core :refer [inconsistent linearizations op]]
            [knossos.util :refer [update]]
            [locksmithing.queue :refer [queue]])
  (:import [java.util.concurrent.atomic AtomicReference]
           [knossos.core Model]
           [locksmithing.queue Node]))

;; Utils
(defmacro dothreads  ;; Stolen from knossos.core-test
  "Calls body in each of t threads, blocking until completion. Binds i to the
  thread number."
  [[i t] & body]
  `(->> (range ~t)
        (map (fn [~i] (future ~@body)))
        doall
        (map deref)
        dorun))

;; Model
(defrecord QueueModel [queue]
  Model
  (step [_ op]
    (let [v (:value op)]
      (condp = (:f op)
        :push (QueueModel. (-> queue (conj v) vec))
        :pop  (if (= v (first queue))
                (QueueModel. (-> queue rest vec))
                (inconsistent
                  (str "pop expected " v " but read " (first queue))))))))

(defn system
  "Our system contains an instance of locksmithing.queue.Queue and a history
  vector, formatted as a map."
  []
  {:queue   (queue)
   :counter (atom 0)  ;; push value counter
   :history []})

;; Queue method "push" state transitions; points of linearization
(declare queue-push-start)
(declare queue-push-2)
(declare queue-push-3)
(declare queue-push-4)
(declare queue-push-complete)
(declare maybe-push-complete)

(defn push-state
  [id]
  (keyword (str "push-state-" (name id))))

(defn push-transition
  [id]
  (keyword (str "push-transition-" (name id))))

(defn queue-push-start
  "Given a system and optionally a value to push (otherwise grabs one from
  push-state), sets up the initial phase of the push method. This involves
  acquiring a head node and setting up a candidate replacement head containing
  the value to push and the previous head, as a tail value. These values are
  associated into the system as push-state.

  Sets queue-push-2 as the transition function. Updates the history vector of
  the system.

  Returns the system map."
  [sys id & [v]]
  (let [v    (or v (swap! (:counter sys) inc))
        head (-> sys :queue .head-ref .get)]
    (-> sys
        (assoc (push-state id)
               {:v     v
                :head  head
                :head' (Node. v (AtomicReference. nil) head)})
        (assoc (push-transition id) queue-push-2)
        (update (op (push-state id) :invoke :push v)))))

(defn queue-push-2
  "Given a system, models the second phase of the push method. This involves
  branching on the value of head (as defined in queue-push-start). When falsey
  (i.e. the first value pushed onto the queue) a CAS operation takes place over
  the head reference currently in the system and the head acquired in
  push-queue-start. If this operation fails, the transition is set to
  queue-push-start. Otherwise it succeeds and transition is set to
  queue-push-complete. When a truthy head is found a CAS operation takes place
  over the value of the prev-ref field of the head. Much like above if this
  fails transition is set to queue-push-start. Otherwise it succeeds and
  transition is set to queue-push-3.

  Sets the transition function to either queue-push-start, queue-push-3, or
  queue-push-complete.

  Returns the system map."
  [sys id]
  (let [state-key   (push-state id)
        v           (-> sys state-key :v)
        head        (-> sys state-key :head)
        head'       (-> sys state-key :head')
        transition  (atom #(queue-push-start %1 %2 v))]

    (if head
      ;; 1a. in locksmithing.queue
      (do (when (.compareAndSet (.prev-ref head) nil head')
            (reset! transition queue-push-3))

          (-> sys
              (assoc (push-transition id) @transition)
              (update (op state-key :info :queue-push-2 v))))

      ;; 1b. in locksmithing.queue
      (do (when (.compareAndSet (-> sys :queue .head-ref) head head')
            (reset! transition queue-push-complete))
          (.set (-> sys :queue .tail-ref) head')

          (-> sys
              (assoc (push-transition id) @transition)
              (update (op state-key :info :queue-push-2 v))
              (maybe-push-complete id))))))

(defn queue-push-3
  "Given a system, models the third (optional) phase of the push method. This
  involves completing the truthy branch of queue-push-2. A CAS operation takes
  place over the head reference currently in the system and the head acquired
  in queue-push-start. If this operation fails, the transition is set to
  queue-push-start. Otherwise it succeeds and transition is set to
  queue-push-complete.

  Sets the transition function to either queue-push-start or
  queue-push-complete.

  Returns the system map."
  [sys id]
  (let [state-key   (push-state id)
        v           (-> sys state-key :v)
        head        (-> sys state-key :head)
        head'       (-> sys state-key :head')
        transition  (atom #(queue-push-start %1 %2 v))]

    ;; 2. in locksmithing.queue
    (do (when (.compareAndSet (-> sys :queue .head-ref) head head')
          (reset! transition queue-push-complete))

        (-> sys
            (assoc (push-transition id) @transition)
            (update (op state-key :info :queue-push-3 v))
            (maybe-push-complete id)))))

(defn maybe-push-complete
  "Given a system, conditionally sets the op completion in the system history
  if the current transition function is queue-push-complete. Calling this
  directly after any potentially completing actions ensures that there is no
  gap between a valid completion and further system states.

  Returns the system map."
  [sys id]
  (let [transition (get sys (push-transition id))]
    (if (identical? transition queue-push-complete)
      (queue-push-complete sys id)
      sys)))

(defn queue-push-complete
  "Given a system, models a successful push.

  Returns the system map."
  [sys id]
  (let [state-key (push-state id)
        v         (-> sys state-key :v)]
    (-> sys
      (dissoc (push-transition id))  ;; Reset the push-transition fn
      (update (op state-key :ok :push v)))))

(defn queue-push
  "Given a system, nodels the push lock-free algorithm via a set of state
  transitions.

  Returns the system map."
  [sys id]
  (let [transition  (get sys (push-transition id))
        transition' (or transition queue-push-start)]
    (transition' sys id)))

;; Queue method "pop" state tranisitons; points of linearization
(declare queue-pop-start)
(declare queue-pop-2)
(declare queue-pop-3)
(declare queue-pop-complete)
(declare maybe-pop-complete)

(defn pop-state
  [id]
  (keyword (str "pop-state-" (name id))))

(defn pop-transition
  [id]
  (keyword (str "pop-transition-" (name id))))

(defn queue-pop-start
  "Given a system sets up the initial phase of the pop method. This involves
  first ensuring and acquiring a tail. Note that if no tail exists, there is
  nothing to pop and the system map is simply returned immediately. Otherwise
  the previous tail, head, and value are retrieved and associated into the
  system as pop-state.

  Sets queue-pop-2 as the transition function.

  Returns the system map."
  [sys id]
  (if-let [tail (-> sys :queue .tail-ref .get)]  ;; Cannot pop without tail
    (let [tail' (-> tail .prev-ref .get)
          head  (-> sys :queue .head-ref .get)
          v     (.data tail)]
      (-> sys
          (assoc (pop-state id) {:tail  tail
                                 :tail' tail'
                                 :head  head
                                 :v     v})
          (assoc (pop-transition id) queue-pop-2)
          (update (op (pop-state id) :invoke :pop v))))
    sys))

(defn queue-pop-2
  "Given a system, models the second phase of the pop method. This involves
  checking if the tail and head acquired in queue-pop-start are the same. When
  truthy a CAS takes place over the value of the head-ref currently in the
  system and head. If this succeeds the transition function is set to
  queue-pop-3. Otherwise queue-pop-start.

  Sets either queue-pop-start or queue-pop-3 as the transition function.

  Returns the system map."
  [sys id]
  (let [state-key  (pop-state id)
        v          (-> sys state-key :v)
        head       (-> sys state-key :head)
        tail       (-> sys state-key :tail)
        transition (atom queue-pop-3)]

    ;; 1. in locksmithing.queue
    (when (identical? tail head)
      (when-not (.compareAndSet (-> sys :queue .head-ref) head nil)
        (reset! transition queue-pop-start)))

    (-> sys
        (assoc (pop-transition id) @transition)
        (update (op state-key :info :queue-pop-2 v)))))

(defn queue-pop-3
  "Given a system, models the third phase of the pop method. This involves a
  CAS over the value of the tail-ref currently in the system and the tail
  acquired in queue-pop-start. If this succeeds, the transition function is set
  to queue-pop-complete. Otherwise queue-pop-start.

  Sets either queue-pop-start or queue-pop-complete as the transition function.

  Returns the system map."
  [sys id]
  (let [state-key  (pop-state id)
        v          (-> sys state-key :v)
        tail       (-> sys state-key :tail)
        tail'      (-> sys state-key :tail')
        transition (atom queue-pop-start)]

    ;; 2. in locksmithing.queue
    (when (.compareAndSet (-> sys :queue .tail-ref) tail tail')
      (reset! transition queue-pop-complete))

    (-> sys
        (assoc (pop-transition id) @transition)
        (update (op state-key :info :queue-pop-3 v))
        (maybe-pop-complete id))))

(defn maybe-pop-complete
  "Given a system, conditionally sets the op completion in the system history
  if the current transition function is queue-pop-complete. Calling this
  directly after any potentially completing actions ensures that there is no
  gap between a valid completion and further system states.

  Returns the system map."
  [sys id]
  (let [transition (get sys (pop-transition id))]
    (if (identical? transition queue-pop-complete)
      (queue-pop-complete sys id)
      sys)))

(defn queue-pop-complete
  "Given a system, models a successful push.

  Returns the system map."
  [sys id]
  (let [state-key (pop-state id)
        v         (-> sys state-key :v)]
    (-> sys
      (dissoc (pop-transition id))  ;; Reset the pop-transition fn
      (update (op state-key :ok :pop v)))))

(defn queue-pop
  "Given a system, nodels the pop lock-free algorithm via a set of state
  transitions.

  Returns the system map."
  [sys id]
  (let [transition  (get sys (pop-transition id))
        transition' (or transition queue-pop-start)]
    (transition' sys id)))

;; Operational models, e.g. concurrent push operations
(defn concurrent-push-step
  "System transitions related to applying concurrent push operations."
  [sys]
  (list #(queue-push sys :a)
        #(queue-push sys :b)
        #(queue-pop  sys :a)))

(defn concurrent-pop-step
  "System transitions related to applying concurrent pop operations."
  [sys]
  (list #(queue-push sys :a)
        #(queue-pop  sys :a)
        #(queue-pop  sys :b)))

(defn trajectory
  "Given a system and a depth, returns a randomized trajectory of the system up
  to the given depth."
  [sys step depth]
  (if (zero? depth)
    sys
    (recur ((rand-nth (step sys))) step (dec depth))))

;; Tests
(deftest concurrent-push-test
  (dothreads [_ 4]
    (dotimes [_ 1e4]
      (let [system  (trajectory (system) concurrent-push-step 30)
            history (:history system)
            model   (QueueModel. nil)
            linears (linearizations model history)]

        ;; Is this history linearizable?
        (is (not (empty? linears)))))))

(deftest concurrent-pop-test
  (dothreads [_ 4]
    (dotimes [_ 1e3]
      (let [system  (trajectory (system) concurrent-pop-step 30)
            history (:history system)
            model   (QueueModel. nil)
            linears (linearizations model history)]

        ;; Is this history linearizable?
        (is (not (empty? linears)))))))
