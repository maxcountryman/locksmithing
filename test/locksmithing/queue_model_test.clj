(ns locksmithing.queue-model-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [knossos.core :refer [linearizations op ->Register]]
            [locksmithing.queue :refer [queue]])
  (:import [locksmithing.queue Node]
           [java.util.concurrent.atomic AtomicReference]))

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

(defn update
  "Appends an operation to the history of a system."
  [sys op]
  (update-in sys [:history] conj op))

;; Model
(defn system
  "Our system contains an instance of locksmithing.queue.Queue and a history
  vector, formatted into a map."
  []
  {:queue   (queue)
   :history []})

;; Queue method "push" state transitions; points of linearization
(declare queue-push-start)
(declare queue-push-2)
(declare queue-push-3)
(declare queue-push-4)
(declare queue-push-complete)
(declare maybe-push-complete)

(def push-state (atom 0))

(defn queue-push-start
  "Given a system and optionally a value to push (otherwise grabs one from
  push-state), sets up the initial phase of the push method. This involves
  acquiring a head node and setting up a candidate replacement head containing
  the value to push and the previous head, as a tail value. These values are
  associated into the system as push-state.

  Sets queue-push-2 as the transition function. Updates the history vector of
  the system.

  Returns the system map."
  [sys & [v]]
  (let [v    (or v (swap! push-state inc))
        head (-> sys :queue .head-ref .get)]
    (-> sys
        (assoc :push-state
               {:v     v
                :head  head
                :head' (Node. v (AtomicReference. nil) head)})
        (assoc :push-transition queue-push-2)
        (update (op :push :invoke :write v)))))

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
  [sys]
  (let [v           (-> sys :push-state :v)
        head        (-> sys :push-state :head)
        head'       (-> sys :push-state :head')
        transition  (atom (constantly (queue-push-start sys v)))]

    (if head
      ;; 1a. in locksmithing.queue
      (do (when (.compareAndSet (.prev-ref head) nil head')
            (reset! transition queue-push-3))

          (-> sys
              (assoc :push-transition @transition)
              (update (op :push :info :queue-push-2 v))))

      ;; 1b. in locksmithing.queue
      (do (when (.compareAndSet (-> sys :queue .head-ref) head head')
            (reset! transition queue-push-complete))
          (.set (-> sys :queue .tail-ref) head')

          (-> sys
              (assoc :push-transition @transition)
              (update (op :push :info :queue-push-2 v))
              maybe-push-complete)))))

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
  [sys]
  (let [v           (-> sys :push-state :v)
        head        (-> sys :push-state :head)
        head'       (-> sys :push-state :head')
        transition  (atom (constantly (queue-push-start sys v)))]

    ;; 2. in locksmithing.queue
    (do (when (.compareAndSet (-> sys :queue .head-ref) head head')
          (reset! transition queue-push-complete))

        (-> sys
            (assoc :push-transition @transition)
            (update (op :push :info :queue-push-3 v))
            maybe-push-complete))))

(defn maybe-push-complete
  "Given a system, conditionally sets the op completion in the system history
  if the current transition function is queue-push-complete. Calling this
  directly after any potentially completing actions ensures that there is no
  gap between a valid completion and further system states.

  Returns the system map."
  [sys]
  (let [transition (:push-transition sys)]
    (if (identical? transition queue-push-complete)
      (queue-push-complete sys)
      sys)))

(defn queue-push-complete
  "Given a system, models a successful push.

  Returns the system map."
  [sys]
  (let [v (-> sys :push-state :v)]
    (-> sys
      (dissoc :push-transition)  ;; Reset the push-transition fn
      (update (op :push :ok :write v)))))

(defn queue-push
  "Given a system, nodels the push lock-free algorithm via a set of state
  transitions.

  Returns the system map."
  [sys]
  (let [transition (or (:push-transition sys) queue-push-start)]
    (transition sys)))


;; Queue method "pop" state tranisitons; points of linearization
(declare queue-pop-start)
(declare queue-pop-2)
(declare queue-pop-3)
(declare queue-pop-complete)
(declare maybe-pop-complete)


(defn queue-pop-start
  "Given a system sets up the initial phase of the pop method. This involves
  first ensuring and acquiring a tail. Note that if no tail exists, there is
  nothing to pop and the system map is simply returned immediately. Otherwise
  the previous tail, head, and value are retrieved and associated into the
  system as pop-state.

  Sets queue-pop-2 as the transition function.

  Returns the system map."
  [sys]
  (if-let [tail (-> sys :queue .tail-ref .get)]  ;; Cannot pop without tail
    (let [tail' (-> tail .prev-ref .get)
          head  (-> sys :queue .head-ref .get)
          v     (.data tail)]
      (-> sys
          (assoc :pop-state {:tail  tail
                             :tail' tail'
                             :head  head
                             :v     v})
          (assoc :pop-transition queue-pop-2)
          (update (op :pop :invoke :read v))))
    sys))

(defn queue-pop-2
  "Given a system, models the second phase of the pop method. This involves
  checking if the tail and head acquired in queue-pop-start are the same. When
  truthy a CAS takes place over the value of the head-ref currently in the
  system and head. If this succeeds the transition function is set to
  queue-pop-3. Otherwise queue-pop-start.

  Sets either queue-pop-start or queue-pop-3 as the transition function.

  Returns the system map."
  [sys]
  (let [v          (-> sys :pop-state :v)
        head       (-> sys :pop-state :head)
        tail       (-> sys :pop-state :tail)
        transition (atom (constantly (queue-pop-start sys)))]

    ;; 1. in locksmithing.queue
    (when (identical? tail head)
      (when (.compareAndSet (-> sys :queue .head-ref) head nil)
        (reset! transition queue-pop-3)))

    (-> sys
        (assoc :pop-transition @transition)
        (update (op :pop :info :queue-pop-2 v)))))

(defn queue-pop-3
  "Given a system, models the third phase of the pop method. This involves a
  CAS over the value of the tail-ref currently in the system and the tail
  acquired in queue-pop-start. If this succeeds, the transition function is set
  to queue-pop-complete. Otherwise queue-pop-start.

  Sets either queue-pop-start or queue-pop-complete as the transition function.

  Returns the system map."
  [sys]
  (let [v          (-> sys :pop-state :v)
        tail       (-> sys :pop-state :tail)
        tail'      (-> sys :pop-state :tail')
        transition (atom (constantly (queue-pop-start sys)))]

    ;; 2. in locksmithing.queue
    (when (.compareAndSet (-> sys :queue .tail-ref) tail tail')
      (reset! transition queue-pop-complete))

    (-> sys
        (assoc :pop-transition @transition)
        (update (op :pop :info :queue-pop-3 v)))))

(defn maybe-pop-complete
  "Given a system, conditionally sets the op completion in the system history
  if the current transition function is queue-pop-complete. Calling this
  directly after any potentially completing actions ensures that there is no
  gap between a valid completion and further system states.

  Returns the system map."
  [sys]
  (let [transition (:pop-transition sys)]
    (if (identical? transition queue-pop-complete)
      (queue-pop-complete sys)
      sys)))

(defn queue-pop-complete
  "Given a system, models a successful push.

  Returns the system map."
  [sys]
  (let [v (-> sys :pop-state :v)]
    (-> sys
      (dissoc :pop-transition)  ;; Reset the pop-transition fn
      (update (op :pop :ok :read v)))))

(defn queue-pop
  "Given a system, nodels the pop lock-free algorithm via a set of state
  transitions.

  Returns the system map."
  [sys]
  (let [transition (or (:pop-transition sys) queue-pop-start)]
    (transition sys)))


(defn step
  "All method results from a given system state."
  [sys]
  (list (queue-push sys)
        (queue-pop  sys)))

(defn trajectory
  "Returns a system from a randomized trajectory, `depth` steps away from the
  given system."
  [sys depth]
  (if (zero? depth)
    sys
    (let [possibilities (step sys)]
      (if (empty? possibilities)
        sys
        (recur (rand-nth possibilities)
               (dec depth))))))

(deftest queue-model-test
  (dothreads [_ 4]
    (dotimes [_ 1e4]
      (let [sys (trajectory (system) 15)]

        ;; Is this system linearizable?
        (let [history (:history sys)
              linears (linearizations (->Register nil) history)]
          (is (not (empty? linears))))))))
