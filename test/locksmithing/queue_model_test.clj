(ns locksmithing.queue-model-test
  (:require [clojure.test :refer [deftest is]]
            [knossos.core :refer [inconsistent linearizations op]]
            [knossos.history :refer [complete]]
            [knossos.util :refer [update]]
            [locksmithing.queue :as q])
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
  {:queue   (q/queue)
   :counter (atom 0)  ;; push value counter
   :history []})

;; Queue method "push" state transitions; points of linearization
(declare queue-push-start)
(declare queue-push-2)
(declare queue-push-3)
(declare queue-push-complete)

(defn push-state
  "Given an id, returns the id appended to the string 'push-state-' and cast
  as a keyword."
  [id]
  (keyword (str "push-state-" (name id))))

(defn push-transition
  "Given an id, returns the id appended to the string 'push-transition-' and
  cast as a keyword."
  [id]
  (keyword (str "push-transition-" (name id))))

(defn queue-push-start
  "Given a system map sys, an id, and optionally a Node n, sets up the initial
  phase of a push operation.
  
  This models locksmithing.queue/push! by setting up the state that will be
  used throughout the state transitions. Once set up, the state is contained
  in a map and added to the system map.
  
  If a node is provided, this call is treated like a recur invocation and the
  node currently contained in the state map is used again. However, the
  remaining state is rebound.
  
  Always sets the state transition to queue-push-2.
  
  Returns a system map."
  [sys id & [n]]
  (let [loop? (not (nil? n))
        n     (or n (q/node (swap! (:counter sys) inc) nil))
        t     (-> sys :queue q/get-tail)
        t'    @t
        s     @(q/get-next @t)]
    (-> sys
        (assoc (push-state id) {:n n :t t :t' t' :s s})
        (assoc (push-transition id) queue-push-2)
        (update (op (push-state id)
                    (if loop? :info :invoke)
                    :push
                    (q/get-value n))))))

(defn queue-push-2
  "Given a system map sys and an id, models a linearizability point of
  locksmithing.queue/push!.
  
  If the bound value of tail, in our saved state, is consistent with the value
  in the actual queue, we advance. When not, the transition will be
  queue-push-start, i.e. we recur.
  
  When successor node is nil, an invocation of cas-next is made. A truthy
  value indicates a successful push and the transition is set to
  queue-push-complete. Otherwise we set the transition to queue-push-3.
  
  Sets the state transition to queue-push-start, queue-push-3, or
  queue-push-complete.
  
  Returns a system map."
  [sys id]
  (let [state-key  (push-state id)
        n          (-> sys state-key :n)
        t          (-> sys state-key :t)
        t'         (-> sys state-key :t')
        s          (-> sys state-key :s)
        transition (atom #(queue-push-start %1 %2 n))]

    (when (identical? t' (-> sys :queue q/get-tail deref))
      (if (nil? s)
        (when (q/cas-next t s n)
          (reset! transition queue-push-complete))
        (reset! transition queue-push-3)))

    (-> sys
        (assoc (push-transition id) @transition)
        (update (op state-key :info :queue-push-2 (q/get-value n))))))

(defn queue-push-3
  "Given a system map sys and an id, models a linearizability point of
  locksmithing.queue/push!.
  
  This is the resulting branch of a cas-next failure. Here cas-tail is invoked
  using the stored state, over t' and s.
  
  The transition is always set to queue-push-start.
  
  Returns a system map."
  [sys id]
  (let [state-key   (push-state id)
        n           (-> sys state-key :n)
        t'          (-> sys state-key :t')
        s           (-> sys state-key :s)
        transition  (atom #(queue-push-start %1 %2 n))]

    (q/cas-tail (:queue sys) t' s)

    (update sys (op state-key :info :queue-push-3 (q/get-value n)))))

(defn queue-push-complete
  "Given a system map sys and an id, completes a push with the current stored
  state. Models a linearizability point of locksmithing.queue/push!.
  
  Before resetting the push-transition to nil, we invoke cas-tail over the
  queue in the system, t', and n, in an attempt to advance the tail.
  
  The transition is always set to nil.
  
  Returns a system map."
  [sys id]
  (let [state-key (push-state id)
        n         (-> sys state-key :n)
        t'        (-> sys state-key :t')]

    (q/cas-tail (:queue sys) t' n)

    (-> sys
        (dissoc (push-transition id))  ;; Reset the push-transition fn
        (update (op state-key :ok :push (q/get-value n))))))

(defn queue-push
  "Given a system map sys and an id, handles state transitions in the push
  operation state machine.
  
  Returns a system map."
  [sys id]
  (let [transition  (get sys (push-transition id))
        transition' (or transition queue-push-start)]
    (transition' sys id)))

;; Queue method "pop" state tranisitons; points of linearization
(declare queue-pop-start)
(declare queue-pop-2)
(declare queue-pop-3)
(declare queue-pop-complete)

(defn pop-state
  "Given an id, returns the id appended to the string 'pop-state-' and cast
  as a keyword."
  [id]
  (keyword (str "pop-state-" (name id))))

(defn pop-transition
  "Given an id, returns the id appended to the string 'pop-transition-' and
  cast as a keyword."
  [id]
  (keyword (str "pop-transition-" (name id))))

(defn queue-pop-start
  "Given a system map sys, an id, and optionally a boolean loop?, sets up the
  initial phase of a pop operation.
  
  This models locksmithing.queue/pop! by setting up the state that will be
  used throughout the state transitions. Once set up, the state is contained
  in a map and added to the system map.
  
  If loop? is truthy, this call is treated like a recur invocation. State is
  once again rebound.
  
  Always sets the state transition to queue-pop-2.
  
  Returns a system map."
  [sys id & [loop?]]
  (let [h  (-> sys :queue q/get-head)
        h' @h
        t  @(-> sys :queue q/get-tail)
        f  (q/get-next @h)
        f' @f]

    (-> sys
        (assoc (pop-state id) {:h h :h' h' :t t :f f :f' f'})
        (assoc (pop-transition id) queue-pop-2)
        (update (op (pop-state id)
                    (if loop? :info :invoke)
                    :pop
                    nil)))))

(defn queue-pop-2
  "Given a sytem map sys and a id, models a linearizability point of
  locksmithing.queue/pop!.
  
  If the bound value of head, in our saved state, is consistent with the value
  in the actual queue, we advance. When not, the transition will be
  queue-pop-start, i.e. we recur.
  
  We then ask if the bound head and tail are consistently. If truthy we ask if
  the saved first node is nil. If it is, the queue must be empty and we set
  the transition to queue-pop-complete. If it's not, then we cas-tail over the
  queue, the bound tail and first node, i.e. we attempt to advance the lagging
  tail.
  
  When the head and tail are inconsistent, we ask if cas-head over the queue,
  the bound head and first node is true. This means we have a value to pop,
  assuming the first node contains non-nil. As long as this is true, we set
  the transition to queue-pop-3.
  
  Sets the transiton to queue-pop-start, queue-pop-3, or queue-pop-complete.
  
  Returns a system map."
  [sys id]
  (let [state-key  (pop-state id)
        h'         (-> sys state-key :h')
        t          (-> sys state-key :t)
        f'         (-> sys state-key :f')
        v          (atom nil)
        transition (atom #(queue-pop-start %1 %2 true))]

    (when (identical? h' @(q/get-head (:queue sys)))
      (if (identical? h' t)
        (if (nil? f')
          (reset! transition queue-pop-complete)
          (q/cas-tail (:queue sys) t f'))
        (when (q/cas-head (:queue sys) h' f')
          (when-let [v' (q/get-value f')]
            (reset! v v')
            (reset! transition queue-pop-3)))))

    (-> sys
        (assoc (pop-transition id) @transition)
        (assoc-in [state-key :v] @v)
        (update (op state-key :info :queue-pop-2 @v)))))

(defn queue-pop-3
  "Given a sytem map sys and a id, models a linearizability point of
  locksmithing.queue/pop!.
  
  We unconditionally set the value of the first node to nil, effectively
  deleting it.
  
  Always sets the transition to queue-pop-complete.
  
  Returns a system map."
  [sys id]
  (let [state-key  (pop-state id)
        f          (-> sys state-key :f)
        v          (-> sys state-key :v)
        transition (atom queue-pop-complete)]

    (q/set-value f nil)

    (-> sys
        (assoc (pop-transition id) @transition)
        (update (op state-key :info :queue-pop-3 v)))))

(defn queue-pop-complete
  "Given a system map sys and an id, completes a pop with the current stored
  state.
  
  The transition is always set to nil.
  
  Returns a system map."
  [sys id]
  (let [state-key (pop-state id)
        v         (-> sys state-key :v)]
    (-> sys
        (dissoc (pop-transition id))  ;; Reset the pop-transition fn
        (update (op state-key :ok :pop v)))))

(defn queue-pop
  "Given a system map sys and an id, handles state transitions in the pop
  operation state machine.
  
  Returns a system map."
  [sys id]
  (let [transition  (get sys (pop-transition id))
        transition' (or transition queue-pop-start)]
    (transition' sys id)))

;; Operational models, e.g. concurrent push operations
(defn concurrent-push-steps
  "System transitions related to applying concurrent push operations."
  [sys]
  (list #(queue-push sys :a)
        #(queue-push sys :b)
        #(queue-pop  sys :a)))

(defn concurrent-push-pop-steps
  "System transitions related to applying concurrent push/pop operations."
  [sys]
  (list #(queue-push sys :a)
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
    (dotimes [_ 100]
      (let [system  (trajectory (system) concurrent-push-steps 50)
            history (-> system :history complete)
            model   (QueueModel. nil)
            linears (linearizations model history)]

        ;; Is this history linearizable?
        (is (not (empty? linears)))))))

(deftest concurrent-push-pop-test
  (dothreads [_ 4]
    (dotimes [_ 100]
      (let [system  (trajectory (system) concurrent-push-pop-steps 50)
            history (-> system :history complete)
            model   (QueueModel. nil)
            linears (linearizations model history)]

        ;; Is this history linearizable?
        (is (not (empty? linears)))))))
