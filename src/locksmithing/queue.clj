(ns locksmithing.queue
  (:refer-clojure :exclude [compare-and-set! pop])
  (:import java.util.concurrent.atomic.AtomicReference))

(defn- compare-and-set!
  [^AtomicReference aref oldval newval]
  (or (.compareAndSet aref oldval newval)
      (throw (RuntimeException. ::loop))))

(defmacro with-contention
  [& body]
  `(loop [ret# ::loop]
     (if (= ret# ::loop)
       (do (Thread/sleep (rand-int 10))
           (recur (try ~@body (catch Exception e# ::loop))))
       ret#)))

(definterface INode
  (setNext [v]))

;; Doubly-linked list.
(deftype Node
  [data
   ;; AtomicReference, containing the `Node` to the previous link in the list
   ^AtomicReference prev-ref

   ;; unsynchronized! mutable field, the `Node` to the next link in the list
   ^:unsynchronized-mutable next]
  INode
  (setNext [_ n] (set! next n)))

(defprotocol IQueue
  (pop [_])
  (push [_ v]))

(deftype Queue
  [;; The current head of the queue, an AtomicReference, containing a `Node`
   ^AtomicReference head-ref

   ;; The current tail of the queue, an AtomicReference, containing a `Node`
   ^AtomicReference tail-ref]
  IQueue
  (pop [_]
    (with-contention

      ;; Acquire current tail `Node`
      (when-let [^Node tail (.get tail-ref)]

        ;; Acquire prev `Node`, head `Node`, tail `Node`'s value
        (let [tail' (.get ^AtomicReference (.prev-ref tail))
              head  (.get head-ref)
              v     (.data tail)]

          ;; 1. If the tail and the head are the same, reset head to nil if 
          ;;    and only if the acquired head matches the current head.
          (when (identical? tail head)
            (compare-and-set! head-ref head nil))

          ;; 2. If and only if the current tail matches the acquired tail, set
          ;;    the tail to tail', i.e. the previous node from the tail.
          (compare-and-set! tail-ref tail tail')

          ;; To prevent dangling pointers, we reset the next pointer. In this
          ;; way, the GC can do its thing and clean up dead nodes.
          (.setNext tail nil)

          v))))

  (push [_ v]
    (with-contention

      ;; Acquire head `Node`, setup a new, candidate `Node`, to be pushed onto
      ;; the queue
      (let [^Node head  (.get head-ref)
            head'       (Node. v (AtomicReference. nil) head)]

        ;; If we have a head, the queue is not empty...
        (if head
          (do
            ;; 1a. Using the previous reference of the acquired head, if and
            ;;    only if it is nil, speculatively set it to head', i.e. the
            ;;    candidate. This may seem slightly confusing at first pass,
            ;;    but importantly, is safe in the event of a failure in step
            ;;    two, because the head-ref will remain intact.
            (compare-and-set! (.prev-ref head) nil head')

            ;; 2. If and only if the current head is equal to the acquired
            ;;    head, set the head-ref to head', i.e. the candidate.
            (compare-and-set! head-ref head head'))
          (do
            ;; 1b. If and only if the current head is equal to the acquired
            ;;    head, set it to the candidate. Finally, set the tail as well.
            (compare-and-set! head-ref head head')
            (.set tail-ref head')))
        v))))

(defn queue [] (Queue. (AtomicReference. nil) (AtomicReference. nil)))
