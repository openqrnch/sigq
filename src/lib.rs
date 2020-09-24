//! Queue which supports pushing and poping nodes from threads/tasks, crossing
//! sync/async boundaries.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::thread;

pub struct Queue<I> {
  signal: Arc<Condvar>,
  q: Arc<Mutex<VecDeque<I>>>
}

impl<I> Queue<I> {
  /// Create, and return, a new queue.
  pub fn new() -> Self {
    Queue {
      signal: Arc::new(Condvar::new()),
      q: Arc::new(Mutex::new(VecDeque::new()))
    }
  }

  /// Returns a boolean indicating whether the queue was empty or not.
  ///
  /// This function is not particularly useful.  If you don't understand why,
  /// then please don't use it.
  pub fn was_empty(&self) -> bool {
    let q = self.q.lock().unwrap();
    q.is_empty()
  }

  /// Push a node on to the queue and unlock one queue reader, if any.
  pub fn push(&self, item: I) {
    let mut q = self.q.lock().unwrap();
    q.push_back(item);
    drop(q);
    self.signal.notify_one();
  }

  /// Pull the oldest node off the queue and return it.  If no nodes are
  /// available on the queue, then block and wait for one to become available.
  pub fn pop(&self) -> I {
    let mut q = self.q.lock().unwrap();

    let node = loop {
      match q.pop_front() {
        Some(node) => {
          break node;
        }
        None => {
          q = self.signal.wait(q).unwrap();
        }
      }
    };
    drop(q);

    node
  }

  /// This method serves the same purpose as the [`pop()`](#method.pop) method,
  /// but rather than block it  returns a `Future` to be used in an `async`
  /// context.
  ///
  /// ```
  /// use sigq::Queue;
  /// async fn test() {
  ///   let q = Queue::new();
  ///   q.push("hello".to_string());
  ///   assert_eq!(q.was_empty(), false);
  ///   let node = q.apop().await;
  ///   assert_eq!(node, "hello");
  ///   assert_eq!(q.was_empty(), true);
  /// }
  /// ```
  pub fn apop(&self) -> PopFuture<I> {
    PopFuture::new(self)
  }
}

#[doc(hidden)]
pub struct PopFuture<I> {
  signal: Arc<Condvar>,
  q: Arc<Mutex<VecDeque<I>>>
}

impl<I> PopFuture<I> {
  fn new(q: &Queue<I>) -> Self {
    PopFuture {
      signal: Arc::clone(&q.signal),
      q: Arc::clone(&q.q)
    }
  }
}

impl<I: 'static + Send> Future for PopFuture<I> {
  type Output = I;
  fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut q = self.q.lock().unwrap();
    match q.pop_front() {
      Some(node) => Poll::Ready(node),
      None => {
        let waker = ctx.waker().clone();
        let qc = Arc::clone(&self.q);
        let signal = Arc::clone(&self.signal);
        thread::spawn(move || {
          let mut iq = qc.lock().unwrap();
          while iq.is_empty() {
            iq = signal.wait(iq).unwrap();
          }
          drop(iq);
          waker.wake();
        });
        drop(q);
        Poll::Pending
      }
    }
  }
}

// vim: set ft=rust et sw=2 ts=2 sts=2 cinoptions=2 tw=79 :
