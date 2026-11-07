//! Persistent driver results, independent of transport and observer ownership.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tokio::sync::Notify;

use crate::ConnectionError;

/// The committed terminal outcome of a connection or endpoint driver.
///
/// This is independent of the connection's close reason. In particular, an I/O
/// error during local close remains observable here. None of these outcomes
/// proves that the protocol core, routing entries or socket owners were disposed.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DriverOutcome {
    /// Protocol draining finished, or an endpoint became idle with no handles.
    ///
    /// The connection's final close reason is present for connection drivers.
    Drained(Option<ConnectionError>),
    /// The original error returned by the driver's I/O operation.
    Io(Arc<io::Error>),
    /// The owning endpoint stopped; its original outcome is retained.
    EndpointLost(Arc<Self>),
    /// The runtime destroyed the unfinished driver, including before its first poll.
    ///
    /// This can also result from panic unwind. Process abort is not observable.
    Stopped,
}

impl fmt::Display for DriverOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drained(Some(reason)) => write!(f, "connection drained: {reason}"),
            Self::Drained(None) => f.write_str("endpoint drained"),
            Self::Io(error) => write!(f, "driver I/O error: {error}"),
            Self::EndpointLost(reason) => write!(f, "endpoint driver stopped: {reason}"),
            Self::Stopped => f.write_str("driver destroyed before reporting an outcome"),
        }
    }
}

impl std::error::Error for DriverOutcome {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Drained(Some(reason)) => Some(reason),
            Self::Io(error) => Some(&**error),
            Self::EndpointLost(reason) => Some(&**reason),
            _ => None,
        }
    }
}

/// Observation of one specific driver, created before that driver is spawned.
///
/// Clones retain only the result and notification state, not the connection,
/// endpoint, socket or driver. Outcomes are persistent, including completion
/// before observation. Dropping a waiter does not cancel the driver or erase its
/// outcome. A replacement driver has a separate lifecycle.
#[derive(Clone, Debug)]
pub struct DriverLifecycle(Arc<Shared>);

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct State {
    outcome: Option<Arc<DriverOutcome>>,
    disposed: bool,
}

impl DriverLifecycle {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Shared::default()))
    }

    /// The persisted terminal outcome, if one has been determined.
    pub fn outcome(&self) -> Option<Arc<DriverOutcome>> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .outcome
            .clone()
    }

    /// Wait for a committed terminal outcome.
    ///
    /// The driver can still be finishing its final poll or cleanup. This does
    /// not wait for destruction; use [`Self::disposed`] for that separate fact.
    pub async fn terminated(&self) -> Arc<DriverOutcome> {
        loop {
            let notified = {
                let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(outcome) = &state.outcome {
                    return outcome.clone();
                }
                // Notify::notified records the notify_waiters generation even
                // before its first poll. Construct it while reading readiness.
                self.0.changed.notified()
            };
            notified.await;
        }
    }

    /// Wait for destruction of the driver's owned future to finish.
    ///
    /// This does not wait for the runtime's task allocation to be freed. It is
    /// also not a protocol-core, route, stream-owner or socket-disposal receipt.
    pub async fn disposed(&self) {
        loop {
            let notified = {
                let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.disposed {
                    return;
                }
                self.0.changed.notified()
            };
            notified.await;
        }
    }

    /// Persist without invoking wakers. A connection can record an I/O cause
    /// before local-close notifications while still holding its protocol lock.
    pub(crate) fn record(&self, outcome: DriverOutcome) {
        {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.outcome.is_some() {
                return;
            }
            state.outcome = Some(Arc::new(outcome));
        }
    }

    fn publish(&self, outcome: DriverOutcome) {
        self.record(outcome);
        // User wakers must never run under this or a Quinn transport lock.
        // The outcome may have been recorded before ordinary close signals;
        // notify here even in that case, after the driver's poll released locks.
        self.0.changed.notify_waiters();
    }

    pub(crate) fn track<F>(&self, driver: F) -> DriverTask<F> {
        DriverTask {
            driver: Some(driver),
            owner: Some(DriverOwner(self.clone())),
        }
    }
}

struct DriverOwner(DriverLifecycle);

impl Drop for DriverOwner {
    fn drop(&mut self) {
        self.0
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .disposed = true;
        self.0.0.changed.notify_waiters();
    }
}

pin_project! {
    pub(crate) struct DriverTask<F> {
        #[pin]
        driver: Option<F>,
        owner: Option<DriverOwner>,
    }

    impl<F> PinnedDrop for DriverTask<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            if let Some(owner) = this.owner.as_ref() {
                owner.0.publish(DriverOutcome::Stopped);
            }
            // A taken Option is not disposal evidence. The owner remains until
            // the actual future and all of its fields have been destroyed.
            this.driver.set(None);
            this.owner.take();
        }
    }
}

impl<F: Future<Output = DriverOutcome>> Future for DriverTask<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        let Some(driver) = this.driver.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };
        let outcome = std::task::ready!(driver.poll(cx));
        if let Some(owner) = this.owner.as_ref() {
            owner.0.publish(outcome.clone());
        }
        if let DriverOutcome::Io(error) = &outcome {
            tracing::error!("I/O error: {error}");
        }
        this.driver.set(None);
        this.owner.take();
        Poll::Ready(())
    }
}

#[cfg(test)]
mod tests;
