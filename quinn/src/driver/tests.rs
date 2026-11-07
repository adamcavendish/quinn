use super::*;
use std::sync::{
    Barrier,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::task::Waker;

fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn unpolled_drop_is_persistent_and_does_not_retain_driver_owners() {
    struct Payload(Arc<()>);
    let lifecycle = DriverLifecycle::new();
    let payload = Payload(Arc::new(()));
    let weak = Arc::downgrade(&payload.0);
    let task = lifecycle.track(async move {
        let _payload = payload;
        std::future::pending::<DriverOutcome>().await
    });
    drop(task);
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        &*lifecycle.outcome().unwrap(),
        DriverOutcome::Stopped
    ));
    assert!(poll(Box::pin(lifecycle.terminated()).as_mut()).is_ready());
    assert!(poll(Box::pin(lifecycle.disposed()).as_mut()).is_ready());
}

#[test]
fn termination_precedes_slow_destruction_on_completion_and_drop() {
    struct Driver {
        complete: bool,
        entered: mpsc::Sender<()>,
        proceed: Arc<Barrier>,
        destroyed: Arc<AtomicBool>,
    }
    impl Future for Driver {
        type Output = DriverOutcome;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            if self.complete {
                Poll::Ready(DriverOutcome::Drained(None))
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for Driver {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.proceed.wait();
            self.destroyed.store(true, Ordering::SeqCst);
        }
    }
    for complete in [false, true] {
        let lifecycle = DriverLifecycle::new();
        let (entered, receiver) = mpsc::channel();
        let proceed = Arc::new(Barrier::new(2));
        let destroyed = Arc::new(AtomicBool::new(false));
        let mut task = Box::pin(lifecycle.track(Driver {
            complete,
            entered,
            proceed: proceed.clone(),
            destroyed: destroyed.clone(),
        }));
        let worker = std::thread::spawn(move || {
            if complete {
                assert!(poll(task.as_mut()).is_ready());
            } else {
                drop(task);
            }
        });
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(lifecycle.outcome().is_some());
        assert!(!destroyed.load(Ordering::SeqCst));
        let mut disposed = Box::pin(lifecycle.disposed());
        assert!(poll(disposed.as_mut()).is_pending());
        proceed.wait();
        worker.join().unwrap();
        assert!(destroyed.load(Ordering::SeqCst));
        assert!(poll(disposed.as_mut()).is_ready());
    }
}

#[derive(Debug)]
struct Marker(u64);
impl fmt::Display for Marker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "marker {}", self.0)
    }
}
impl std::error::Error for Marker {}

#[test]
fn io_error_retains_its_type_and_survives_later_drop() {
    let lifecycle = DriverLifecycle::new();
    let error = Arc::new(io::Error::new(io::ErrorKind::PermissionDenied, Marker(42)));
    let mut task = Box::pin(lifecycle.track(std::future::ready(DriverOutcome::Io(error.clone()))));
    assert!(poll(task.as_mut()).is_ready());
    drop(task);
    let outcome = lifecycle.outcome().unwrap();
    let DriverOutcome::Io(actual) = &*outcome else {
        panic!("lost I/O cause")
    };
    assert!(Arc::ptr_eq(actual, &error));
    assert_eq!(actual.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(
        actual
            .get_ref()
            .unwrap()
            .downcast_ref::<Marker>()
            .unwrap()
            .0,
        42
    );
}

#[test]
fn cancelled_and_late_observers_do_not_erase_or_lose_outcomes() {
    for _ in 0..256 {
        let lifecycle = DriverLifecycle::new();
        let mut waiter = Box::pin(lifecycle.terminated());
        assert!(poll(waiter.as_mut()).is_pending());
        drop(waiter);
        let mut waiter = Box::pin(lifecycle.terminated());
        assert!(poll(waiter.as_mut()).is_pending());
        let task = lifecycle.track(std::future::pending::<DriverOutcome>());
        drop(task);
        let Poll::Ready(first) = poll(waiter.as_mut()) else {
            panic!("lost wakeup")
        };
        let Poll::Ready(late) = poll(Box::pin(lifecycle.terminated()).as_mut()) else {
            panic!("lost result")
        };
        assert!(Arc::ptr_eq(&first, &late));
    }
}

#[test]
fn panic_unwind_reports_loss_and_disposal() {
    let lifecycle = DriverLifecycle::new();
    let task = lifecycle.track(async { panic!("driver panic") });
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = poll(Box::pin(task).as_mut());
        }))
        .is_err()
    );
    assert!(matches!(
        &*lifecycle.outcome().unwrap(),
        DriverOutcome::Stopped
    ));
    assert!(poll(Box::pin(lifecycle.disposed()).as_mut()).is_ready());
}
