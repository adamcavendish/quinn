use super::*;
use crate::{DriverOutcome, Runtime as QuinnRuntime};

struct CloseWake {
    lifecycle: crate::DriverLifecycle,
    observed_cause: AtomicBool,
}

impl std::task::Wake for CloseWake {
    fn wake(self: Arc<Self>) {
        // Called synchronously by Quinn's ordinary close notification. The
        // original I/O cause must already exist before LocallyClosed is visible.
        self.observed_cause.store(
            matches!(
                self.lifecycle.outcome().as_deref(),
                Some(DriverOutcome::Io(_) | DriverOutcome::EndpointLost(_))
            ),
            Ordering::SeqCst,
        );
    }
}

#[derive(Debug)]
struct ControlledRuntime {
    inner: Arc<dyn QuinnRuntime>,
    tasks: Mutex<Vec<Option<tokio::sync::oneshot::Sender<()>>>>,
    discard: usize,
}

impl ControlledRuntime {
    fn cancel(&self, index: usize) {
        let cancel = self.tasks.lock().unwrap()[index].take().unwrap();
        cancel.send(()).unwrap();
    }
}

impl QuinnRuntime for ControlledRuntime {
    fn new_timer(&self, deadline: Instant) -> Pin<Box<dyn AsyncTimer>> {
        self.inner.new_timer(deadline)
    }
    fn now(&self) -> Instant {
        self.inner.now()
    }
    fn wrap_udp_socket(&self, socket: UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        self.inner.wrap_udp_socket(socket)
    }
    fn spawn(&self, mut future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
        let index = {
            let mut tasks = self.tasks.lock().unwrap();
            let index = tasks.len();
            tasks.push(Some(cancel));
            index
        };
        if index == self.discard {
            drop(future);
            return;
        }
        self.inner.spawn(Box::pin(async move {
            std::future::poll_fn(|cx| {
                if Pin::new(&mut cancelled).poll(cx).is_ready() {
                    Poll::Ready(())
                } else {
                    future.as_mut().poll(cx)
                }
            })
            .await;
            drop(future);
        }));
    }
}

#[derive(Debug)]
struct WritableFailure;
impl std::fmt::Display for WritableFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("injected writable failure")
    }
}
impl std::error::Error for WritableFailure {}

#[derive(Debug)]
struct FaultSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    pollers: Mutex<Vec<Arc<AtomicBool>>>,
}

#[derive(Debug)]
struct FaultPoller {
    inner: Pin<Box<dyn UdpPoller>>,
    fail: Arc<AtomicBool>,
}

impl UdpPoller for FaultPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fail.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                WritableFailure,
            )))
        } else {
            self.inner.as_mut().poll_writable(cx)
        }
    }
}

impl AsyncUdpSocket for FaultSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        let fail = Arc::new(AtomicBool::new(false));
        self.pollers.lock().unwrap().push(fail.clone());
        Box::pin(FaultPoller {
            inner: self.inner.clone().create_io_poller(),
            fail,
        })
    }
    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        self.inner.try_send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

struct Pair {
    client: Endpoint,
    server: Endpoint,
    runtime: Arc<ControlledRuntime>,
    socket: Arc<FaultSocket>,
    receive_failure: Arc<SocketFailure>,
}

impl Pair {
    fn new(inner: Arc<dyn QuinnRuntime>, discard: usize) -> Self {
        let factory = EndpointFactory::new();
        let key = PrivateKeyDer::Pkcs8(factory.cert.signing_key.serialize_der().into());
        let server_config =
            crate::ServerConfig::with_single_cert(vec![factory.cert.cert.der().clone()], key)
                .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(factory.cert.cert.der().clone()).unwrap();
        let runtime = Arc::new(ControlledRuntime {
            inner: inner.clone(),
            tasks: Mutex::new(Vec::new()),
            discard,
        });
        let receive_failure = Arc::new(SocketFailure::default());
        let socket = Arc::new(FaultSocket {
            inner: Arc::new(FailingSocket {
                inner: inner
                    .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
                    .unwrap(),
                failure: receive_failure.clone(),
            }),
            pollers: Mutex::new(Vec::new()),
        });
        let mut client = Endpoint::new_with_abstract_socket(
            factory.endpoint_config.clone(),
            None,
            socket.clone(),
            runtime.clone(),
        )
        .unwrap();
        client.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let server = Endpoint::new(
            factory.endpoint_config,
            Some(server_config),
            UdpSocket::bind("127.0.0.1:0").unwrap(),
            inner,
        )
        .unwrap();
        Self {
            client,
            server,
            runtime,
            socket,
            receive_failure,
        }
    }

    async fn connect(&self) -> (crate::Connection, crate::Connection) {
        let (client, server) = tokio::join!(
            self.client
                .connect(self.server.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { self.server.accept().await.unwrap().await },
        );
        (client.unwrap(), server.unwrap())
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.client.close(0u32.into(), b"test complete");
        self.server.close(0u32.into(), b"test complete");
    }
}

async fn within<F: Future>(runtime: &dyn QuinnRuntime, future: F) -> F::Output {
    let mut timer = runtime.new_timer(runtime.now() + Duration::from_secs(5));
    let mut future = pin!(future);
    std::future::poll_fn(|cx| {
        if let Poll::Ready(result) = future.as_mut().poll(cx) {
            return Poll::Ready(result);
        }
        assert!(
            timer.as_mut().poll(cx).is_pending(),
            "driver observation timed out"
        );
        Poll::Pending
    })
    .await
}

async fn heartbeat(client: &crate::Connection, server: &crate::Connection) {
    let mut send = client.open_uni().await.unwrap();
    send.write_all(b"healthy sibling").await.unwrap();
    send.finish().unwrap();
    let mut recv = server.accept_uni().await.unwrap();
    assert_eq!(recv.read_to_end(128).await.unwrap(), b"healthy sibling");
}

fn assert_io(outcome: &DriverOutcome) {
    let DriverOutcome::Io(error) = outcome else {
        panic!("lost I/O error: {outcome:?}")
    };
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<WritableFailure>()
            .is_some()
    );
}

async fn scenarios(runtime: Arc<dyn QuinnRuntime>) {
    // Actual per-connection writable failure, including failure while closing.
    for local_close in [false, true] {
        let pair = Pair::new(runtime.clone(), usize::MAX);
        let (connection, _server) = within(&*runtime, pair.connect()).await;
        let (sibling, server_sibling) = within(&*runtime, pair.connect()).await;
        let lifecycle = connection.driver_lifecycle();
        let close_wake = Arc::new(CloseWake {
            lifecycle: lifecycle.clone(),
            observed_cause: AtomicBool::new(false),
        });
        let waker = Waker::from(close_wake.clone());
        let mut closed = Box::pin(connection.closed());
        if !local_close {
            assert!(
                closed
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        pair.socket.pollers.lock().unwrap()[0].store(true, Ordering::SeqCst);
        if local_close {
            connection.close(0u32.into(), b"local close");
        } else {
            connection
                .send_datagram(Bytes::from_static(b"trigger"))
                .unwrap();
        }
        assert_io(within(&*runtime, lifecycle.terminated()).await.as_ref());
        within(&*runtime, lifecycle.disposed()).await;
        if !local_close {
            assert!(close_wake.observed_cause.load(Ordering::SeqCst));
        }
        assert!(matches!(
            connection.close_reason(),
            Some(crate::ConnectionError::LocallyClosed)
        ));
        within(&*runtime, heartbeat(&sibling, &server_sibling)).await;
        assert!(pair.client.driver_lifecycle().outcome().is_none());
    }

    // Cancelling a single established connection driver does not stop its endpoint.
    {
        let pair = Pair::new(runtime.clone(), usize::MAX);
        let (connection, _server) = within(&*runtime, pair.connect()).await;
        let (sibling, server_sibling) = within(&*runtime, pair.connect()).await;
        let lifecycle = connection.driver_lifecycle();
        pair.runtime.cancel(1);
        assert!(matches!(
            &*within(&*runtime, lifecycle.terminated()).await,
            DriverOutcome::Stopped
        ));
        within(&*runtime, lifecycle.disposed()).await;
        within(&*runtime, heartbeat(&sibling, &server_sibling)).await;
        // Deliberately retained handles still own the core and routes.
        assert_eq!(pair.client.open_connections(), 2);
        connection.close(0u32.into(), b"cleanup requested");
    }

    // Endpoint outcomes precede channel teardown and are shared with every child.
    for io_failure in [false, true] {
        let pair = Pair::new(runtime.clone(), usize::MAX);
        let (one, _server_one) = within(&*runtime, pair.connect()).await;
        let (two, _server_two) = within(&*runtime, pair.connect()).await;
        let endpoint = pair.client.driver_lifecycle();
        let close_wake = Arc::new(CloseWake {
            lifecycle: one.driver_lifecycle(),
            observed_cause: AtomicBool::new(false),
        });
        let waker = Waker::from(close_wake.clone());
        let mut closed = Box::pin(one.closed());
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if io_failure {
            pair.receive_failure.trigger();
        } else {
            pair.runtime.cancel(0);
        }
        let root = within(&*runtime, endpoint.terminated()).await;
        if io_failure {
            let DriverOutcome::Io(error) = &*root else {
                panic!("lost receive error")
            };
            assert_eq!(error.to_string(), "injected socket failure");
        } else {
            assert!(matches!(&*root, DriverOutcome::Stopped));
        }
        for conn in [&one, &two] {
            let lifecycle = conn.driver_lifecycle();
            let outcome = within(&*runtime, lifecycle.terminated()).await;
            let DriverOutcome::EndpointLost(cause) = &*outcome else {
                panic!("endpoint loss reported as drain")
            };
            assert!(Arc::ptr_eq(cause, &root));
            within(&*runtime, lifecycle.disposed()).await;
        }
        assert!(close_wake.observed_cause.load(Ordering::SeqCst));
        within(&*runtime, endpoint.disposed()).await;
    }

    // The connection can finish before Endpoint::connect returns its Connecting.
    {
        let pair = Pair::new(runtime.clone(), 1);
        let connecting = pair
            .client
            .connect(pair.server.local_addr().unwrap(), "localhost")
            .unwrap();
        let lifecycle = connecting.driver_lifecycle();
        assert!(matches!(
            &*lifecycle.outcome().unwrap(),
            DriverOutcome::Stopped
        ));
        within(&*runtime, lifecycle.disposed()).await;
        assert!(within(&*runtime, connecting).await.is_err());
    }

    // Endpoint construction can similarly return after an unpolled driver loss.
    {
        let pair = Pair::new(runtime.clone(), 0);
        let lifecycle = pair.client.driver_lifecycle();
        assert!(matches!(
            &*lifecycle.outcome().unwrap(),
            DriverOutcome::Stopped
        ));
        within(&*runtime, lifecycle.disposed()).await;
        assert!(
            pair.client
                .connect(pair.server.local_addr().unwrap(), "localhost")
                .is_err()
        );
    }

    // Retaining receipts after normal draining does not keep the UDP owner alive.
    {
        let pair = Pair::new(runtime.clone(), usize::MAX);
        let (connection, server) = within(&*runtime, pair.connect()).await;
        let connection_lifecycle = connection.driver_lifecycle();
        let endpoint_lifecycle = pair.client.driver_lifecycle();
        let socket = Arc::downgrade(&pair.socket);
        drop((connection, server, pair));
        let outcome = within(&*runtime, connection_lifecycle.terminated()).await;
        assert!(matches!(&*outcome, DriverOutcome::Drained(Some(_))));
        within(&*runtime, connection_lifecycle.disposed()).await;
        within(&*runtime, endpoint_lifecycle.disposed()).await;
        assert!(socket.upgrade().is_none());
    }
}

#[tokio::test]
async fn tokio_driver_lifecycle_and_original_causes() {
    scenarios(Arc::new(TokioRuntime)).await;
}

#[cfg(feature = "runtime-smol")]
#[test]
fn smol_driver_lifecycle_and_original_causes() {
    smol::block_on(scenarios(Arc::new(crate::SmolRuntime)));
}
