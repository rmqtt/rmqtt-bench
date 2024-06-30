use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::{any, collections::VecDeque, future::Future, io, net::SocketAddr, pin::Pin};
use std::{mem, rc::Weak};

use ntex::connect::{Address, Connect, ConnectError, Resolver};
use ntex::io::{types, Handle, Io, IoStream, ReadContext, ReadStatus, WriteContext, WriteStatus};
use ntex::util::{ready, Either, PoolId, PoolRef};

use ntex::service::Service;
use ntex::time::{sleep, Sleep};
use ntex_bytes::{Buf, BufMut, BytesVec};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use log::{error, trace};

pub struct Connector<T> {
    resolver: Resolver<T>,
    pool: PoolRef,
    bind_addr: Option<SocketAddr>,
}

impl<T> Connector<T> {
    /// Construct new connect service with custom dns resolver
    pub fn new(bind_addr: Option<SocketAddr>) -> Self {
        Connector {
            resolver: Resolver::new(),
            pool: PoolId::P0.pool_ref(),
            bind_addr,
        }
    }
}

impl<T> Clone for Connector<T> {
    fn clone(&self) -> Self {
        Connector {
            resolver: self.resolver.clone(),
            pool: self.pool,
            bind_addr: self.bind_addr,
        }
    }
}

impl<T: Address> Service<Connect<T>> for Connector<T> {
    type Response = Io;
    type Error = ConnectError;
    type Future = ConnectServiceResponse<T>;

    #[inline]
    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&self, req: Connect<T>) -> Self::Future {
        ConnectServiceResponse::new(self.resolver.call(req), self.bind_addr)
    }
}

enum ConnectState<T: Address> {
    Resolve(<Resolver<T> as Service<Connect<T>>>::Future),
    Connect(TcpConnectorResponse),
}

#[doc(hidden)]
pub struct ConnectServiceResponse<T: Address> {
    state: ConnectState<T>,
    bind_addr: Option<SocketAddr>,
    pool: PoolRef,
}

impl<T: Address> ConnectServiceResponse<T> {
    pub(super) fn new(
        fut: <Resolver<T> as Service<Connect<T>>>::Future,
        bind_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            state: ConnectState::Resolve(fut),
            bind_addr,
            pool: PoolId::P0.pool_ref(),
        }
    }
}

impl<T: Address> Future for ConnectServiceResponse<T> {
    type Output = Result<Io, ConnectError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            ConnectState::Resolve(ref mut fut) => match Pin::new(fut).poll(cx)? {
                Poll::Pending => Poll::Pending,
                Poll::Ready(mut address) => {
                    let req = address.get_ref();
                    let port = address.port();
                    let req = req.host().to_owned();
                    let mut addrs = address.take_addrs().collect::<Vec<SocketAddr>>();

                    if let Some(addr) = addrs.pop() {
                        self.state = ConnectState::Connect(TcpConnectorResponse::new(
                            req,
                            port,
                            Either::Left(addr),
                            self.bind_addr,
                            self.pool,
                        ));
                        self.poll(cx)
                    } else if let Some(addr) = req.addr() {
                        self.state = ConnectState::Connect(TcpConnectorResponse::new(
                            req,
                            addr.port(),
                            Either::Left(addr),
                            self.bind_addr,
                            self.pool,
                        ));
                        self.poll(cx)
                    } else {
                        error!("TCP connector: got unresolved address");
                        Poll::Ready(Err(ConnectError::Unresolved))
                    }
                }
            },
            ConnectState::Connect(ref mut fut) => Pin::new(fut).poll(cx),
        }
    }
}

/// Tcp stream connector response future
struct TcpConnectorResponse {
    req: Option<ReqAddress>,
    port: u16,
    addrs: Option<VecDeque<SocketAddr>>,
    bind_addr: Option<SocketAddr>,
    #[allow(clippy::type_complexity)]
    stream: Option<Pin<Box<dyn Future<Output = Result<Io, io::Error>>>>>,
    pool: PoolRef,
}

impl TcpConnectorResponse {
    fn new(
        req: ReqAddress,
        port: u16,
        addr: Either<SocketAddr, VecDeque<SocketAddr>>,
        bind_addr: Option<SocketAddr>,
        pool: PoolRef,
    ) -> TcpConnectorResponse {
        log::trace!(
            "TCP connector - connecting to {:?} port:{}, addr: {:?}",
            req.host(),
            port,
            addr
        );

        match addr {
            Either::Left(addr) => TcpConnectorResponse {
                req: Some(req),
                addrs: None,
                bind_addr,
                stream: Some(Box::pin(tcp_bind_connect_in(addr, bind_addr, pool))),
                pool,
                port,
            },
            Either::Right(addrs) => TcpConnectorResponse {
                port,
                pool,
                req: Some(req),
                addrs: Some(addrs),
                bind_addr,
                stream: None,
            },
        }
    }

    fn can_continue(&self, err: &io::Error) -> bool {
        log::trace!(
            "TCP connector - failed to connect to {:?} port: {} err: {:?}",
            self.req.as_ref().unwrap().host(),
            self.port,
            err
        );
        !(self.addrs.is_none() || self.addrs.as_ref().unwrap().is_empty())
    }
}

impl Future for TcpConnectorResponse {
    type Output = Result<Io, ConnectError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // connect
        loop {
            if let Some(new) = this.stream.as_mut() {
                match new.as_mut().poll(cx) {
                    Poll::Ready(Ok(sock)) => {
                        let req = this.req.take().unwrap();
                        trace!(
                            "TCP connector - successfully connected to connecting to {:?} - {:?}",
                            req.host(),
                            sock.query::<types::PeerAddr>().get()
                        );
                        return Poll::Ready(Ok(sock));
                    }
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => {
                        if !this.can_continue(&err) {
                            return Poll::Ready(Err(err.into()));
                        }
                    }
                }
            }

            // try to connect
            let addr = this.addrs.as_mut().unwrap().pop_front().unwrap();
            this.stream = Some(Box::pin(tcp_bind_connect_in(
                addr,
                this.bind_addr,
                this.pool,
            )));
        }
    }
}

struct TcpStream(tokio::net::TcpStream);

impl IoStream for TcpStream {
    fn start(self, read: ReadContext, write: WriteContext) -> Option<Box<dyn Handle>> {
        let io = Rc::new(RefCell::new(self.0));
        tokio::task::spawn_local(ReadTask::new(io.clone(), read));
        tokio::task::spawn_local(WriteTask::new(io.clone(), write));
        Some(Box::new(HandleWrapper(io)))
    }
}

struct HandleWrapper(Rc<RefCell<tokio::net::TcpStream>>);

impl Handle for HandleWrapper {
    fn query(&self, id: any::TypeId) -> Option<Box<dyn any::Any>> {
        log::info!("id: {:?}", id);
        if id == any::TypeId::of::<types::PeerAddr>() {
            if let Ok(addr) = self.0.borrow().peer_addr() {
                return Some(Box::new(types::PeerAddr(addr)));
            }
        } else if id == any::TypeId::of::<SocketOptions>() {
            return Some(Box::new(SocketOptions(Rc::downgrade(&self.0))));
        }
        None
    }
}

/// Query TCP Io connections for a handle to set socket options
#[allow(dead_code)]
pub struct SocketOptions(Weak<RefCell<tokio::net::TcpStream>>);

/// Read io task
struct ReadTask {
    io: Rc<RefCell<tokio::net::TcpStream>>,
    state: ReadContext,
}

impl ReadTask {
    /// Create new read io task
    fn new(io: Rc<RefCell<tokio::net::TcpStream>>, state: ReadContext) -> Self {
        Self { io, state }
    }
}

impl Future for ReadTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref();

        loop {
            match ready!(this.state.poll_ready(cx)) {
                ReadStatus::Ready => {
                    let pool = this.state.memory_pool();
                    let mut io = this.io.borrow_mut();
                    let mut buf = self.state.get_read_buf();
                    let (hw, lw) = pool.read_params().unpack();

                    // read data from socket
                    let mut new_bytes = 0;
                    let mut close = false;
                    let mut pending = false;
                    loop {
                        // make sure we've got room
                        let remaining = buf.remaining_mut();
                        if remaining < lw {
                            buf.reserve(hw - remaining);
                        }

                        match poll_read_buf(Pin::new(&mut *io), cx, &mut buf) {
                            Poll::Pending => {
                                pending = true;
                                break;
                            }
                            Poll::Ready(Ok(n)) => {
                                if n == 0 {
                                    log::trace!("tokio stream is disconnected");
                                    close = true;
                                } else {
                                    new_bytes += n;
                                    if new_bytes <= hw {
                                        continue;
                                    }
                                }
                                break;
                            }
                            Poll::Ready(Err(err)) => {
                                log::trace!("read task failed on io {:?}", err);
                                drop(io);
                                this.state.release_read_buf(buf, new_bytes);
                                this.state.close(Some(err));
                                return Poll::Ready(());
                            }
                        }
                    }

                    drop(io);
                    if new_bytes == 0 && close {
                        this.state.close(None);
                        return Poll::Ready(());
                    }
                    this.state.release_read_buf(buf, new_bytes);
                    return if close {
                        this.state.close(None);
                        Poll::Ready(())
                    } else if pending {
                        Poll::Pending
                    } else {
                        continue;
                    };
                }
                ReadStatus::Terminate => {
                    log::trace!("read task is instructed to shutdown");
                    return Poll::Ready(());
                }
            }
        }
    }
}

pub fn poll_read_buf<T: AsyncRead>(
    io: Pin<&mut T>,
    cx: &mut Context<'_>,
    buf: &mut BytesVec,
) -> Poll<io::Result<usize>> {
    if !buf.has_remaining_mut() {
        return Poll::Ready(Ok(0));
    }

    let n = {
        let dst = unsafe { &mut *(buf.chunk_mut() as *mut _ as *mut [mem::MaybeUninit<u8>]) };
        let mut buf = ReadBuf::uninit(dst);
        let ptr = buf.filled().as_ptr();
        if io.poll_read(cx, &mut buf)?.is_pending() {
            return Poll::Pending;
        }

        // Ensure the pointer does not change from under us
        assert_eq!(ptr, buf.filled().as_ptr());
        buf.filled().len()
    };

    // Safety: This is guaranteed to be the number of initialized (and read)
    // bytes due to the invariants provided by `ReadBuf::filled`.
    unsafe {
        buf.advance_mut(n);
    }

    Poll::Ready(Ok(n))
}

#[derive(Debug)]
enum IoWriteState {
    Processing(Option<Sleep>),
    Shutdown(Sleep, Shutdown),
}

#[derive(Debug)]
enum Shutdown {
    None,
    Flushed,
    Stopping(u16),
}

/// Write io task
struct WriteTask {
    st: IoWriteState,
    io: Rc<RefCell<tokio::net::TcpStream>>,
    state: WriteContext,
}

impl WriteTask {
    /// Create new write io task
    fn new(io: Rc<RefCell<tokio::net::TcpStream>>, state: WriteContext) -> Self {
        Self {
            io,
            state,
            st: IoWriteState::Processing(None),
        }
    }
}

impl Future for WriteTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();

        match this.st {
            IoWriteState::Processing(ref mut delay) => {
                match this.state.poll_ready(cx) {
                    Poll::Ready(WriteStatus::Ready) => {
                        if let Some(delay) = delay {
                            if delay.poll_elapsed(cx).is_ready() {
                                this.state.close(Some(io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "Operation timedout",
                                )));
                                return Poll::Ready(());
                            }
                        }

                        // flush framed instance
                        match flush_io(&mut *this.io.borrow_mut(), &this.state, cx) {
                            Poll::Pending | Poll::Ready(true) => Poll::Pending,
                            Poll::Ready(false) => Poll::Ready(()),
                        }
                    }
                    Poll::Ready(WriteStatus::Timeout(time)) => {
                        log::trace!("initiate timeout delay for {:?}", time);
                        if delay.is_none() {
                            *delay = Some(sleep(time));
                        }
                        self.poll(cx)
                    }
                    Poll::Ready(WriteStatus::Shutdown(time)) => {
                        log::trace!("write task is instructed to shutdown");

                        let timeout = if let Some(delay) = delay.take() {
                            delay
                        } else {
                            sleep(time)
                        };

                        this.st = IoWriteState::Shutdown(timeout, Shutdown::None);
                        self.poll(cx)
                    }
                    Poll::Ready(WriteStatus::Terminate) => {
                        log::trace!("write task is instructed to terminate");

                        if !matches!(
                            this.io.borrow().linger(),
                            Ok(Some(std::time::Duration::ZERO))
                        ) {
                            // call shutdown to prevent flushing data on terminated Io. when
                            // linger is set to zero, closing will reset the connection, so
                            // shutdown is not neccessary.
                            let _ = Pin::new(&mut *this.io.borrow_mut()).poll_shutdown(cx);
                        }
                        this.state.close(None);
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            IoWriteState::Shutdown(ref mut delay, ref mut st) => {
                // close WRITE side and wait for disconnect on read side.
                // use disconnect timeout, otherwise it could hang forever.
                loop {
                    match st {
                        Shutdown::None => {
                            // flush write buffer
                            match flush_io(&mut *this.io.borrow_mut(), &this.state, cx) {
                                Poll::Ready(true) => {
                                    *st = Shutdown::Flushed;
                                    continue;
                                }
                                Poll::Ready(false) => {
                                    log::trace!("write task is closed with err during flush");
                                    this.state.close(None);
                                    return Poll::Ready(());
                                }
                                _ => (),
                            }
                        }
                        Shutdown::Flushed => {
                            // shutdown WRITE side
                            match Pin::new(&mut *this.io.borrow_mut()).poll_shutdown(cx) {
                                Poll::Ready(Ok(_)) => {
                                    *st = Shutdown::Stopping(0);
                                    continue;
                                }
                                Poll::Ready(Err(e)) => {
                                    log::trace!("write task is closed with err during shutdown");
                                    this.state.close(Some(e));
                                    return Poll::Ready(());
                                }
                                _ => (),
                            }
                        }
                        Shutdown::Stopping(ref mut count) => {
                            // read until 0 or err
                            let mut buf = [0u8; 512];
                            loop {
                                let mut read_buf = ReadBuf::new(&mut buf);
                                match Pin::new(&mut *this.io.borrow_mut())
                                    .poll_read(cx, &mut read_buf)
                                {
                                    Poll::Ready(Err(_)) | Poll::Ready(Ok(_))
                                        if read_buf.filled().is_empty() =>
                                    {
                                        this.state.close(None);
                                        log::trace!("write task is stopped");
                                        return Poll::Ready(());
                                    }
                                    Poll::Pending => {
                                        *count += read_buf.filled().len() as u16;
                                        if *count > 4096 {
                                            log::trace!("write task is stopped, too much input");
                                            this.state.close(None);
                                            return Poll::Ready(());
                                        }
                                        break;
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }

                    // disconnect timeout
                    if delay.poll_elapsed(cx).is_pending() {
                        return Poll::Pending;
                    }
                    log::trace!("write task is stopped after delay");
                    this.state.close(None);
                    return Poll::Ready(());
                }
            }
        }
    }
}

/// Flush write buffer to underlying I/O stream.
pub(super) fn flush_io<T: AsyncRead + AsyncWrite + Unpin>(
    io: &mut T,
    state: &WriteContext,
    cx: &mut Context<'_>,
) -> Poll<bool> {
    let mut buf = if let Some(buf) = state.get_write_buf() {
        buf
    } else {
        return Poll::Ready(true);
    };
    let len = buf.len();
    let pool = state.memory_pool();

    if len != 0 {
        // log::trace!("flushing framed transport: {:?}", buf.len());

        let mut written = 0;
        while written < len {
            match Pin::new(&mut *io).poll_write(cx, &buf[written..]) {
                Poll::Pending => break,
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        log::trace!("Disconnected during flush, written {}", written);
                        pool.release_write_buf(buf);
                        state.close(Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write frame to transport",
                        )));
                        return Poll::Ready(false);
                    } else {
                        written += n
                    }
                }
                Poll::Ready(Err(e)) => {
                    log::trace!("Error during flush: {}", e);
                    pool.release_write_buf(buf);
                    state.close(Some(e));
                    return Poll::Ready(false);
                }
            }
        }
        log::trace!("flushed {} bytes", written);

        // remove written data
        let result = if written == len {
            buf.clear();
            if let Err(e) = state.release_write_buf(buf) {
                state.close(Some(e));
                return Poll::Ready(false);
            }
            Poll::Ready(true)
        } else {
            buf.advance(written);
            if let Err(e) = state.release_write_buf(buf) {
                state.close(Some(e));
                return Poll::Ready(false);
            }
            Poll::Pending
        };

        // flush
        match Pin::new(&mut *io).poll_flush(cx) {
            Poll::Ready(Ok(_)) => result,
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                log::trace!("error during flush: {}", e);
                state.close(Some(e));
                Poll::Ready(false)
            }
        }
    } else if let Err(e) = state.release_write_buf(buf) {
        state.close(Some(e));
        Poll::Ready(false)
    } else {
        Poll::Ready(true)
    }
}

/// Opens a TCP connection to a remote host and use specified memory pool and bind local addr.
pub async fn tcp_bind_connect_in(
    addr: SocketAddr,
    bind_addr: Option<SocketAddr>,
    pool: PoolRef,
) -> io::Result<Io> {
    if let Some(bind_addr) = bind_addr {
        let socket = match &addr {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        socket.bind(bind_addr)?;
        let sock = socket.connect(addr).await?;
        sock.set_nodelay(true)?;
        Ok(Io::with_memory_pool(TcpStream(sock), pool))
    } else {
        tcp_connect_in(addr, pool).await
    }
}

/// Opens a TCP connection to a remote host and use specified memory pool.
pub async fn tcp_connect_in(addr: SocketAddr, pool: PoolRef) -> io::Result<Io> {
    let sock = tokio::net::TcpStream::connect(addr).await?;
    sock.set_nodelay(true)?;
    Ok(Io::with_memory_pool(TcpStream(sock), pool))
}

pub struct ConnectorFactory<U = Connector<ReqAddress>> {
    srv: U,
}

impl ConnectorFactory {
    pub fn new(bind_addr: Option<SocketAddr>) -> Self {
        Self {
            srv: Connector::new(bind_addr),
        }
    }
}

impl<U> ntex::service::IntoService<U, Connect<ReqAddress>> for ConnectorFactory<U>
where
    U: Service<Connect<ReqAddress>, Error = ConnectError>,
{
    fn into_service(self) -> U {
        self.srv
    }
}

pub type ReqAddress = String;
