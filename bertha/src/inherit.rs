//! Helper traits to avoid re-writing common Chunnel implementation code for simple Chunnels.

use super::ChunnelConnection;

/// A standard way to access the downstream chunnel.
///
/// Be careful: Most implementors which contain Arcs will need to do a deep clone to implement
/// context_mut().
pub trait Context {
    /// The inner chunnel this chunnel is wrapping.
    type ChunnelType;

    fn context(&self) -> &Self::ChunnelType;
    fn context_mut(&mut self) -> &mut Self::ChunnelType;
}

/// A simpler trait to implement for traits with simpler (or no) Chunnel setup.
pub trait InheritChunnel<CxConn: ChunnelConnection>: Context {
    type Connection: ChunnelConnection;
    type Config: Clone + Send + Sync + 'static;

    fn get_config(&mut self) -> Self::Config;
    fn make_connection(cx: CxConn, cfg: Self::Config) -> Self::Connection;
}

#[macro_export]
macro_rules! inherit_listener (
    ($tname:ident, $innerdata:ident, $cntype: ident, $chdata:ty) => {
impl<Inner, $innerdata> $crate::ChunnelListener<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelListener<$innerdata>,
    Inner::Connection: ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>,
    >;
    type Stream = Pin<
        Box<
            dyn futures_util::stream::Stream<Item = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        use futures_util::StreamExt;
        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await?;
            Ok(
                Box::pin(conn_stream.map(move |conn: Result<Inner::Connection, _>| {
                    Ok(Self::make_connection(conn?, cfg.clone()))
                })) as _,
            )
        })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
    ($tname:ident, $innerdata:ty, $cntype: ident, $chdata:ident) => {
impl<Inner, $chdata> $crate::ChunnelListener<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelListener<$innerdata>,
    Inner::Connection: ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>,
    >;
    type Stream = Pin<
        Box<
            dyn futures_util::stream::Stream<Item = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        use futures_util::StreamExt;
        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await?;
            Ok(
                Box::pin(conn_stream.map(move |conn: Result<Inner::Connection, _>| {
                    Ok(Self::make_connection(conn?, cfg.clone()))
                })) as _,
            )
        })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
    ($tname:ident, $innerdata:ty, $cntype: ident, $chdata:ty) => {
impl<Inner> $crate::ChunnelListener<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelListener<$innerdata>,
    Inner::Connection: ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>,
    >;
    type Stream = Pin<
        Box<
            dyn futures_util::stream::Stream<Item = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        use futures_util::StreamExt;
        let f = self.context_mut().listen(a);
        let cfg = self.get_config();
        Box::pin(async move {
            let cfg = cfg;
            let conn_stream = f.await?;
            Ok(
                Box::pin(conn_stream.map(move |conn: Result<Inner::Connection, _>| {
                    Ok(Self::make_connection(conn?, cfg.clone()))
                })) as _,
            )
        })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
);

#[macro_export]
macro_rules! inherit_connector (
    ($tname:ident, $innerdata:ident, $cntype: ident, $chdata:ty) => {
impl<Inner, $innerdata> $crate::ChunnelConnector<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelConnector<$innerdata>,
    Inner::Connection: $crate::ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(Self::make_connection(f.await?, cfg)) })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
    ($tname:ident, $innerdata:ty, $cntype: ident, $chdata:ident) => {
impl<Inner, $chdata> $crate::ChunnelConnector<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelConnector<$innerdata>,
    Inner::Connection: $crate::ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(Self::make_connection(f.await?, cfg)) })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
    ($tname:ident, $innerdata:ty, $cntype: ident, $chdata:ty) => {
impl<Inner> $crate::ChunnelConnector<$chdata> for $tname<Inner>
where
    Inner: $crate::ChunnelConnector<$innerdata>,
    Inner::Connection: $crate::ChunnelConnection<Data = $innerdata> + Send + Sync + 'static,
    Self: InheritChunnel<Inner::Connection, Connection = $cntype<Inner::Connection>>,
    Self: Context<ChunnelType = Inner>,
{
    type Addr = Inner::Addr;
    type Connection = $cntype<Inner::Connection>;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Connection, Self::Error>>
                + Send
                + 'static,
        >,
    >;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let f = self.context_mut().connect(a);
        let cfg = self.get_config();
        Box::pin(async move { Ok(Self::make_connection(f.await?, cfg)) })
    }

    fn scope() -> crate::Scope {
        Inner::scope()
    }

    fn endedness() -> crate::Endedness {
        Inner::endedness()
    }

    fn implementation_priority() -> usize {
        Inner::implementation_priority()
    }
}
    };
);

// blanket implementation across all functionality for Context types - might need to reverse this later.
impl<C, T> crate::Negotiate<T> for C
where
    C: Context,
    T: crate::CapabilitySet + crate::NegotiateDummy,
{
}
