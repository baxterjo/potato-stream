# API Use Remarks

My experience of first time use with the ditto bus API. These remarks are naive and do not take into account the requirements of the implementation of the ditto bus nor the likely storied history of the bus as a feature. Because I am not familiar with the implementation of the bus, it gives me the ability to comment on the usability of the bus api and accompanying documentation with fresh eyes. While using this API for the first time, I had a preexisting set of assumptions (many I/O apis are similar), and a long history with Rust API's and the accompanying docs. So I had certain expectations when opening the docs. The following sections and lists are in no particular order.

## Naming Things

We all know the second hardest thing in software development is naming things. But I think there are a few assumptions I made based off of the name of these features that are not true.

### Ditto Bus

This one is spicy, so get ready.

When I hear the term bus, I go back to my embedded roots and think of a CAN bus. On a CAN bus, data is constantly flowing from producers to the same PHY layer. If a consumer node wants to receive that data, all it needs to do is tap into the bus to receive the data. The other OSI layers are implemented with varying degrees of completeness depending on the needs of the application. Importantly, the work on the PHY layer of this communication bus is not replicated. One message can be emitted from a producer and be seen by many consumers.

With this pre-existing definition of "bus" in my mind, I drew an analogous connection between a CAN bus and a Ditto bus but several layers up. I thought that when I wanted to receive data on this bus, all I would have to do is subscribe to a topic, and I would be able to see data without needing any other knowledge about the mesh.

The bus as it exists today is actually more like a socket. Where there are servers and clients. The clients must know the address (pubkey) and port (topic) that the server is listening on ahead of time, and the server must handle multiple sockets and replicate data streams to those sockets, resulting in repeated use of the PHY layer on the mesh for the same data.

All this to say, it is my opinion that this feature should have socket in the name and not bus. I am partial to DDS (ditto domain sockets) or DS (ditto socket), but I'll leave the naming to sales/marketing. When I as a developer hear "socket" I am already thinking of the type of code I will need to write.

### Acceptor

Falling in line with the socket suggestion above. An "acceptor" should be called a listener.

## Rust SDK

The application I'm writing is written in the Rust API and I don't know Kotlin. So all of my feedback on usage will be targeted at the Rust API. (Apologies ahead of time Pierre)

Again, I am writing this opinion with no prior knowledge about what is going on under the hood of the bus, nor the product level requirements of the bus.

Generally, it is my opinion that the Rust API should model itself after existing, well known Rust networking APIs. I will mostly be referencing `tokio::net`,`std::net`, or `tokio-tungstenite` (the tokio websocket crate) with some minor references to higher level application layer network protocols mixed in.

There is [some research](https://jserd.springeropen.com/articles/10.1186/s40411-018-0050-8#Sec24) that correlates the simplicity of documentation to an SDK's adoption. But I think most developers intuitively know that their own adoption of an SDK correlates to the simplicity of its API and the thoroughness of its documentation.

**STOP HERE IF YOU DISAGREE THAT THIS API SHOULD BE MODELED AFTER OTHER RUST NETWORKING APIS**

### Docs Entrypoint

The `dittolive_ditto::experimental::bus` module level docs should EITHER:

- Have the bulk of the documentation with most of the examples. This should be seen as a "quick start" guide for developers using the bus for the first time.
- Have an "organization" section that looks like [Tokio's net module](https://docs.rs/tokio/latest/tokio/net/index.html)

### API

Many of these suggestions will boil down to two major points:

- How do we make this interface look as close to existing socket libraries as possible? If I as a developer have already written countless TCP, UDP, or Websocket applications, what assumptions will I make while writing an application that uses the ditto bus? Will those assumptions help or hinder my interactions with this SDK?
- Where we do not have a concrete example from existing socket libraries: K.I.S.S. (keep it simple stupid) my first impression of the bus API is that we are trying to provide too much flexibility at the expense of keeping the API easy to pick up and use.

#### Establishing a Connection

When establishing a connection using either the `bind_topic()` or `connect()` functions, the bus does not actually do what the function signatures suggest. Instead of taking the action described by the function signatures, they actually instantiate an `AcceptorBuilder` and `ConnectionBuilder` respectively that then need to be tweaked and `finished()` before they are actually bound or connected.

Referencing the `tokio::net` module, if a developer wants to use the builder pattern, they will first instantiate a `TcpSocket` then tweak to their liking, then call the `connect()` method on the socket.

If we want to mirror this functionality, then the following adjustments should be made to the bus API.

##### Establishing a Connection using `bind_topic()`

The `Acceptor` struct is analogous to a socket listener, so it should likely have the same name and methods as a socket listener.

To mirror other networking libraries, `Bus::bind_topic()` should return a listener with default settings that can `accept()` connections. This `accept()` method should return a `Result<Stream, ConnectionError>`.

If we want to continue providing a builder pattern, then we should provide a `Bus::acceptor_builder()` method that instantiates a builder. This builder should then have a `bind()` method that returns a listener.

##### Establishing a Connection using `connect()`

To mirror other networking libraries `Bus::connect()` should open a connection to the supplied `PubKey` and `Topic` with default settings and return a `Result<Stream, ConnectionError>`

A new function called `Bus::connect_async()` should do the same but wrapped in a future.

If a user wants to change the default settings of a connection, we can provide a `Bus::connection()` method to return a connection builder that then has its own corresponding `ConnectionBuilder::connect()` and `ConnectionBuilder::connect_async()` methods.

#### The return type of connections

Ideally the `Acceptor::accept()` (see above), `Bus::connect()` (see above), and `ConnectionBuilder::connect()` methods should all return the same type, (with wrapper's depending on the function). Ideally this return type would be `Result<Stream, ConnectionError>`

#### Stream Candidate

Stream candidate should go away, if the connection is made via one of the above methods, an open stream should be returned to the user.

#### Sending and Receiving Data

User provided channels (and by extension `IntoChannel`) should also go away, this is a major pain point for the creation of streams, and the readability of the documentation. It is not uncommon for networking crates to provide their own channels (see [rumqttc](https://docs.rs/rumqttc/latest/rumqttc/struct.AsyncClient.html) which uses `flume`) Instead, a ditto stream should implement `send()` and `read()` directly, as well as implement their async trait counterparts `Sink` and `Stream`.
