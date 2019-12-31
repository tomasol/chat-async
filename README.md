# Chat-async

Simple TCP chat based on [async-std tutorial](https://book.async.rs/tutorial/index.html).


## Usage
Running the server using `cargo run` will start TCP server listening on `127.0.0.1:8080`.
Telnet can be used to connect two clients:
```
$ telnet 127.0.0.1 8080
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
joe
```

```
$ telnet 127.0.0.1 8080
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
john
```
In session 1, type:
```
john: hello john!
```
In session 2, message should appear:
```
Got message from 'joe':  hello john!
```

### Supported commands:

* `/w` - list connected users
* `/q` - disconnect current user
