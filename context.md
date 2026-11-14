`context` is a built-in package in Go that is used to carry information related to cancellation signals, timeouts, deadlines and other request-scoped data across functions and go routines.

One of the most common use cases of `context` is to _cancel_ stuff in go. Suppose you've work going on in your program across several go routines, maybe serving HTTP requests or serving DB calls. And suddenly due to some reason (say system failure) you want to cancel everything that the program is doing right now and exit. `context` lets us do exactly that so that every part of your code gets the signal to exit and they do not keep wasting resources.

We'll see the different use cases that `context` provides us and why do we even need context for those use cases in the first place.

But let's start with the basics of context.

This is what the `Context` interface inside the `context` package looks like. Every context implements all the methods in this interface.

```go
type Context interface {
	Deadline() (deadline time, ok bool) // deadline
	Done() <-chan struct{}              // cancellation signal
	Err() error
	Value(key any) any                  // other values
}
```

A context simply carries three primary components: a deadline, a cancellation signal and other values that might be useful while making/serving a request.
We'll understand more about these components in detail as we see how to create and use a context.

## Creating a context:

```go
ctx := context.Background() // root context
```

The above function call returns an empty (but non-nil) context. The deadline, cancellation and value component for an empty context (and hence a background context) are all nil.
Therefore a Background context can never be cancelled. (What it means for a context to be cancelled will be clear in a while, but I mentioned this here for the sake of completeness).

This background context is not useful in itself but is used as the root/parent to derive other contexts from.

There are 4 main ways to create a useful context:

## `WithCancel`

```go
ctx, cancel := context.WithCancel(context.Background())
```

You can see we passed in a background context as a parameter to the WithCancel function and it returned us 2 things: and new context `ctx` and a `cancel` function.
`ctx` here is a derived (child) context from the root (parent) background context. A child context points to its parent context.

We use this context typically when we want to release resources associated with our program when we're done executing it.

We can call the `cancel` function and it will cancel the context `ctx` and all the resources associated with it.

How does this cancellation mechanism work internally?

We saw the Context interface above, which has a `Done()` method. This method returns a channel from which we can read like this:

```go
<- ctx.Done()
```

Now, `WithCancel` [returns](https://cs.opensource.google/go/go/+/refs/tags/go1.26.1:src/context/context.go;l=277) a context of type [cancelCtx](https://cs.opensource.google/go/go/+/refs/tags/go1.26.1:src/context/context.go;l=431). You can see that this type has a channel named `done` among other things.
The `Done()` method of a context returns exactly this channel. So we can block and try to read from this channel.

When the `cancel` function associated with a context is called, the `done` channel of the context is closed. Since we were waiting to receive from this channel as soon as the channel is closed, we'll receive zero value from this channel and we can exit from whatever we're doing.

Let us see this in action through an example where this context cancellation would be _actually_ useful.

In the code below inside the `main()` we have a long running process, say serving 1k requests. For each request we call a function `handleRequest()` which internally calls a goroutine `getDouble()`. This go routine does some processing and sends the result into a channel. We read from this channel inside `handleRequest()` and do some processing. The condition `if i % 2 == 0 { return }`, just indicates that there might be some reason (system errors ) due which the receiver exited before reading from the channel.

```go
func getDouble(i int, ch chan<- int) {
	// doing some processing
	i *= 2
	ch <- i // this send will only happen if there's a corresponding receiver
}

func handleRequest(i int) {
	ch := make(chan int)

	go getDouble(i, ch)

	// simulating a random error
	if i % 2 == 0 {
		return // early exit without reading from the channel
	}

	// read from channel and do some processing
	<- ch
}

func main() {

	// simulating a long running process
	for i := range 1000 {
		handleRequest(i)
		fmt.Printf("Request number %d served\n", i)
		fmt.Printf("Goroutines alive: %d\n", runtime.NumGoroutine())
		time.Sleep(1 * time.Second)
	}

}
```

If you run this program everything seems to work fine on the surface. But there's a subtle issue.

If you examine the code closely, ideally for each request there should be at max 2 go routines running: one the main go routine and the other `getDouble()`. Once a particular request is served, the `getDouble()` spawned in it's handleRequest should exit.

However, if you see the result getting printed you'll see that as the requests are served, the number of go routines alive in the go runtime keeps increasing. Why is that happening?

This is because when `handleRequest()` (i.e. the receiver) exits in mid-way due to an error and returns to main there is no receiver remaining for `ch` but the `getDouble()` go routine (i.e. the sender) is still blocked trying to send into the channel `ch`.
So even though `handleRequest()` exits, the go routine it spawns is still alive.

(Note that `handleRequest()` cannot `close` the channel `ch` while returning. That will cause the program to panic because `getDouble()` will try to send into a closed channel. Only the sender should ever close a channel not the receiver.)

This is a typical example of a go routine leak. Because go runtime cannot garbage collect a running go routine, even though that go routine is no longer needed by the program, it is still there, alive and consuming resources.

In our case by the time all 1k requests are served, there would be at least 500 go routines in the runtime while in reality there should've been just 2. These goroutines will be alive until all the 1k requests are served, the `main()` exits and the process is terminated.

In a long running process (like a HTTP server serving millions of requests without stopping) such leaks can accumulate over time exhausting the system resources.

Now that we know the issue, how can we solve it? If there was a way for us to _signal_ to the `getDouble` goroutine that `handleRequest` is exiting and so it should exit too, then our problem is solved.

This is exactly what context cancellation is used for.

In the code below we create a context `ctx` using `WithCancel()` in the `handleRequest()` function and pass it as argument to the `getDouble()` goroutine. Inside `getDouble()` we use a select statement to await on channel operations.

```go
func getDouble(ctx context.Context, i int, ch chan<- int) {
	// doing some processing
	i *= 2
	select {
	case <- ctx.Done(): {
		fmt.Println("getDouble: exiting cleanly")
	}
	case ch <- i : {
		return
	}
	}
}

func handleRequest(i int) {
	// create a context using WithCancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // this will be called whenever handleRequest exits

	ch := make(chan int)

	// pass the context to the goroutine
	go getDouble(ctx, i, ch)

	// simulating a random error
	if i % 2 == 0 {
		return // early exit without reading from the channel
	}

	// read from channel and do some processing
	<- ch
}

func main() {

	// simulating a long running process
	for i := range 1000 {
		handleRequest(i)
		fmt.Printf("Request number %d served\n", i)
		fmt.Printf("Goroutines alive: %d\n", runtime.NumGoroutine())
		time.Sleep(1 * time.Second)
	}

}
```

In the above code, when `handleRequest()` returns, the `cancel()` func associated with the context is automatically called. Due to this, the `done` channel of this context is closed and the `case <- ctx.Done()` case inside `getDouble()` gets triggered and the go routine exits without blocking.

This is how the cancellation signal is used to signal to multiple parts of the code that they should stop doing whatever they're doing and exit immediately.
