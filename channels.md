Channels in Go are a communication mechanism through which different go routines can communicate.

One common way of thinking about channels is imagining them as pipelines connecting the go routines.

The key characteristic of a channel is : if a channel is full, it blocks (i.e. cannot take any more values in) until a value is taken out of it.

## Unbuffered channels :

Unbuffered channels are channels with size 0.

```go
func main() {
	var ch1 chan int // create a channel variable
	ch := make(chan int) // create a channel with an instance
	ch <- 25 // put a value into channel
	val := <- ch // receive a value from channel
	close(ch) // close a channel
}
```

In the above code we're creating an unbuffered channel (channel with size 0) that can store values of type `int`.

If you run the above code, however, it will fail with a deadlock error.

Why's that the case? To understand this, let's understand how unbuffered channels behave.

I said earlier that: if a channel is full it blocks until a value is taken out of it.

An unbuffered channel is of size 0. You can think of it as a pipe with length 0 (essentially just a ring). This means it does not have any capacity to store even a single value.

This implies that you can only put a value through this ring if there's someone (i.e. another go routine) on the other side to receive the value (otherwise the value will be lost). Until there's no receiver the sending operation is blocked.

The opposite of this also true. If there's a go routine trying to read a value from the channel it will be blocked until there is someone putting in a value in the same channel.

This results in synchronous behaviour of unbuffered channel: Sender and receiver are synchronized (the sender blocks until the receiver is ready and vice versa).

In the above example both the sending and receiving is being done within the same go routine. The code gets blocked at the line where we're trying to put the value in the channel (`ch <- 25`) and never reaches the next line where the receiving is being done. Even if you switch the lines, the code will block at the receiving line and never reach the next line.
Hence the deadlock.

Unbuffered channels work only when there's at least 2 go routines with one sending a value into the channel and another ready to receive it. Unbuffered channels guarantee that a send operation will not complete until there is a corresponding receive operation.

```go

// <-chan indicates that ch is meant only for receiving
go consumer(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	data := <-ch
	fmt.Println("Received:", data)
}

// chan<- indicates that ch is meant only for sending
go producer(ch chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()

	ch <- 25
}

func main() {
	ch := make(chan string)
	var wg sync.WaitGroup
	wg.Add(2)

	go consumer(ch, &wg)

	go producer(ch, &wg)

	wg.Wait()
	close(ch)
}
```

The above will work perfectly because two separate go routines are sending and receiving from the channel respectively.

We can use this synchronous behaviour to synchronise execution across go routines

```go
go worker(ch chan<- int) {
	defer wg.Done()

	ch <- 25
}

func main() {
	ch := make(chan string)

	go worker(ch)

	data := <- ch
	close(ch)
}
```

In this the code execution in the `main` go routine will be blocked (at the line `data := <- ch`) till the `worker` go routine is done putting the value into the channel. Hence the two goroutines are synchronized without even using any `WaitGroup`.

## Buffered channels:

Buffered channels are channels with non-zero size. They act as a FIFO queue.

```go
func main() {
	ch := make(chan string, 2); // creating a channel with size 2

	// sending in 2 values into the channel:
	ch <- 1
	ch <- 2

	// receiving 2 values from the channels
	data := <- ch
	data = <- ch
}
```

```go
func main() {
	ch := make(chan int, 2);

	ch <- 1
	ch <- 2
	ch <- 3 // execution will block here b'cz ch is full. Deadlock will happen
	data := <- ch
	data = <- ch
}
```

## Closing channels

You can close a channel after you're done sending values into it:

```go
func main() {
	ch := make(chan int, 2)

	ch <- 1
	ch <- 2

	close(ch) // closing a channel
}
```

You might ask, why would you even want to close channels ? Well closing a channel is not always necessary. It is generally done when you want to signal the receiver that no more values will be sent into the channel and it can stop receiving. You'll better understand this use case in the next section when we'll discuss ranging over channels.

Next question: what happens if you try sending values into a closed channel? It will cause the program to exit with panic.

```go
func main() {
	ch := make(chan int, 2)

	ch <- 1

	close(ch) // closing a channel

	ch <- 3 // sending value into channel after it is closed -> error

}
```

Here's the next important question: what happens if you try to receive from a closed channel ?
One might expect that it might also lead to some sort of an error. But that is not the case! In face it is an expected use case: the sender can close the channel after it is done sending values while the receiver can still independently receive the values and can stop receiving once it's done.

```go
func main() {
	ch := make(chan int, 2)

	ch <- 1
	ch <- 2

	close(ch) // closing a channel

	val := <- ch
	fmt.Println(val) // 1
}
```

What if the receiver is still trying to receive from the channel after all the buffered values have been received and the channel becomes empty? One might expect the receiver to block, because there's no one sending more values into the channel. That would've been true if we would not have closed the channel.

But because we have closed the channel, the receiver simply keeps receiving zero value (0, "", `nil`) indefinitely _without blocking_.

```go
func main() {
	ch := make(chan int, 2)

	ch <- 1
	ch <- 2

	close(ch) // channel closed

	val := <- ch
	fmt.Println(val) // 1

	val = <- ch
	fmt.Println(val) // 2

	// all the values have been received

	val = <- ch
	fmt.Println(val) // 0 (getting the zero value)
}
```

Note that receiver starts receiving these zero values automatically without anyone having to send them into the channel.

This is a very important feature of go: Receiving from an **open**, empty channel blocks (until someone sends a value), but receiving from a **closed**, empty channel immediately returns the zero value of that type without ever blocking.

We can also receive a second parameter alongside the value to check if a channel has been closed. This second parameter will be true as long as the buffered values are being received from the channel and will become false after that. We can put a check in our receiver to stop receiving when this second parameter becomes false.

```go
func main() {
	ch := make(chan int, 2)

	ch <- 1
	ch <- 2

	close(ch) // closing a channel

	val, ok := <- ch
	fmt.Println(val, ok) // 1 true

	val, ok = <- ch
	fmt.Println(val, ok) // 2 true

	// all the values have been received

	val, ok = <- ch
	fmt.Println(val, ok) // 0 false (getting the zero value)
}
```

One final point: As a good practice, only the sender should close a channel, never the receiver. Because there's no way for a receiver to reliably know if all the senders sending into a channel are done sending. If the receiver closes a channel and then a sender sends into it, it will cause panic.

## Ranging over channels

```go
func main() {
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2

	for elem := range ch {
		fmt.Println(elem)
	}
}
```

Doing `elem := range ch` receives the value from the channel. The above code will fail with a deadlock, though. Can you guess why? Because after the buffered values (1 and 2) are received, we're blocked at `elem := range ch` trying to receive more values.

To solve this we'll close the channel after we're done sending the values:

```go
func main() {
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2
	close(ch)

	for elem := range ch {
		fmt.Println(elem) // will print 1, 2 and then loop will exit
	}
}
```

The above code works because `range` loop knows that the channel is closed. We can still safely read values from the channel as discussed in the previous section. After the two buffered values have been received, the `range` loop internally applies the second parameter check to verify if there are more values. That parameter must've become false, and the loop exits.

This is the importance of closing a channel: The sender needs to signal the receiver that no more values will be sent into the channel (by closing it), especially when the receiver is running a loop to receive values from the channel.

## Select

`select` lets us wait on multiple channel operations.

`select` blocks until one of the cases inside it gets executed. After executing one case, it exits. Following summarises the behaviour in 3 scenarios:

- no case is ready: blocks until at least one case is ready. Note if there's a default block it will run immediately if no case is ready
- one case is ready : runs that case and exits
- multiple cases are ready: randomly picks one case, runs it and exits

```go
func main() {
	// create 2 channels:
	ch1 := make(chan string)
	ch2 := make(chan string)

	// First goroutine
	go func() {
		time.Sleep(2 * time.Second)
		ch1 <- "Message from ch1"
	}()

	// Second goroutine
	go func() {
		time.Sleep(1 * time.Second)
		ch2 <- "Message from ch2"
	}()

	select {
		case msg := <-ch1:
			fmt.Println("Received:", msg)
		case msg := <-ch2:
			fmt.Println("Received:", msg) // this will be printed
	}
}
```

In the above example we `select` across 2 channels. The `select` awaits both of the values (from `ch1` and `ch2`) simultaneously and is blocked till at least one of the values is received. Since value from `ch2` is received first (as 2nd go routine sleeps for a shorter time), that block will be executed and the program will exit.

If we want to execute all the blocks, we can have `select` await for the values in a for loop:

```go
func main() {
	// create 2 channels:
	ch1 := make(chan string)
	ch2 := make(chan string)

	// First goroutine
	go func() {
		time.Sleep(2 * time.Second)
		ch1 <- "Message from ch1"
	}()

	// Second goroutine
	go func() {
		time.Sleep(1 * time.Second)
		ch2 <- "Message from ch2"
	}()

	for range 2 {
		select {
			case msg := <-ch1:
				fmt.Println("Received:", msg)
			case msg := <-ch2:
				fmt.Println("Received:", msg)
		}
	}
}
```

In this, the loop runs for the first time and the `select` awaits for values from both the channels. Value from `ch2` will be received first and that block will get executed. Then the loop runs for the second time and `select` again awaits for values from both the channels. Now since `ch2` does not have any more values, that case will be forever blocked. So whenever a value is received from `ch1`, that block gets executed. Then the loop exits.

In this both the print statements will be executed.

From the examples we just saw, we can derive a good use case for `select` : ranging over multiple channels simultaneously. We learned how we can range over a single channel in the previous section. Here's how we can range over multiple channels:

```go
func main() {
	// create 2 channels:
	ch1 := make(chan string)
	ch2 := make(chan string)

	// First goroutine
	go func() {
		for {
			time.Sleep(1 * time.Second)
			ch1 <- "Tick from ch1"
		}
	}()

	// Second goroutine
	go func() {
		for {
			time.Sleep(2 * time.Second)
			ch2 <- "Tick from ch2"
		}
	}()

	// select inside an infinite loop
	for {
		select {
			case msg := <-ch1:
				fmt.Println("Received:", msg)
			case msg := <-ch2:
				fmt.Println("Received:", msg)
		}
	}
}
```

In this we are running `select` inside an infinite loop awaiting values from both the channels and printing them as they're received.

Note that this code works because both the go routines are sending values into the channels indefinitely. If they'll stop sending values after a while, then our `select` will be forever blocked since it is in an infinite loop. In that case we'll have to make our `select` non-blocking by specifying a default case.

We can implement non-blocking receives and sends by specifying a `default` block.

Another use case: implementing timeouts with `select`
