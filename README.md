# Microbatch golang implementation

## Main points
1. each job has it's own return value
2. job processing order does not matter

## Implementation details
There is a single goroutine which periodically(interval configurable) check whether jobs are present, if so trigger another goroutine to process those jobs\
Other than that, when submitting a job, it also checks whether the batch size has been reached, if so trigger another goroutine to process the jobs

Other possible approaches \
single go routine to process jobs, which will wait on a golang `select` with two channels,\
first channel will be triggered from submit job if batch size is reached,\
second channel will be triggered from ticker which will periodically trigger according to the configured interval.\
this approach would have helped if we needed jobs to preserve order when processing as it is running in a single go routine\
and there are couple more approaches as well. 

## Build the library 
since this is a library no artifacts will be generated but following command will show if there are compile errors\
run below command to build the library\
`go build .`

## Running tests
use following command to run tests\
`go test . -v`

### Test coverage
1. run following command to generate coverage report\
`go test -coverprofile=coverage.out`
2. run following command to generate userfriendly html version\
`go tool cover -html=coverage.out -o=coverage.html`

## Generating docs
you can use following commands to see the code documentation\
`go doc`\
`go doc --all`

## How to use the library
1. import the library as follows in `go.mod`
`require github.com/lankavitharana/microbatch v1.0.0`
2. Implement the `Job[T any]` interface\
you can refer to the sample implementation in tests `TestJobImpl`, notice the embedded `Result[int]` type, which you need to do as well for 
communicating job result back, here type can be anything you like, even a custom type you have
3. Implement `BatchProcessor[T any]` interface to provide batch processing functionality\
make sure that batch processor will return the result through `resultChannel` once each job is done.
4. Instantiate `MicroBatchProcessor[T any]` using the function `NewMicroBatchProcessor` by providing required parameters\
example as follows
```
microBatch := NewMicroBatchProcessor(3, time.Millisecond*10, TestBatchProcessorImpl{})
```
5. Submit jobs as follows
```
result, err := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})
```
6. You can wait for job results using the result channel as follows
```
jobId := <-result
```
7. Shutdown the micro batch processor as follows
```
err = microBatch.Shutdown()
```

## Building the project inside docker
you can run following command to build the project inside docker\
`docker run --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang:1.23.2 go build -v`\
running tests inside docker container\
`docker run --rm -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang:1.23.2 go test . -v`

