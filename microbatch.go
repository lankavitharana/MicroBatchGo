/*
This is the microbatch package which provides the basic interfaces required as well as
Micro batch processor implementation
*/
package microbatch

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"
)

// logger to be used for logging
var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	AddSource: true,
}))

// Batch processor interface that is used to process jobs as batches, user needs inject and instance of this to the micro batch processor
// T is the generic type which will be the type of job result
type BatchProcessor[T any] interface {
	// API method which needs to be implemented to process batch jobs
	ProcessBatch(jobs []Job[T])
}

// Job type which is accepted by the batch processor to be processed, here T is the return type of the job result
// and it can be any (generic) type
type Job[T any] interface {
	// Method to set the channel which will send the response back to the caller
	SetResultChannel(result chan<- T)
	// Method to get the channel which will send the response back to the caller
	GetResultChannel() chan<- T
}

// Actual Job implementation can use below struct to embed the result channel to the implementation
// can use generics to define the actual return type they expect as well
type Result[T any] struct {
	// channel which will be used to communicate results back to user
	resultChannel chan<- T
}

// Micro batch processor interface which has a simple API to submit jobs and shutdown the processor
type MicroBatchProcessor[T any] interface {
	// Method to submit a job to the micro batch processor which will return a channel for the result type T
	// which can be used by the submitter to wait for the job result
	// returns error if shutdown triggered
	SubmitJob(job Job[T]) (<-chan T, error)
	// Method to shutdown the system, this will make sure all the accepted jobs will run before shutting down
	// returns error if shutdown was already triggered
	Shutdown() error
}

// Micro batch processor implementation, note the fields are not exposed, only needed for internal package use
type MicroBatchProcessorImpl[T any] struct {
	// provided batch processor instance
	batchProcessor BatchProcessor[T]
	// batch size for which processing events are triggered
	batchSize int
	// lock to make sure job process and submit(and parallel submits) are thread safe
	jobLock sync.Mutex
	// lock to make sure while shutdown is being triggered, no jobs are submitted
	shutdownLock sync.Mutex
	// slice to hold the submitted jobs (could have used an array as well)
	jobs []Job[T]
	// waitlock which will be used to safely shutdown the system
	waitLock sync.WaitGroup
	// periodic scheduler
	scheduledTicker *time.Ticker
	// channel to notify periodic scheduler to shutdown
	scheduleDone chan bool
	// to make sure no jobs can be submitted after shutdown triggered
	shutdownTriggered bool
}

// Submit job implementation which will take the job, put that into the queue(thread safe)
// if batch size is reached, then process current job list
// T here is the job result type
func (m *MicroBatchProcessorImpl[T]) SubmitJob(job Job[T]) (<-chan T, error) {
	logger.Debug("Submit Job")

	// Making the channel non blocking so that even if the user does not read, this does not block
	resultChannel := make(chan T, 1)
	// setting result channel to the job so that batch processor can asynchronously send data when available
	job.SetResultChannel(resultChannel)

	// acquiring shutdown lock to avoid being shutdown while adding a job
	m.shutdownLock.Lock()

	// acquirng lock so that multiple threads can send jobs safely
	m.jobLock.Lock()

	// adding below defer order so that it first unlock shutdown lock and then joblock (reverse order)
	defer m.jobLock.Unlock()      // releasing the lock at the end of the function
	defer m.shutdownLock.Unlock() // releasing shutdown lock at the end of the function

	if m.shutdownTriggered { // this makes sure no new job will be added if shutdown was triggered
		logger.Error("Shutdown already triggered, cannot add new jobs")
		return nil, errors.New("Shutdown triggered, cannot add new jobs")
	}

	m.jobs = append(m.jobs, job) // add new job
	m.waitLock.Add(1)            // increment waitlock so that system knows accepted job count

	if len(m.jobs) >= m.batchSize { // if batch size reached, process jobs using batch processor
		logger.Debug("Job count larger than batch size, processing jobs", "jobs length", len(m.jobs))
		m.processJobs()
	}
	return resultChannel, nil // return result channel to the user to be able to wait on that(or implement async function)
}

// Shutdown method implementation which will wait for all the submitted jobs to be processed
// before shutting down
func (m *MicroBatchProcessorImpl[T]) Shutdown() error {
	logger.Debug("Shutdown triggered")

	m.shutdownLock.Lock() // this makes sure no new jobs can be added while shutdown is triggered
	defer m.shutdownLock.Unlock()

	if m.shutdownTriggered { // this makes sure shutdown trigger won't happen twice
		logger.Error("Shutdown already triggered")
		return errors.New("Shutdown already triggered")
	}

	m.shutdownTriggered = true

	m.waitLock.Wait() // this waits for all the submitted jobs to be processed

	m.scheduledTicker.Stop() // stop the scheduled ticker
	m.scheduleDone <- true   // send signal to shutdown the scheduler go routine

	return nil
}

// This will be triggered by the scheduler periodically, it will simply process jobs if job count in the queue is > 0
func (m *MicroBatchProcessorImpl[T]) processSchedule() {
	m.jobLock.Lock() // thread safe lock
	defer m.jobLock.Unlock()
	if len(m.jobs) > 0 { // process only if there are any jobs to be processed
		logger.Debug("There are jobs to process, hence triggering process", "jobs length", len(m.jobs))
		m.processJobs()
	}
}

// This method will simply copy the jobs in the current queue, reset the current queue and then start
// a go routine to process the copied jobs
func (m *MicroBatchProcessorImpl[T]) processJobs() {
	runnableJobs := make([]Job[T], len(m.jobs))
	copy(runnableJobs, m.jobs)
	m.jobs = make([]Job[T], 0) // reset with zero size so that len only increase when entries are added
	go func() {
		logger.Debug("Processing jobs", "jobs count", len(runnableJobs))
		defer m.waitLock.Add(-len(runnableJobs))    // couting down processed jobs
		m.batchProcessor.ProcessBatch(runnableJobs) // process jobs using batch processor
	}()
}

// This method is to instantiate the micro batch processor instance, it accepts batch size, scheduler inteval and a batch processor implementation
// as parameters,
func NewMicroBatchProcessor[T any](batchSize int, interval time.Duration, batchProcessor BatchProcessor[T]) MicroBatchProcessor[T] {
	logger.Info("Initializing Micro Batch processor", "batchsize", batchSize, "interval", interval)
	done := make(chan bool)            // scheduler done channel
	ticker := time.NewTicker(interval) // new ticker for the scheduler
	microBatch := &MicroBatchProcessorImpl[T]{
		batchSize:         batchSize,
		batchProcessor:    batchProcessor,
		jobs:              make([]Job[T], 0), // initialize with 0 size so that len returns actual size
		scheduledTicker:   ticker,
		scheduleDone:      done,
		shutdownTriggered: false,
	}

	// this is the scheduler go routine, which waits on the ticker for periodic processing and
	// done channel for shutdown signal
	go func() {
		for {
			select {
			case <-done: // shutdown trigger will notify this channel so this goroutine can exit
				logger.Info("Shutting down the scheduler")
				return
			case <-ticker.C: // scheduler will trigger this to periodically check for jobs
				logger.Debug("Scheduler triggered for processing jobs")
				microBatch.processSchedule()
			}
		}
	}()

	return microBatch
}
