/*
This is the microbatch package test suite
*/
package microbatch

import (
	"testing"
	"time"
)

// tests return values and see whether they return correctly and processing happens because of batch size
func TestReturnValues(t *testing.T) {
	microBatch := NewMicroBatchProcessor(2, time.Hour*2, TestBatchProcessorImpl{})
	result1, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})
	result2, err2 := microBatch.SubmitJob(&TestJobImpl{jobId: 2, sleepInMillis: 1})

	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}
	if err2 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err2)
	}

	jobId1 := <-result1
	jobId2 := <-result2
	if jobId1 != 101 {
		t.Errorf("Failed, Expected: %d, Actual: %d", jobId1, 101)
	}
	if jobId2 != 102 {
		t.Errorf("Failed, Expected: %d, Actual: %d", jobId2, 102)
	}

	err := microBatch.Shutdown()
	if err != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err)
	}
}

// tests the flow where processing happens because of the interval rather than batch size
func TestBatchProcessingWithInterval(t *testing.T) {
	microBatch := NewMicroBatchProcessor(5, time.Second*1, TestBatchProcessorImpl{})
	result1, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})

	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}

	jobId1 := <-result1
	if jobId1 != 101 {
		t.Errorf("Failed, Actual: %d, Expected: %d", jobId1, 101)
	}

	err := microBatch.Shutdown()
	if err != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err)
	}
}

// tests the flow where jobs are processed after the first scheduled run (in second scheduled run)
func TestSkippingOneScheduleRun(t *testing.T) {
	microBatch := NewMicroBatchProcessor(5, time.Millisecond*10, TestBatchProcessorImpl{})
	time.Sleep(time.Microsecond * 20)
	result1, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})

	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}

	jobId1 := <-result1
	if jobId1 != 101 {
		t.Errorf("Failed, Actual: %d, Expected: %d", jobId1, 101)
	}

	err := microBatch.Shutdown()
	if err != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err)
	}
}

// tests the flow where processing happens because of the batch size rather than interval
func TestSubmitAfterShutdown(t *testing.T) {
	microBatch := NewMicroBatchProcessor(3, time.Second*2, TestBatchProcessorImpl{})
	result1, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})

	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}

	go func() {
		err := microBatch.Shutdown()
		if err1 != nil {
			t.Errorf("Failed, Expected: nil, Actual: %s", err)
		}
	}()
	time.Sleep(time.Millisecond * 30)

	_, err2 := microBatch.SubmitJob(&TestJobImpl{jobId: 2, sleepInMillis: 1})

	if err2 == nil {
		t.Errorf("Failed, Expected: error, Actual: nil")
	}
	jobId1 := <-result1
	if jobId1 != 101 {
		t.Errorf("Failed, Actual: %d, Expected: %d", jobId1, 101)
	}
}

// tests multiple shutdown triggers
func TestMultipleShutdownTriggers(t *testing.T) {
	microBatch := NewMicroBatchProcessor(3, time.Millisecond*10, TestBatchProcessorImpl{})
	result1, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})
	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}

	jobId1 := <-result1
	if jobId1 != 101 {
		t.Errorf("Failed, Actual: %d, Expected: %d", jobId1, 101)
	}

	err := microBatch.Shutdown()
	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err)
	}
	err = microBatch.Shutdown()
	if err == nil {
		t.Errorf("Failed, Expected: error, Actual: nil")
	}
}

// tests not listening to return values
func TestNotListeningToReturnValues(t *testing.T) {
	microBatch := NewMicroBatchProcessor(3, time.Millisecond*10, TestBatchProcessorImpl{})
	_, err1 := microBatch.SubmitJob(&TestJobImpl{jobId: 1, sleepInMillis: 1})
	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err1)
	}
	err := microBatch.Shutdown()
	if err1 != nil {
		t.Errorf("Failed, Expected: nil, Actual: %s", err)
	}
}

// test batch processor implementation
type TestBatchProcessorImpl struct{}

// test job implementation
type TestJobImpl struct {
	Result[int]
	jobId         int
	sleepInMillis int
}

// result channel set method
func (j *TestJobImpl) SetResultChannel(result chan<- int) {
	j.resultChannel = result
}

// result channel get method
func (j *TestJobImpl) GetResultChannel() chan<- int {
	return j.resultChannel
}

// run job method for testing
func (j *TestJobImpl) RunJob() int {
	logger.Info("Before sleep", "Job id", j.jobId)
	time.Sleep(time.Millisecond * time.Duration(j.sleepInMillis))
	logger.Info("After sleep", "Job id", j.jobId)
	return j.jobId + 100
}

// batch processing implementation
func (b TestBatchProcessorImpl) ProcessBatch(jobs []Job[int]) {
	for _, job := range jobs {
		if testJob, ok := job.(*TestJobImpl); ok {
			res := testJob.RunJob()
			job.GetResultChannel() <- res
		}
	}
}
