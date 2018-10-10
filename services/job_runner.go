package services

import (
	"errors"
	"fmt"
	"sync"

	"github.com/smartcontractkit/chainlink/adapters"
	"github.com/smartcontractkit/chainlink/logger"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/utils"
)

// JobRunner safely handles coordinating job runs.
type JobRunner interface {
	Start() error
	Stop()
	resumeSleepingRuns() error
	channelForRun(string) chan<- struct{}
	workerCount() int
}

type jobRunner struct {
	started              bool
	done                 chan struct{}
	bootMutex            sync.Mutex
	store                *store.Store
	workerMutex          sync.RWMutex
	workers              map[string]chan struct{}
	workersWg            sync.WaitGroup
	demultiplexStopperWg sync.WaitGroup
}

// NewJobRunner initializes a JobRunner.
func NewJobRunner(str *store.Store) JobRunner {
	return &jobRunner{
		store:   str,
		workers: make(map[string]chan struct{}),
	}
}

// Start reinitializes runs and starts the execution of the store's runs.
func (rm *jobRunner) Start() error {
	rm.bootMutex.Lock()
	defer rm.bootMutex.Unlock()

	if rm.started {
		return errors.New("JobRunner already started")
	}
	rm.done = make(chan struct{})
	rm.started = true

	var starterWg sync.WaitGroup
	starterWg.Add(1)
	go rm.demultiplexRuns(&starterWg)
	starterWg.Wait()

	rm.demultiplexStopperWg.Add(1)
	return rm.resumeSleepingRuns()
}

// Stop closes all open worker channels.
func (rm *jobRunner) Stop() {
	rm.bootMutex.Lock()
	defer rm.bootMutex.Unlock()

	if !rm.started {
		return
	}
	close(rm.done)
	rm.started = false
	rm.demultiplexStopperWg.Wait()
}

func (rm *jobRunner) resumeSleepingRuns() error {
	pendingRuns, err := rm.store.JobRunsWithStatus(models.RunStatusPendingSleep)
	if err != nil {
		return err
	}
	for _, run := range pendingRuns {
		rm.store.RunChannel.Send(run.ID)
	}
	return nil
}

func (rm *jobRunner) demultiplexRuns(starterWg *sync.WaitGroup) {
	starterWg.Done()
	defer rm.demultiplexStopperWg.Done()
	for {
		select {
		case <-rm.done:
			logger.Debug("JobRunner demultiplexing of job runs finished")
			rm.workersWg.Wait()
			return
		case rr, ok := <-rm.store.RunChannel.Receive():
			if !ok {
				logger.Panic("RunChannel closed before JobRunner, can no longer demultiplexing job runs")
				return
			}
			rm.channelForRun(rr.ID) <- struct{}{}
		}
	}
}

func (rm *jobRunner) channelForRun(runID string) chan<- struct{} {
	rm.workerMutex.Lock()
	defer rm.workerMutex.Unlock()

	workerChannel, present := rm.workers[runID]
	if !present {
		workerChannel = make(chan struct{}, 1000)
		rm.workers[runID] = workerChannel
		rm.workersWg.Add(1)

		go func() {
			rm.workerLoop(runID, workerChannel)

			rm.workerMutex.Lock()
			delete(rm.workers, runID)
			rm.workersWg.Done()
			rm.workerMutex.Unlock()

			logger.Debug("Worker finished for ", runID)
		}()
	}
	return workerChannel
}

func (rm *jobRunner) workerLoop(runID string, workerChannel chan struct{}) {
	for {
		select {
		case <-workerChannel:
			run, err := rm.store.FindJobRun(runID)
			if err != nil {
				logger.Errorw(fmt.Sprint("Error finding run ", runID), run.ForLogger("error", err)...)
			}

			if err = executeRun(&run, rm.store); err != nil {
				logger.Errorw(fmt.Sprint("Error executing run ", runID), run.ForLogger("error", err)...)
				return
			}

			if run.Status.Finished() {
				logger.Infow("All tasks complete for run", []interface{}{"run", run.ID}...)
				return
			} else if run.Status.Runnable() {
				logger.Infow("Adding next task to job run queue", []interface{}{"run", run.ID}...)
				rm.store.RunChannel.Send(run.ID)
			}

		case <-rm.done:
			logger.Debug("JobRunner worker loop for ", runID, " finished")
			return
		}
	}
}

func (rm *jobRunner) workerCount() int {
	rm.workerMutex.RLock()
	defer rm.workerMutex.RUnlock()

	return len(rm.workers)
}

func executeTask(nextTaskRun *models.TaskRun, store *store.Store) {
	logger.Debugw("Looking up adapter", []interface{}{"task", nextTaskRun.ID, "params", nextTaskRun.Task.Params}...)

	adapter, err := adapters.For(nextTaskRun.Task, store)
	if err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		return
	}

	logger.Debugw("Executing task", []interface{}{"task", nextTaskRun.ID, "adapter", nextTaskRun.Task.Type}...)

	result := adapter.Perform(nextTaskRun.Result, store)

	logger.Debugw("Task execution complete", []interface{}{
		"task", nextTaskRun.ID,
		"result", result.Error(),
		"response", result.Data,
		"status", result.Status,
	}...)

	nextTaskRun.ApplyResult(result)

	if nextTaskRun.Status.Runnable() {
		logger.Debugw("Marking task as completed", []interface{}{"task", nextTaskRun.ID}...)
		nextTaskRun.MarkCompleted()
	}

	logger.Debugw("Task result", []interface{}{
		"result", nextTaskRun.Result.Error(),
		"data", nextTaskRun.Result.Data,
	}...)
}

func executeRun(run *models.JobRun, store *store.Store) error {
	logger.Infow("Executing run", run.ForLogger()...)

	if !run.Status.Runnable() {
		return fmt.Errorf("Run triggered in non runnable state %s", run.Status)
	}

	if !run.TasksRemain() {
		return errors.New("Run triggered with no remaining tasks")
	}

	nextTaskRun := run.NextTaskRun()

	var err error

	// FIXME: This feels a bit out of place, it's expected by the
	// TestIntegration_ExternalAdapter_Copy test which seems to suggest that the
	// external adapter result should be saved to the params, but that's like
	// everything in one big bundle?
	if nextTaskRun.Task.Params, err = run.Overrides.Data.Merge(nextTaskRun.Task.Params); err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		run.ApplyResult(nextTaskRun.Result)
		return store.Save(run)
	}

	executeTask(nextTaskRun, store)

	if !nextTaskRun.Status.Runnable() {
		// The task failed or is pending, mark the run likewise
		logger.Debugw("Task execution blocked", []interface{}{"run", run.ID, "task", nextTaskRun.ID, "state", nextTaskRun.Result.Status}...)
		run.ApplyResult(nextTaskRun.Result)
	} else {
		// Task succeeded, are there any more tasks to process?
		if run.TasksRemain() {
			futureRun := run.NextTaskRun()

			futureRun.Result.Data = nextTaskRun.Result.Data

			adapter, err := adapters.For(futureRun.Task, store)
			if err != nil {
				futureRun.ApplyResult(futureRun.Result.WithError(err))
				return store.Save(run)
			}

			// If the minimum confirmations for this task are greater than 0, put this
			// job run in pending status confirmations, only the head tracker / run log
			// should wake it up again
			minConfs := utils.MaxUint64(
				store.Config.MinIncomingConfirmations,
				futureRun.Task.Confirmations,
				adapter.MinConfs())
			if minConfs > 0 {
				run.Status = models.RunStatusPendingConfirmations
			}

			logger.Debugw("Future task run", []interface{}{
				"task", futureRun.ID,
				"params", futureRun.Result.Data,
				"minimum_confirmations", minConfs,
			}...)
		} else {
			logger.Debugw("All tasks completed, marking job as completed", []interface{}{"run", run.ID, "task", nextTaskRun.ID}...)
			run.ApplyResult(nextTaskRun.Result)
			run.MarkCompleted()
		}
	}

	if err = store.Save(run); err != nil {
		return err
	}
	logger.Infow("Run finished executing", run.ForLogger()...)

	return nil
}
