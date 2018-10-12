package services

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/smartcontractkit/chainlink/adapters"
	"github.com/smartcontractkit/chainlink/logger"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/utils"
)

// CreateRunLogInitiatedRun saves a new job run for a RunLog and schedules it in the job runner
func CreateRunLogInitiatedRun(
	job models.JobSpec,
	initiator models.Initiator,
	input models.RunResult,
	store *store.Store,
	creationHeight hexutil.Big,
) (models.JobRun, error) {

	run, err := NewRunLogInitiatedRun(job, initiator, input, store, creationHeight)
	if err != nil {
		return models.JobRun{}, err
	}

	logger.Debugw("New run created from RunLog", []interface{}{
		"run", run.ID,
		"job", run.JobID,
		"input", input.Data,
	}...)

	return run, saveAndTrigger(&run, store)
}

// CreateWebInitiatedRun saves a new job run for a web initator and schedules it in the job runner
func CreateWebInitiatedRun(
	job models.JobSpec,
	input models.RunResult,
	store *store.Store,
) (models.JobRun, error) {

	run, err := NewWebInitiatedRun(job, input, store)
	if err != nil {
		return models.JobRun{}, err
	}

	logger.Debugw("New run created from web request", []interface{}{
		"run", run.ID,
		"job", run.JobID,
		"input", input.Data,
	}...)

	return run, saveAndTrigger(&run, store)
}

// CreateCronInitiatedRun saves a new job run for a cron initiator and schedules it in the job runner
func CreateCronInitiatedRun(
	job models.JobSpec,
	initiator models.Initiator,
	store *store.Store,
) (models.JobRun, error) {
	run, err := BuildRun(job, initiator, store)
	if err != nil {
		return run, err
	}

	logger.Debugw("New run created from cron trigger", []interface{}{
		"run", run.ID,
		"job", run.JobID,
		"initiator", initiator.Type,
	}...)

	run.Overrides.JobRunID = run.ID

	run.Status = models.RunStatusInProgress
	return run, saveAndTrigger(&run, store)
}

// ResumeConfirmingTask resumes a confirming run if the minimum confirmations have been met
func ResumeConfirmingTask(
	run *models.JobRun,
	store *store.Store,
	currentBlockHeight uint64,
) error {

	logger.Debugw("New head resuming run", []interface{}{
		"run", run.ID,
		"job", run.JobID,
		"current_block_height", currentBlockHeight,
	}...)

	if !run.Status.PendingConfirmations() {
		logger.Errorw("ResumeConfirmingTask for non pending task!", run.ForLogger()...)
		return nil
	}

	nextTaskRun := run.NextTaskRun()
	minimumConfirmations, err := RunMinimumConfirmations(run, nextTaskRun, store)
	if err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		run.ApplyResult(nextTaskRun.Result.WithError(err))
		return store.Save(run)
	}

	if !run.Runnable(currentBlockHeight, minimumConfirmations) {
		logger.Debugw("Insufficient confirmations to wake job", []interface{}{
			"run", run.ID,
			"job", run.JobID,
			"observed_height", currentBlockHeight,
			"expected_height", currentBlockHeight + minimumConfirmations,
		}...)
		run.Status = models.RunStatusPendingConfirmations
	} else {
		logger.Debugw("Minimum confirmations met, resuming job", []interface{}{
			"run", run.ID,
			"job", run.JobID,
			"observed_height", currentBlockHeight,
			"expected_height", currentBlockHeight + minimumConfirmations,
		}...)
		run.Status = models.RunStatusInProgress
	}

	return saveAndTrigger(run, store)
}

// ResumePendingTask takes the body provided from an external adapter,
// saves it for the next task to process, then tells the job runner to execute
// it
func ResumePendingTask(
	run *models.JobRun,
	store *store.Store,
	input models.RunResult,
) error {

	logger.Debugw("External adapter resuming job", []interface{}{
		"run", run.ID,
		"job", run.JobID,
		"status", run.Status,
		"input_data", input.Data,
		"input_result", input.Status,
	}...)

	if !run.Status.PendingBridge() {
		return fmt.Errorf("Attempting to resume non pending run %s", run.ID)
	}

	err := SaveInput(run, input)
	if err != nil {
		return store.Save(run)
	}

	if !run.Status.Errored() {
		nextTaskRun := run.NextTaskRun()
		if nextTaskRun != nil {
			run.Status = models.RunStatusInProgress
		} else {
			run.MarkCompleted()
		}
	}

	return saveAndTrigger(run, store)
}

func saveAndTrigger(run *models.JobRun, store *store.Store) error {
	if err := store.Save(run); err != nil {
		return err
	}

	if run.Status == models.RunStatusInProgress {
		logger.Debugw("New run is ready to start, adding to queue", []interface{}{"run", run.ID}...)
		return store.RunChannel.Send(run.ID)
	}

	return nil
}

// NewRunLogInitiatedRun returns a new job run for a RunLog
func NewRunLogInitiatedRun(
	job models.JobSpec,
	initiator models.Initiator,
	input models.RunResult,
	store *store.Store,
	creationHeight hexutil.Big,
) (models.JobRun, error) {
	run, err := BuildRun(job, initiator, store)
	if err != nil {
		return run, err
	}

	if input.Amount != nil {
		paymentValid, err := ValidateMinimumContractPayment(store, job, *input.Amount)

		if err != nil {
			err = fmt.Errorf(
				"Rejecting job %s error validating contract payment: %v",
				job.ID,
				err,
			)
		} else if !paymentValid {
			err = fmt.Errorf(
				"Rejecting job %s with payment %s below minimum threshold (%s)",
				job.ID,
				input.Amount,
				store.Config.MinimumContractPayment.Text(10))
		}

		if err != nil {
			run.ApplyResult(input.WithError(err))
			return run, nil
		}
	}

	err = SaveInput(&run, input)
	if err != nil {
		return run, nil
	}

	run.CreationHeight = &creationHeight

	// If the minimum confirmations for this task are greater than 0, put this
	// job run in pending status confirmations, only the head tracker / run log
	// should wake it up again
	nextTaskRun := run.NextTaskRun()
	minimumConfirmations, err := RunMinimumConfirmations(&run, nextTaskRun, store)
	if err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		run.ApplyResult(input.WithError(err))
		return run, nil
	}

	if minimumConfirmations > 0 {
		run.Status = models.RunStatusPendingConfirmations
	} else {
		run.Status = models.RunStatusInProgress
	}

	return run, nil
}

// RunMinimumConfirmations returns the minimum confirmations for the current task in a run
func RunMinimumConfirmations(run *models.JobRun, taskRun *models.TaskRun, store *store.Store) (uint64, error) {
	adapter, err := adapters.For(taskRun.Task, store)
	if err != nil {
		return 0, err
	}

	return utils.MaxUint64(
		store.Config.MinIncomingConfirmations,
		taskRun.Task.Confirmations,
		adapter.MinConfs()), nil
}

// NewWebInitiatedRun returns a new job run for a web request
func NewWebInitiatedRun(
	job models.JobSpec,
	input models.RunResult,
	store *store.Store,
) (models.JobRun, error) {

	initiator := job.InitiatorsFor(models.InitiatorWeb)[0]
	run, err := BuildRun(job, initiator, store)
	if err != nil {
		return run, err
	}

	err = SaveInput(&run, input)
	if err != nil {
		return run, nil
	}

	run.Status = models.RunStatusInProgress
	return run, nil
}

// BuildRun checks to ensure the given job has not started or ended before
// creating a new run for the job.
func BuildRun(job models.JobSpec, initiator models.Initiator, store *store.Store) (models.JobRun, error) {
	now := store.Clock.Now()
	if !job.Started(now) {
		return models.JobRun{}, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v unstarted: %v before job's start time %v", job.ID, now, job.EndAt),
		}
	}
	if job.Ended(now) {
		return models.JobRun{}, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v ended: %v past job's end time %v", job.ID, now, job.EndAt),
		}
	}
	return job.NewRun(initiator), nil
}

// FIXME: Save name suggests disk write, which I'd like to avoid
// SaveInput stores the input against the run, which means:
// 1) Merging with overrides
// 2) Saving as the input to the next task
// 3) Saving as the result for the run
func SaveInput(run *models.JobRun, input models.RunResult) error {
	run.Overrides.Data = input.Data
	run.Overrides.JobRunID = run.ID

	nextTaskRun := run.NextTaskRun()
	result, err := run.Overrides.Merge(input)
	if err != nil {
		nextTaskRun.ApplyResult(input.WithError(err))
		run.ApplyResult(input.WithError(err))
		return err
	}

	nextTaskRun.ApplyResult(result)
	run.ApplyResult(result)
	return nil
}

// RecurringScheduleJobError contains the field for the error message.
type RecurringScheduleJobError struct {
	msg string
}

// Error returns the error message for the run.
func (err RecurringScheduleJobError) Error() string {
	return err.msg
}
