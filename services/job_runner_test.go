package services_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/smartcontractkit/chainlink/adapters"
	"github.com/smartcontractkit/chainlink/internal/cltest"
	"github.com/smartcontractkit/chainlink/services"
	"github.com/smartcontractkit/chainlink/store/assets"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	null "gopkg.in/guregu/null.v3"
)

func TestJobRunner_resumeSleepingRuns(t *testing.T) {
	store, cleanup := cltest.NewStore()
	defer cleanup()
	rm, cleanup := cltest.NewJobRunner(store)
	defer cleanup()

	j := models.NewJob()
	i := models.Initiator{Type: models.InitiatorWeb}
	j.Initiators = []models.Initiator{i}
	json := fmt.Sprintf(`{"until":"%v"}`, utils.ISO8601UTC(time.Now().Add(time.Second)))
	j.Tasks = []models.TaskSpec{cltest.NewTask("sleep", json)}
	assert.NoError(t, store.Save(&j))

	jr := j.NewRun(i)
	jr.Status = models.RunStatusPendingSleep
	jr.Result.Data = cltest.JSONFromString(`{"foo":"bar"}`)
	assert.NoError(t, store.Save(&jr))

	assert.NoError(t, services.ExportedResumeSleepingRuns(rm))
	rr, open := <-store.RunChannel.Receive()
	assert.Equal(t, jr.ID, rr.ID)
	assert.True(t, open)
}

func TestJobRunner_ChannelForRun_equalityBetweenRuns(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore()
	defer cleanup()
	rm, cleanup := cltest.NewJobRunner(store)
	defer cleanup()

	job, initr := cltest.NewJobWithWebInitiator()
	run1 := job.NewRun(initr)
	run2 := job.NewRun(initr)

	chan1a := services.ExportedChannelForRun(rm, run1.ID)
	chan2 := services.ExportedChannelForRun(rm, run2.ID)
	chan1b := services.ExportedChannelForRun(rm, run1.ID)

	assert.NotEqual(t, chan1a, chan2)
	assert.Equal(t, chan1a, chan1b)
	assert.NotEqual(t, chan2, chan1b)
}

func TestJobRunner_ChannelForRun_sendAfterClosing(t *testing.T) {
	t.Parallel()

	s, cleanup := cltest.NewStore()
	defer cleanup()
	rm, cleanup := cltest.NewJobRunner(s)
	defer cleanup()
	assert.NoError(t, rm.Start())

	j, initr := cltest.NewJobWithWebInitiator()
	assert.NoError(t, s.SaveJob(&j))
	jr := j.NewRun(initr)
	assert.NoError(t, s.Save(&jr))

	chan1 := services.ExportedChannelForRun(rm, jr.ID)
	chan1 <- struct{}{}
	cltest.WaitForJobRunToComplete(t, s, jr)

	gomega.NewGomegaWithT(t).Eventually(func() chan<- struct{} {
		return services.ExportedChannelForRun(rm, jr.ID)
	}).Should(gomega.Not(gomega.Equal(chan1))) // eventually deletes the channel

	chan2 := services.ExportedChannelForRun(rm, jr.ID)
	chan2 <- struct{}{} // does not panic
}

func TestJobRunner_ChannelForRun_equalityWithoutClosing(t *testing.T) {
	t.Parallel()

	s, cleanup := cltest.NewStore()
	defer cleanup()
	rm, cleanup := cltest.NewJobRunner(s)
	defer cleanup()
	assert.NoError(t, rm.Start())

	j, initr := cltest.NewJobWithWebInitiator()
	j.Tasks = []models.TaskSpec{cltest.NewTask("nooppend")}
	assert.NoError(t, s.SaveJob(&j))
	jr := j.NewRun(initr)
	assert.NoError(t, s.Save(&jr))

	chan1 := services.ExportedChannelForRun(rm, jr.ID)

	chan1 <- struct{}{}
	cltest.WaitForJobRunToPendConfirmations(t, s, jr)

	chan2 := services.ExportedChannelForRun(rm, jr.ID)
	assert.Equal(t, chan1, chan2)
}

func TestJobRunner_Stop(t *testing.T) {
	t.Parallel()

	s, cleanup := cltest.NewStore()
	defer cleanup()
	rm, cleanup := cltest.NewJobRunner(s)
	defer cleanup()
	j, initr := cltest.NewJobWithWebInitiator()
	jr := j.NewRun(initr)

	require.NoError(t, rm.Start())

	services.ExportedChannelForRun(rm, jr.ID)
	assert.Equal(t, 1, services.ExportedWorkerCount(rm))

	rm.Stop()

	gomega.NewGomegaWithT(t).Eventually(func() int {
		return services.ExportedWorkerCount(rm)
	}).Should(gomega.Equal(0))
}

func TestJobRunner_transitionToPendingConfirmations(t *testing.T) {
	t.Parallel()

	config, cfgCleanup := cltest.NewConfig()
	defer cfgCleanup()
	config.MinIncomingConfirmations = 10

	store, cleanup := cltest.NewStoreWithConfig(config)
	defer cleanup()
	jobRunner, cleanup := cltest.NewJobRunner(store)
	defer cleanup()
	jobRunner.Start()

	creationHeightNum := uint64(1000)
	creationHeight := cltest.IndexableBlockNumber(creationHeightNum).Number
	initialData := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}
	configMin := store.Config.MinIncomingConfirmations

	tests := []struct {
		name           string
		confirmations  uint64
		triggeringConf uint64
	}{
		{"not defined in task spec", 0, configMin},
		{"task spec > global min confs", configMin + 1, configMin + 1},
		{"task spec == global min confs", configMin, configMin},
		{"task spec < global min confs", configMin - 1, configMin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, initr := cltest.NewJobWithLogInitiator()
			job.Tasks = []models.TaskSpec{
				{
					Type:          adapters.TaskTypeNoOp,
					Confirmations: uint64(test.confirmations),
				},
			}

			run, err := services.NewRunLogInitiatedRun(job, initr, models.RunResult{Data: initialData}, store, creationHeight)
			assert.NoError(t, err)
			assert.NoError(t, store.Save(&run))

			early := creationHeightNum + test.triggeringConf - 2
			services.ResumeConfirmingTask(&run, store, early)

			cltest.WaitForJobRunStatus(t, store, run, models.RunStatusPendingConfirmations)
			store.One("ID", run.ID, &run)
			assert.Equal(t, initialData, run.Result.Data)

			trigger := creationHeightNum + test.triggeringConf - 1
			services.ResumeConfirmingTask(&run, store, trigger)
			cltest.WaitForJobRunStatus(t, store, run, models.RunStatusCompleted)
			store.One("ID", run.ID, &run)
			assert.Equal(t, initialData, run.Result.Data)
		})
	}
}

func TestJobRunner_transitionToPendingConfirmationsWithBridgeTask(t *testing.T) {
	t.Parallel()

	config, cfgCleanup := cltest.NewConfig()
	defer cfgCleanup()
	config.MinIncomingConfirmations = 10
	store, cleanup := cltest.NewStoreWithConfig(config)
	defer cleanup()
	jobRunner, cleanup := cltest.NewJobRunner(store)
	defer cleanup()
	jobRunner.Start()
	creationHeightNum := uint64(1000)
	creationHeight := cltest.IndexableBlockNumber(creationHeightNum).Number
	configMin := store.Config.MinIncomingConfirmations

	tests := []struct {
		name                    string
		bridgeTypeConfirmations uint64
		taskSpecConfirmations   uint64
		triggeringConf          uint64
	}{
		{"not defined in task spec or bridge type", 0, 0, configMin},
		{"bridge type confirmations > task spec confirmations", configMin + 1, configMin, configMin + 1},
		{"bridge type confirmations = task spec confirmations", configMin, configMin, configMin},
		{"bridge type confirmations < task spec confirmations", configMin - 2, configMin - 1, configMin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, initr := cltest.NewJobWithLogInitiator()
			job.Tasks = []models.TaskSpec{
				{
					Type:          models.MustNewTaskType("randomNumber"),
					Confirmations: uint64(test.taskSpecConfirmations),
				},
			}

			bt := cltest.NewBridgeTypeWithConfirmations(uint64(test.bridgeTypeConfirmations), "randomNumber")
			assert.Nil(t, store.Save(&bt))

			run, err := services.NewRunLogInitiatedRun(job, initr, models.RunResult{}, store, creationHeight)
			assert.NoError(t, err)
			assert.Nil(t, store.Save(&run))

			mockServer, _ := cltest.NewHTTPMockServer(t, 200, "POST", "{\"todo\": \"todo\"}",
				func(_ http.Header, b string) {
					body := cltest.JSONFromString(b)

					id := body.Get("id")
					assert.True(t, id.Exists())
					assert.Equal(t, run.ID, id.String())

					data := body.Get("data")
					assert.True(t, data.Exists())
					assert.Equal(t, data.Type, gjson.JSON)
				},
			)

			bt.URL = cltest.WebURL(mockServer.URL)
			assert.Nil(t, store.Save(&bt))

			early := creationHeightNum + test.triggeringConf - 2
			services.ResumeConfirmingTask(&run, store, early)
			assert.NoError(t, store.Save(&run))

			cltest.WaitForJobRunStatus(t, store, run, models.RunStatusPendingConfirmations)

			trigger := creationHeightNum + test.triggeringConf - 1
			services.ResumeConfirmingTask(&run, store, trigger)
			assert.NoError(t, store.Save(&run))
			cltest.WaitForJobRunStatus(t, store, run, models.RunStatusCompleted)
		})
	}
}

func TestJobRunner_transitionToPending(t *testing.T) {
	t.Parallel()

	store, cleanup := cltest.NewStore()
	defer cleanup()
	jobRunner, cleanup := cltest.NewJobRunner(store)
	defer cleanup()
	jobRunner.Start()

	job, initr := cltest.NewJobWithWebInitiator()
	job.Tasks = []models.TaskSpec{cltest.NewTask("NoOpPend")}

	run := job.NewRun(initr)
	assert.NoError(t, store.Save(&run))

	store.RunChannel.Send(run.ID)
	cltest.WaitForJobRunStatus(t, store, run, models.RunStatusPendingConfirmations)
}

func TestJobRunner_BuildRunWithPayments(t *testing.T) {
	config, cfgCleanup := cltest.NewConfig()
	defer cfgCleanup()
	config.MinimumContractPayment = *assets.NewLink(10)

	store, cleanup := cltest.NewStoreWithConfig(config)
	defer cleanup()

	tests := []struct {
		name       string
		amount     *assets.Link
		invalid    bool
		wantStatus models.RunStatus
	}{
		{"job with insufficient amount", assets.NewLink(9), true, models.RunStatusErrored},
		{"job with no amount", nil, false, models.RunStatusInProgress},
		{"job with exact amount", assets.NewLink(10), false, models.RunStatusInProgress},
		{"job with valid amount", assets.NewLink(11), false, models.RunStatusInProgress},
	}

	head := cltest.BigHexInt(0)
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			job, initr := cltest.NewJobWithWebInitiator()
			job.Tasks = []models.TaskSpec{
				cltest.NewTask("ethtx"),
			}
			assert.Nil(t, store.SaveJob(&job))

			runResult := models.RunResult{
				Amount: test.amount,
			}
			run, _ := services.NewRunLogInitiatedRun(job, initr, runResult, store, head)
			assert.Equal(t, test.wantStatus, run.Status)
		})
	}
}

func TestJobRunner_BuildRun(t *testing.T) {
	pastTime := cltest.ParseNullableTime("2000-01-01T00:00:00.000Z")
	futureTime := cltest.ParseNullableTime("3000-01-01T00:00:00.000Z")
	nullTime := null.Time{Valid: false}

	tests := []struct {
		name    string
		startAt null.Time
		endAt   null.Time
		errored bool
	}{
		{"job not started", futureTime, nullTime, true},
		{"job started", pastTime, futureTime, false},
		{"job with no time range", nullTime, nullTime, false},
		{"job ended", nullTime, pastTime, true},
	}

	store, cleanup := cltest.NewStore()
	defer cleanup()
	clock := cltest.UseSettableClock(store)
	clock.SetTime(time.Now())

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			job, initr := cltest.NewJobWithWebInitiator()
			job.StartAt = test.startAt
			job.EndAt = test.endAt
			assert.Nil(t, store.SaveJob(&job))

			_, err := services.BuildRun(job, initr, store)
			if test.errored {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
