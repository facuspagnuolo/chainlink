package services_test

import (
	"testing"

	"github.com/smartcontractkit/chainlink/internal/cltest"
	"github.com/smartcontractkit/chainlink/services"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

func TestNewRunLogInitiatedRun(t *testing.T) {
	// run log height is too low
	// run log height is just right
	// run log height has been exceeded
	// input amount has not been met
	// input amount has been met
	//

}

func TestNewWebInitiatedRun(t *testing.T) {
	store, cleanup := cltest.NewStore()
	defer cleanup()

	input := models.JSON{Result: gjson.Parse(`{"address":"0xdfcfc2b9200dbb10952c2b7cce60fc7260e03c6f"}`)}
	jobSpec, _ := cltest.NewJobWithWebInitiator()

	// test input is saved to first task result
	run, err := services.NewWebInitiatedRun(jobSpec, models.RunResult{Data: input}, store)
	assert.NoError(t, err)
	assert.Equal(t, run.Status, models.RunStatusInProgress)
	assert.Len(t, run.TaskRuns, 1)
	assert.Equal(t, run.TaskRuns[0].Result.Data, input)

	// test initiator is set to web
	assert.Equal(t, run.Initiator.Type, models.InitiatorWeb)

	// test result of error is set to final run result
	run, err = services.NewWebInitiatedRun(jobSpec, models.RunResult{Data: input, Status: models.RunStatusErrored}, store)
	assert.NoError(t, err)
	assert.Equal(t, run.Status, models.RunStatusErrored)
	assert.Len(t, run.TaskRuns, 1)
	assert.Equal(t, run.TaskRuns[0].Result.JobRunID, run.ID)
	assert.Equal(t, run.TaskRuns[0].Result.Status, models.RunStatusErrored)
	assert.Equal(t, run.Result.Status, models.RunStatusErrored)
}

//func Test

//body.Task.Param

//(body).Perform(input) -----------> newResult
//|
//previousTaskResult         |
//|
///--------------------/
//(body).Perform(input)

//POST -> /runs requestBody={}\ // bridge adapter
//|
//JobRun.Overrides = requestBody
//nextTaskRun.Result = models.RunResult{requestBody} // error
//jobRun.Result = ^^^^^^^ <- // only if errored

//CopyAdapter

//BridgeAdapter -> POST {"id": run.id}
//nextTaskRun.Task.Params.JobRunID = run.ID
