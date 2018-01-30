package web_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/h2non/gock"
	. "github.com/onsi/gomega"
	"github.com/smartcontractkit/chainlink/internal/cltest"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/utils"
	"github.com/stretchr/testify/assert"
)

func TestCreateJobSchedulerIntegration(t *testing.T) {
	RegisterTestingT(t)
	t.Parallel()

	app, cleanup := cltest.NewApplication()
	defer cleanup()
	app.Start()

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/scheduler_job.json")

	jobRuns := []models.JobRun{}
	Eventually(func() []models.JobRun {
		app.Store.Where("JobID", j.ID, &jobRuns)
		return jobRuns
	}).Should(cltest.HaveLenAtLeast(1))

	var initr models.Initiator
	app.Store.One("JobID", j.ID, &initr)
	assert.Equal(t, models.InitiatorCron, initr.Type)
	assert.Equal(t, "* * * * *", string(initr.Schedule), "Wrong cron schedule saved")
}

func TestCreateJobIntegration(t *testing.T) {
	RegisterTestingT(t)

	config, _ := cltest.NewConfig()
	cltest.AddPrivateKey(config, "../internal/fixtures/keys/3cb8e3fd9d27e39a5e9e6852b0e96160061fd4ea.json")
	app, cleanup := cltest.NewApplicationWithConfig(config)
	assert.Nil(t, app.Store.KeyStore.Unlock(cltest.Password))
	eth := app.MockEthClient()
	app.Start()
	defer cleanup()

	defer cltest.CloseGock(t)
	gock.EnableNetworking()

	tickerResponse := `{"high": "10744.00", "last": "10583.75", "timestamp": "1512156162", "bid": "10555.13", "vwap": "10097.98", "volume": "17861.33960013", "low": "9370.11", "ask": "10583.00", "open": "9927.29"}`
	gock.New("https://www.bitstamp.net").
		Get("/api/ticker/").
		Reply(200).
		JSON(tickerResponse)

	eth.Register("eth_getTransactionCount", `0x0100`)
	hash, err := utils.StringToHash("0xb7862c896a6ba2711bccc0410184e46d793ea83b3e05470f1d359ea276d16bb5")
	assert.Nil(t, err)
	sentAt := uint64(23456)
	confirmed := sentAt + 1
	safe := confirmed + config.EthMinConfirmations
	eth.Register("eth_blockNumber", utils.Uint64ToHex(sentAt))
	eth.Register("eth_sendRawTransaction", hash)
	eth.Register("eth_blockNumber", utils.Uint64ToHex(confirmed))
	eth.Register("eth_getTransactionReceipt", store.TxReceipt{})
	eth.Register("eth_blockNumber", utils.Uint64ToHex(safe))
	eth.Register("eth_getTransactionReceipt", store.TxReceipt{
		Hash:        hash,
		BlockNumber: confirmed,
	})

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/hello_world_job.json")
	jr := cltest.CreateJobRunViaWeb(t, app, j)

	Eventually(func() string {
		assert.Nil(t, app.Store.One("ID", jr.ID, jr))
		return jr.Status
	}).Should(Equal(models.StatusCompleted))
	assert.Equal(t, tickerResponse, jr.TaskRuns[0].Result.Value())
	assert.Equal(t, "10583.75", jr.TaskRuns[1].Result.Value())
	assert.Equal(t, hash.String(), jr.TaskRuns[3].Result.Value())
	assert.Equal(t, hash.String(), jr.Result.Value())

	assert.True(t, eth.AllCalled())
}

func TestCreateJobWithRunAtIntegration(t *testing.T) {
	RegisterTestingT(t)
	t.Parallel()
	app, cleanup := cltest.NewApplication()
	defer cleanup()
	app.InstantClock()

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/run_at_job.json")

	var initr models.Initiator
	app.Store.One("JobID", j.ID, &initr)
	assert.Equal(t, models.InitiatorRunAt, initr.Type)
	assert.Equal(t, "2018-01-08T18:12:01Z", initr.Time.ISO8601())

	app.Start()
	jobRuns := []models.JobRun{}
	Eventually(func() []models.JobRun {
		app.Store.Where("JobID", j.ID, &jobRuns)
		return jobRuns
	}).Should(HaveLen(1))
}

func TestCreateJobWithEthLogIntegration(t *testing.T) {
	RegisterTestingT(t)
	t.Parallel()
	app, cleanup := cltest.NewApplication()
	defer cleanup()
	eth := app.MockEthClient()

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/eth_log_job.json")
	address, _ := utils.StringToAddress("0x3cCad4715152693fE3BC4460591e3D3Fbd071b42")

	var initr models.Initiator
	app.Store.One("JobID", j.ID, &initr)
	assert.Equal(t, models.InitiatorEthLog, initr.Type)
	assert.Equal(t, address, initr.Address)

	logs := make(chan store.EventLog, 1)
	eth.RegisterSubscription("logs", logs)
	app.Start()

	logs <- store.EventLog{Address: address}

	jobRuns := []models.JobRun{}
	Eventually(func() []models.JobRun {
		app.Store.Where("JobID", j.ID, &jobRuns)
		return jobRuns
	}).Should(HaveLen(1))
}

func TestCreateJobWithEndAtIntegration(t *testing.T) {
	t.Parallel()
	RegisterTestingT(t)

	app, cleanup := cltest.NewApplication()
	defer cleanup()
	clock := cltest.UseSettableClock(app.Store)
	app.Start()

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/end_at_job.json")
	endAt := utils.ParseISO8601("3000-01-01T00:00:00.000Z")
	assert.Equal(t, endAt, j.EndAt.Time)

	cltest.CreateJobRunViaWeb(t, app, j)

	clock.SetTime(endAt.Add(time.Nanosecond))

	url := app.Server.URL + "/v2/jobs/" + j.ID + "/runs"
	resp := cltest.BasicAuthPost(url, "application/json", &bytes.Buffer{})
	assert.Equal(t, 500, resp.StatusCode)
	jobRuns := []models.JobRun{}
	Consistently(func() []models.JobRun {
		app.Store.Where("JobID", j.ID, &jobRuns)
		return jobRuns
	}).Should(HaveLen(1))
}

func TestCreateJobWithStartAtIntegration(t *testing.T) {
	t.Parallel()
	RegisterTestingT(t)

	app, cleanup := cltest.NewApplication()
	defer cleanup()
	clock := cltest.UseSettableClock(app.Store)
	app.Start()

	j := cltest.FixtureCreateJobViaWeb(t, app, "../internal/fixtures/web/start_at_job.json")
	startAt := utils.ParseISO8601("3000-01-01T00:00:00.000Z")
	assert.Equal(t, startAt, j.StartAt.Time)

	url := app.Server.URL + "/v2/jobs/" + j.ID + "/runs"
	resp := cltest.BasicAuthPost(url, "application/json", &bytes.Buffer{})
	assert.Equal(t, 500, resp.StatusCode)
	jobRuns := []models.JobRun{}
	Consistently(func() []models.JobRun {
		app.Store.Where("JobID", j.ID, &jobRuns)
		return jobRuns
	}).Should(HaveLen(0))

	clock.SetTime(startAt)

	cltest.CreateJobRunViaWeb(t, app, j)
}