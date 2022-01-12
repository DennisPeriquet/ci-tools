package jobrunaggregatorapi

import (
	"strings"

	"cloud.google.com/go/bigquery"

	prowv1 "k8s.io/test-infra/prow/apis/prowjobs/v1"

	"github.com/openshift/ci-tools/pkg/junit"
)

const (
	TestRunTableName = "TestRuns"

	// The TestRunsSchema below is used to build the "TestRuns" table.
	//
	TestRunsSchema = `
[
  {
    "mode": "REQUIRED",
    "name": "Name",
    "description" : "Name of the test run",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "JobRunName",
    "description" : "Name of the JobRun (big number) that ran this test (e.g., 1389486541524439040)",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "JobName",
    "description" : "Name of the Job that as this test in it",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "Status",
    "description" : "Status of the test (e.g., pass, fail)",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "TestSuite",
    "description" : "Testsuite that this test belongs to",
    "type": "STRING"
  }
]
`
)

// Move here from jobrunbigqueryloader/types.go
//
type TestRunRow struct {
	ProwJob    *prowv1.ProwJob
	JobRun     JobRunInfo
	TestSuites []string
	TestCase   *junit.TestCase
}

// Ensure (at compile time) that testRunRow implements the bigquery.ValueSaver interface
var _ bigquery.ValueSaver = &TestRunRow{}

func (v *TestRunRow) Save() (map[string]bigquery.Value, string, error) {

	// the linter requires not setting a default value. This seems strictly worse and more error-prone to me, but
	// I am a slave to the bot.
	//status := "Unknown"
	var status string
	switch {
	case v.TestCase.FailureOutput != nil:
		status = "Failed"
	case v.TestCase.SkipMessage != nil:
		status = "Skipped"
	default:
		status = "Passed"
	}

	var testSuiteStr string
	if len(v.TestSuites) == 1 {
		testSuiteStr = v.TestSuites[0]
	} else {
		// There is generally a single test suite.  But if there are more, we will
		// concat them to make a unique test suite name.
		testSuiteStr = strings.Join(v.TestSuites, "|||")
	}
	row := map[string]bigquery.Value{
		"Name":       v.TestCase.Name,
		"JobRunName": v.JobRun.GetJobRunID(),
		"JobName":    v.JobRun.GetJobName(),
		"Status":     status,
		"TestSuite":  testSuiteStr,
	}

	return row, "", nil
}
