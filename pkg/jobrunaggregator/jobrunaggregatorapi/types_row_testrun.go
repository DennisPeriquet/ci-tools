package jobrunaggregatorapi

// The jobSchema below is used to build the "Jobs" table.
//
const (
	TestRunTableName = "TestRuns"
	TestRunSchema    = `
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
