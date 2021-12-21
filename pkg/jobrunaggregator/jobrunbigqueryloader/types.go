package jobrunbigqueryloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"

	prowv1 "k8s.io/test-infra/prow/apis/prowjobs/v1"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
	"github.com/openshift/ci-tools/pkg/junit"
)

type BigQueryInserter interface {

	// dryRunInserter implements its own "Put" method below for writing to stdout only.
	// Otherwise, the "Put" method in the bigquery package is called (which writes to Big Query).
	//
	Put(ctx context.Context, src interface{}) (err error)
}

type dryRunInserter struct {
	table string
	out   io.Writer
}

func NewDryRunInserter(out io.Writer, table string) BigQueryInserter {
	return dryRunInserter{
		table: table,
		out:   out,
	}
}

// For dry run mode, we use a "Put" method that writes to stdout only.
func (d dryRunInserter) Put(ctx context.Context, src interface{}) (err error) {
	srcVal := reflect.ValueOf(src)

	// single INSERT
	if srcVal.Kind() != reflect.Slice {
		fmt.Fprintf(d.out, "INSERT into %v: %+v\n", d.table, src.(*jobrunaggregatorapi.JobRunRow))
		return
	}

	if srcVal.Len() == 0 {
		return
	}

	// Build one BULK INSERT
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "BULK INSERT into %v\n", d.table)
	for i := 0; i < srcVal.Len(); i++ {
		s := srcVal.Index(i).Interface()
		fmt.Fprintf(buf, "\tINSERT into %v: %+v\n", d.table, s.(*testRunRow))
	}
	fmt.Fprint(d.out, buf.String())

	return nil
}

func newJobRunRow(jobRun jobrunaggregatorapi.JobRunInfo, prowJob *prowv1.ProwJob) *jobrunaggregatorapi.JobRunRow {
	var endTime time.Time
	if prowJob.Status.CompletionTime != nil {
		endTime = prowJob.Status.CompletionTime.Time
	}
	return &jobrunaggregatorapi.JobRunRow{
		Name:       jobRun.GetJobRunID(),
		JobName:    jobRun.GetJobName(),
		Status:     string(prowJob.Status.State),
		StartTime:  prowJob.Status.StartTime.Time,
		EndTime:    endTime,
		ReleaseTag: prowJob.Labels["release.openshift.io/analysis"],
		Cluster:    prowJob.Spec.Cluster,
	}

}

type testRunRow struct {
	prowJob    *prowv1.ProwJob
	jobRun     jobrunaggregatorapi.JobRunInfo

	// ci-tools/pkg/junit/types.go mentions that a TestSuite can itself hold
	// TestSuites, hence declared as a slice below; but we only use a single
	// TestSuite today.
	testSuites []string
	testCase   *junit.TestCase
}

func newTestRunRow(jobRun jobrunaggregatorapi.JobRunInfo, prowJob *prowv1.ProwJob, testSuites []string, testCase *junit.TestCase) *testRunRow {
	return &testRunRow{
		prowJob:    prowJob,
		jobRun:     jobRun,
		testSuites: testSuites,
		testCase:   testCase,
	}

}

// This compiles fine with or without it.
// Why is this here?
var _ bigquery.ValueSaver = &testRunRow{}

// The "Put" method uploads a row to Big Query.
// "Put" uses a "Save" method (if defined) to produce a new row.  Here, testRunRow implements
// "Save", so this function is called when creating a row for uploading.
func (v *testRunRow) Save() (map[string]bigquery.Value, string, error) {

	// the linter requires not setting a default value. This seems strictly worse and more error-prone to me, but
	// I am a slave to the bot.
	//status := "Unknown"
	var status string
	switch {
	case v.testCase.FailureOutput != nil:
		status = "Failed"
	case v.testCase.SkipMessage != nil:
		status = "Skipped"
	default:
		status = "Passed"
	}

	// We can add v.TestSuites[0] here so that the TestRun table gets populated with it.
	row := map[string]bigquery.Value{
		"Name":       v.testCase.Name,
		"JobRunName": v.jobRun.GetJobRunID(),
		"JobName":    v.jobRun.GetJobName(),
		"Status":     status,
	}

	return row, "", nil
}
