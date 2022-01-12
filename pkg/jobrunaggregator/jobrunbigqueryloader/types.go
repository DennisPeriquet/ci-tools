package jobrunbigqueryloader

import (
	"time"

	prowv1 "k8s.io/test-infra/prow/apis/prowjobs/v1"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
	"github.com/openshift/ci-tools/pkg/junit"
)

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

func newTestRunRow(jobRun jobrunaggregatorapi.JobRunInfo, prowJob *prowv1.ProwJob, testSuites []string, testCase *junit.TestCase) *jobrunaggregatorapi.TestRunRow {
	return &jobrunaggregatorapi.TestRunRow{
		ProwJob:    prowJob,
		JobRun:     jobRun,
		TestSuites: testSuites,
		TestCase:   testCase,
	}

}
