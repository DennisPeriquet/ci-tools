package jobruntestcaseanalyzer

import (
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"gopkg.in/yaml.v2"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorlib"
	"github.com/openshift/ci-tools/pkg/junit"
)

type testIdentifier struct {
	testSuites []string
	testName   string
}

var (
	installTestSuites     = []string{"cluster install"}
	installTest           = "install should succeed: overall"
	installTestIdentifier = testIdentifier{testSuites: installTestSuites, testName: installTest}
)

// JobGetter gets related jobs for further analysis
type JobGetter interface {
	GetJobs(ctx context.Context) ([]jobrunaggregatorapi.JobRow, error)
}

type testCaseAnalyzerJobGetter struct {
	platform        string
	infrastructure  string
	network         string
	excludeJobNames sets.String
	jobGCSPrefixes  *[]jobGCSPrefix
	ciDataClient    jobrunaggregatorlib.CIDataClient
}

// GetJobs find all related jobs for the test case analyzer
// For PR payload, this contains jobs correspond to the list of jobGCSPreix passed
// For release-controller generated payload, this contains all jobs meeting selection criteria
// from command args.
func (s *testCaseAnalyzerJobGetter) GetJobs(ctx context.Context) ([]jobrunaggregatorapi.JobRow, error) {
	jobs, err := s.ciDataClient.ListAllJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list all jobs: %w", err)
	}

	// if PR payload, only find the exact jobs
	if s.jobGCSPrefixes != nil && len(*s.jobGCSPrefixes) > 0 {
		jobNames := sets.String{}
		for i := range *s.jobGCSPrefixes {
			jobGCSPrefix := (*s.jobGCSPrefixes)[i]
			jobNames.Insert(jobGCSPrefix.jobName)
		}
		jobs = s.filterJobsByNames(jobNames, jobs)
	} else {
		// Non PR payload, select by criteria
		jobs = s.filterJobsForPayload(jobs)
	}
	return jobs, nil
}

func getJobInfrastructure(name string) string {
	if strings.Contains(name, "upi") {
		return "upi"
	}
	return "ipi"
}

func (s *testCaseAnalyzerJobGetter) filterJobsForPayload(allJobs []jobrunaggregatorapi.JobRow) []jobrunaggregatorapi.JobRow {
	jobs := []jobrunaggregatorapi.JobRow{}
	for i := range allJobs {
		job := allJobs[i]
		if (len(s.platform) != 0 && job.Platform != s.platform) ||
			(len(s.network) != 0 && job.Network != s.network) ||
			(len(s.infrastructure) != 0 && s.infrastructure != getJobInfrastructure(job.JobName)) {
			continue
		}

		if s.isJobNameFiltered(job.JobName) {
			continue
		}

		jobs = append(jobs, job)
	}
	return jobs
}

func (s *testCaseAnalyzerJobGetter) isJobNameFiltered(jobName string) bool {

	if s.excludeJobNames == nil {
		return false
	}

	for key := range s.excludeJobNames {
		if strings.Contains(jobName, key) {
			return true
		}
	}

	return false
}

func (s *testCaseAnalyzerJobGetter) filterJobsByNames(jobNames sets.String, allJobs []jobrunaggregatorapi.JobRow) []jobrunaggregatorapi.JobRow {
	ret := []jobrunaggregatorapi.JobRow{}
	for i := range allJobs {
		curr := allJobs[i]
		if jobNames.Has(curr.JobName) {
			ret = append(ret, curr)
		}
	}
	return ret
}

// TestCaseChecker checks if a test passes certain criteria across all job runs
type TestCaseChecker interface {
	// CheckTestCase returns a test suite based on whether a test has passed certain criteria across job runs
	CheckTestCase(ctx context.Context, jobRunJunits map[jobrunaggregatorapi.JobRunInfo]*junit.TestSuites) *junit.TestSuite
}

type minimumRequiredPassesTestCaseChecker struct {
	id testIdentifier
	// testNameSuffix is a string that will be appended to the test name for the test case to
	// be created. This might include variant info like platform, network and infrastructure etc.
	testNameSuffix         string
	requiredNumberOfPasses int
}

type testStatus int

const (
	testSkipped testStatus = iota
	testPassed
	testFailed
)

func getTestStatus(id testIdentifier, testSuite *junit.TestSuite) testStatus {
	if len(id.testSuites) == 0 || id.testSuites[0] != testSuite.Name {
		return testSkipped
	}
	// We have a top level suite match, search for test case
	if len(id.testSuites) == 1 {
		for _, testCase := range testSuite.TestCases {
			// Exact match will result in either pass or fail
			if testCase.Name == id.testName {
				if testCase.FailureOutput == nil {
					return testPassed
				} else {
					return testFailed
				}
			}
		}
		return testSkipped
	}
	// Search next level
	next := id
	next.testSuites = id.testSuites[1:]
	for _, childSuite := range testSuite.Children {
		if len(next.testSuites) > 0 && next.testSuites[0] == childSuite.Name {
			status := getTestStatus(next, childSuite)
			if status == testPassed || status == testFailed {
				return status
			}
		}
	}
	return testSkipped
}

func (r minimumRequiredPassesTestCaseChecker) addTestResultToDetails(currDetails *jobrunaggregatorlib.TestCaseDetails,
	jobRun jobrunaggregatorapi.JobRunInfo, status testStatus) {
	switch status {
	case testPassed:
		currDetails.Passes = append(
			currDetails.Passes,
			jobrunaggregatorlib.TestCasePass{
				JobRunID:       jobRun.GetJobRunID(),
				HumanURL:       jobRun.GetHumanURL(),
				GCSArtifactURL: jobRun.GetGCSArtifactURL(),
			})
	case testFailed:
		currDetails.Failures = append(
			currDetails.Failures,
			jobrunaggregatorlib.TestCaseFailure{
				JobRunID:       jobRun.GetJobRunID(),
				HumanURL:       jobRun.GetHumanURL(),
				GCSArtifactURL: jobRun.GetGCSArtifactURL(),
			})
	default:
		currDetails.Skips = append(
			currDetails.Skips,
			jobrunaggregatorlib.TestCaseSkip{
				JobRunID:       jobRun.GetJobRunID(),
				HumanURL:       jobRun.GetHumanURL(),
				GCSArtifactURL: jobRun.GetGCSArtifactURL(),
			})
	}
}

// addToTestSuiteFromSuiteNames adds embedded TestSuite from array of suite names to top suite.
// It returns the bottom level suite for easy access for callers.
func addToTestSuiteFromSuiteNames(topSuite *junit.TestSuite, suiteNames []string) *junit.TestSuite {
	previousSuite := topSuite
	for _, suiteName := range suiteNames {
		currentSuite := &junit.TestSuite{
			Name:      suiteName,
			TestCases: []*junit.TestCase{},
		}
		previousSuite.Children = append(previousSuite.Children, currentSuite)
		previousSuite = currentSuite
	}
	return previousSuite
}

func updateTestCountsInSuite(suite *junit.TestSuite) {
	var numTests, numFailed uint
	for _, test := range suite.TestCases {
		numTests++
		if test.FailureOutput != nil {
			numFailed++
		}
	}
	for _, child := range suite.Children {
		updateTestCountsInSuite(child)
		numTests += child.NumTests
		numFailed += child.NumFailed
	}
	suite.NumTests = numTests
	suite.NumFailed = numFailed
}

// CheckTestCase returns a test case based on whether a test has passed certain criteria across job runs
func (r minimumRequiredPassesTestCaseChecker) CheckTestCase(ctx context.Context, jobRunJunits map[jobrunaggregatorapi.JobRunInfo]*junit.TestSuites) *junit.TestSuite {
	topSuite := &junit.TestSuite{
		Name:      "minimum-required-passes-checker",
		TestCases: []*junit.TestCase{},
	}
	bottomSuite := addToTestSuiteFromSuiteNames(topSuite, r.id.testSuites)

	testName := fmt.Sprintf("test '%s' has required number of successful passes across payload jobs", r.id.testName)
	if len(r.testNameSuffix) > 0 {
		testName += fmt.Sprintf(" for %s", r.testNameSuffix)
	}
	testCase := &junit.TestCase{
		Name: testName,
	}
	bottomSuite.TestCases = append(bottomSuite.TestCases, testCase)

	start := time.Now()
	successCount := 0
	currDetails := &jobrunaggregatorlib.TestCaseDetails{
		Name:          r.id.testName,
		TestSuiteName: strings.Join(r.id.testSuites, jobrunaggregatorlib.TestSuitesSeparator),
	}
	for jobRun, testSuites := range jobRunJunits {
		status := testSkipped
		// Now go through test suites check test result
		for _, testSuite := range testSuites.Suites {
			status = getTestStatus(r.id, testSuite)
			found := false
			switch status {
			case testPassed:
				found = true
				successCount++
			case testFailed:
				found = true
			}
			if found {
				break
			}
		}
		r.addTestResultToDetails(currDetails, jobRun, status)
	}
	currDetails.Summary = fmt.Sprintf("Total job runs: %d, passes: %d, failures: %d, skips %d", len(jobRunJunits), len(currDetails.Passes), len(currDetails.Failures), len(currDetails.Skips))
	detailsYaml, err := yaml.Marshal(currDetails)
	if err != nil {
		return nil
	}
	testCase.Duration = time.Since(start).Seconds()
	testCase.SystemOut = string(detailsYaml)
	if successCount < r.requiredNumberOfPasses {
		testCase.FailureOutput = &junit.FailureOutput{
			Message: fmt.Sprintf("required minimum successful count %d, got %d", r.requiredNumberOfPasses, successCount),
		}
	}
	updateTestCountsInSuite(topSuite)
	return topSuite
}

// JobRunTestCaseAnalyzerOptions
// 1. either gets a list of jobs from big query that meet the passed criteria: platform, network, or uses the passed jobs
// 2. finds job runs for matching jobs for the specified payload tag
// 3. runs all test case checkers and constructs a synthetic junit
type JobRunTestCaseAnalyzerOptions struct {
	payloadTag string
	workingDir string
	// jobRunStartEstimate is used by job run locator to calculate the time window to search for job runs.
	jobRunStartEstimate time.Time
	timeout             time.Duration
	ciDataClient        jobrunaggregatorlib.CIDataClient
	ciGCSClient         jobrunaggregatorlib.CIGCSClient
	gcsClient           *storage.Client
	testCaseCheckers    []TestCaseChecker
	payloadInvocationID string
	jobGCSPrefixes      *[]jobGCSPrefix
	jobGetter           JobGetter
}

func (o *JobRunTestCaseAnalyzerOptions) findJobRunsWithRetry(ctx context.Context,
	jobName string, jobRunLocator jobrunaggregatorlib.JobRunLocator) ([]jobrunaggregatorapi.JobRunInfo, error) {
	errorsInARow := 0
	for {
		jobRuns, err := jobRunLocator.FindRelatedJobs(ctx)
		if err != nil {
			if errorsInARow > 20 {
				fmt.Printf("give up finding job runs for %s after retries: %v", jobName, err)
				return nil, err
			}
			errorsInARow++
			fmt.Printf("error finding job runs for %s: %v", jobName, err)
		} else {
			return jobRuns, nil
		}

		fmt.Printf("   waiting and will attempt to find related jobs for %s in a minute\n", jobName)
		select {
		case <-ctx.Done():
			// Simply return. Caller will check ctx and return error
			return nil, ctx.Err()
		case <-time.After(1 * time.Minute):
			continue
		}
	}
}

// GetRelatedJobRuns gets all related job runs for analysis
func (o *JobRunTestCaseAnalyzerOptions) GetRelatedJobRuns(ctx context.Context) ([]jobrunaggregatorapi.JobRunInfo, error) {
	var jobRunsToReturn []jobrunaggregatorapi.JobRunInfo
	jobs, err := o.jobGetter.GetJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get related jobs: %w", err)
	}

	waitGroup := sync.WaitGroup{}
	resultCh := make(chan []jobrunaggregatorapi.JobRunInfo, len(jobs))
	for i := range jobs {
		job := jobs[i]
		var jobRunLocator jobrunaggregatorlib.JobRunLocator

		if len(o.payloadTag) > 0 {
			jobRunLocator = jobrunaggregatorlib.NewPayloadAnalysisJobLocatorForReleaseController(
				job.JobName,
				o.payloadTag,
				o.jobRunStartEstimate,
				o.ciDataClient,
				o.ciGCSClient,
				o.gcsClient,
				"origin-ci-test",
			)
		}
		if len(o.payloadInvocationID) > 0 {
			jobRunLocator = jobrunaggregatorlib.NewPayloadAnalysisJobLocatorForPR(
				job.JobName,
				o.payloadInvocationID,
				jobrunaggregatorlib.PayloadInvocationIDLabel,
				o.jobRunStartEstimate,
				o.ciDataClient,
				o.ciGCSClient,
				o.gcsClient,
				"origin-ci-test",
				(*o.jobGCSPrefixes)[i].gcsPrefix,
			)
		}

		fmt.Printf("  launching findJobRunsWithRetry for %q\n", job.JobName)

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			jobRuns, err := o.findJobRunsWithRetry(ctx, job.JobName, jobRunLocator)
			if err == nil {
				resultCh <- jobRuns
			}
		}()
	}
	waitGroup.Wait()
	close(resultCh)

	// drain the result channel first
	for jobRuns := range resultCh {
		jobRunsToReturn = append(jobRunsToReturn, jobRuns...)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		break
	}
	return jobRunsToReturn, nil
}

func (o *JobRunTestCaseAnalyzerOptions) runTestCaseCheckers(ctx context.Context,
	finishedJobRuns []jobrunaggregatorapi.JobRunInfo, unfinishedJobRuns []jobrunaggregatorapi.JobRunInfo) *junit.TestSuite {
	suiteName := "payload-cross-jobs"
	topSuite := &junit.TestSuite{
		Name:      suiteName,
		TestCases: []*junit.TestCase{},
	}

	allJobRuns := append(finishedJobRuns, unfinishedJobRuns...)
	jobRunJunitMap := map[jobrunaggregatorapi.JobRunInfo]*junit.TestSuites{}
	for i := range allJobRuns {
		jobRun := allJobRuns[i]

		testSuites, err := jobRun.GetCombinedJUnitTestSuites(ctx)
		if err != nil {
			continue
		}
		jobRunJunitMap[jobRun] = testSuites
	}
	for _, checker := range o.testCaseCheckers {
		testSuite := checker.CheckTestCase(ctx, jobRunJunitMap)
		topSuite.Children = append(topSuite.Children, testSuite)
		topSuite.NumTests += testSuite.NumTests
		topSuite.NumFailed += testSuite.NumFailed
	}
	return topSuite
}

func (o *JobRunTestCaseAnalyzerOptions) Run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	matchID := o.payloadTag
	if len(matchID) == 0 {
		matchID = o.payloadInvocationID
	}

	outputDir := filepath.Join(o.workingDir, matchID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating output directory %q: %w", outputDir, err)
	}

	// if it hasn't been more than hour since the jobRuns started, the list isn't complete.
	readyAt := o.jobRunStartEstimate.Add(1 * time.Hour)

	durationToWait := o.timeout - 20*time.Minute
	timeToStopWaiting := o.jobRunStartEstimate.Add(durationToWait)

	fmt.Printf("Analyzing test status for job runs for %q.  now=%v, ReadyAt=%v, timeToStopWaiting=%v.\n", matchID, time.Now(), readyAt, timeToStopWaiting)

	err := jobrunaggregatorlib.WaitUntilTime(ctx, readyAt)
	if err != nil {
		return err
	}
	finishedJobRuns, unfinishedJobRuns, _, _, err := jobrunaggregatorlib.WaitAndGetAllFinishedJobRuns(ctx, timeToStopWaiting, o, outputDir)
	if err != nil {
		return err
	}

	testSuite := o.runTestCaseCheckers(ctx, finishedJobRuns, unfinishedJobRuns)
	jobrunaggregatorlib.OutputTestCaseFailures([]string{"root"}, testSuite)

	// Done with all tests
	junitXML, err := xml.Marshal(testSuite)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(filepath.Join(outputDir, "junit-test-case-analysis.xml"), junitXML, 0644); err != nil {
		return err
	}
	if testSuite.NumFailed > 0 {
		return jobrunaggregatorlib.ErrorTestCheckerFailed
	}
	return nil
}
