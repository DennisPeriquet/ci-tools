package jobruntestcaseanalyzer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorlib"
)

const (
	installTestGroup                  string = "install"
	defaultMinimumSuccessfulTestCount int    = 1
	// maxTimeout is our guess of the maximum duration for a job run
	maxTimeout time.Duration = 4*time.Hour + 35*time.Minute
)

var (
	knownPlatforms = sets.String{
		"aws":     sets.Empty{},
		"azure":   sets.Empty{},
		"gcp":     sets.Empty{},
		"libvirt": sets.Empty{},
		"metal":   sets.Empty{},
		"ovirt":   sets.Empty{},
		"vsphere": sets.Empty{},
	}
	knownNetworks        = sets.String{"ovn": sets.Empty{}, "sdn": sets.Empty{}}
	knownInfrastructures = sets.String{"upi": sets.Empty{}, "ipi": sets.Empty{}}
	knownTestGroups      = sets.String{installTestGroup: sets.Empty{}}
)

type jobGCSPrefix struct {
	jobName   string
	gcsPrefix string
}

type jobGCSPrefixSlice struct {
	values *[]jobGCSPrefix
}

func (s *jobGCSPrefixSlice) String() string {
	if len(*s.values) == 0 {
		return ""
	}
	var jobPairs []string
	for _, value := range *s.values {
		jobPairs = append(jobPairs, fmt.Sprintf("%s=%s", value.jobName, value.gcsPrefix))
	}
	return strings.Join(jobPairs, ",")
}

func (s *jobGCSPrefixSlice) Set(value string) error {
	if len(value) == 0 {
		*s.values = nil
		return nil
	}
	jobPairs := strings.Split(value, ",")
	if len(jobPairs) == 0 {
		return fmt.Errorf("need at least one GCS prefix configured with explicit-gcs-prefixes")
	}
	for _, jobPair := range jobPairs {
		jStrs := strings.Split(jobPair, "=")
		if len(jStrs) != 2 {
			return fmt.Errorf("GCS prefix should consist of job name and GCS prefix separated by '='")
		}
		*s.values = append(*s.values, jobGCSPrefix{jobName: jStrs[0], gcsPrefix: jStrs[1]})
	}
	return nil
}

func (s *jobGCSPrefixSlice) Type() string {
	return "jobGCSPrefixSlice"
}

type stringSlice struct {
	values []string
}

func (s *stringSlice) String() string {
	return strings.Join(s.values, ",")
}

func (s *stringSlice) Set(value string) error {
	s.values = append(s.values, value)
	return nil
}

func (s *stringSlice) Type() string {
	return "stringSlice"
}

type JobRunsTestCaseAnalyzerFlags struct {
	DataCoordinates *jobrunaggregatorlib.BigQueryDataCoordinates
	Authentication  *jobrunaggregatorlib.GoogleAuthenticationFlags

	TestGroups                  stringSlice
	WorkingDir                  string
	PayloadTag                  string
	Timeout                     time.Duration
	EstimatedJobStartTimeString string
	Platform                    string
	Infrastructure              string
	Network                     string
	MinimumSuccessfulTestCount  int
	PayloadInvocationID         string
	JobGCSPrefixes              []jobGCSPrefix
	ExcludeJobNames             []string
}

func NewJobRunsTestCaseAnalyzerFlags() *JobRunsTestCaseAnalyzerFlags {
	return &JobRunsTestCaseAnalyzerFlags{
		DataCoordinates: jobrunaggregatorlib.NewBigQueryDataCoordinates(),
		Authentication:  jobrunaggregatorlib.NewGoogleAuthenticationFlags(),

		WorkingDir:                  "test-case-analyzer-working-dir",
		EstimatedJobStartTimeString: time.Now().Format(kubeTimeSerializationLayout),
		Timeout:                     3*time.Hour + 30*time.Minute,
		MinimumSuccessfulTestCount:  defaultMinimumSuccessfulTestCount,
	}
}

const kubeTimeSerializationLayout = time.RFC3339

func (f *JobRunsTestCaseAnalyzerFlags) BindFlags(fs *pflag.FlagSet) {
	f.DataCoordinates.BindFlags(fs)
	f.Authentication.BindFlags(fs)

	fs.Var(&f.TestGroups, "test-group", "One or more test groups to analyze, like install")
	fs.StringVar(&f.PayloadTag, "payload-tag", f.PayloadTag, "The release controller payload tag to analyze test case status, like 4.9.0-0.ci-2021-07-19-185802")
	fs.StringVar(&f.EstimatedJobStartTimeString, "job-start-time", f.EstimatedJobStartTimeString, fmt.Sprintf("Start time in RFC822Z: %s. This defines the search window for job runs. Only job runs whose start time is in between job-start-time - %s and job-start-time + %s will be included.", kubeTimeSerializationLayout, jobrunaggregatorlib.JobSearchWindowStartOffset, jobrunaggregatorlib.JobSearchWindowEndOffset))
	fs.StringVar(&f.Platform, "platform", f.Platform, "The platform used to narrow down a subset of the jobs to analyze, ex: aws|gcp|azure|vsphere")
	fs.StringVar(&f.Infrastructure, "infrastructure", f.Infrastructure, "The infrastructure used to narrow down a subset of the jobs to analyze, ex: upi|ipi")
	fs.StringVar(&f.Network, "network", f.Network, "The network used to narrow down a subset of the jobs to analyze, ex: sdn|ovn")
	fs.IntVar(&f.MinimumSuccessfulTestCount, "minimum-successful-count", defaultMinimumSuccessfulTestCount, "minimum number of successful test counts among jobs meeting criteria")
	usage := fmt.Sprintf("mutually exclusive to --payload-tag.  Matches the .label[%s] on the prowjob, which is a UID", jobrunaggregatorlib.PayloadInvocationIDLabel)
	fs.StringVar(&f.PayloadInvocationID, "payload-invocation-id", f.PayloadInvocationID, usage)

	fs.StringVar(&f.WorkingDir, "working-dir", f.WorkingDir, "The directory to store caches, output, and the like.")
	fs.DurationVar(&f.Timeout, "timeout", f.Timeout, "Time to wait for analyzing job to complete.")
	fs.Var(&jobGCSPrefixSlice{&f.JobGCSPrefixes}, "explicit-gcs-prefixes", "a list of gcs prefixes for jobs created for payload. Only used by per PR payload promotion jobs. The format is comma-separated elements, each consisting of job name and gcs prefix separated by =, like openshift-machine-config-operator=3028-ci-4.11-e2e-aws-ovn-upgrade~logs/openshift-machine-config-operator-3028-ci-4.11-e2e-aws-ovn-upgrade")

	fs.StringArrayVar(&f.ExcludeJobNames, "exclude-job-names", f.ExcludeJobNames, "Applied only when --explicit-gcs-prefixes is not specified.  The flag can be specified multiple times to create a list of substrings used to filter JobNames from the analysis")
}

func NewJobRunsTestCaseAnalyzerCommand() *cobra.Command {
	f := NewJobRunsTestCaseAnalyzerFlags()

	/* Example runs for release controller:
	 ./job-run-aggregator analyze-test-case
	    --google-service-account-credential-file=credential.json
	    --test-group=install
	    --platform=aws
	    --network=sdn
	    --infrastructure=ipi
	    --payload-tag=4.11.0-0.nightly-2022-04-28-102605
	    --job-start-time=2022-04-28T10:28:48Z
	    --minimum-successful-count=10
		--exclude-job-names=upgrade
		--exclude-job-names=ipv6

	Example runs for PR based paylaod:
	 ./job-run-aggregator analyze-test-case
	    --google-service-account-credential-file=credential.json
	    --test-group=install
	    --payload-invocation-id=09406e30ea661e228c17120f28eff3c6
	    --job-start-time=2022-03-18T13:10:20Z
	    --minimum-successful-count=10
	    --explicit-gcs-prefixes=periodic-ci-openshift-release-master-ci-4.11-e2e-aws-ovn-upgrade=logs/openshift-machine-config-operator-3028-ci-4.11-e2e-aws-ovn-upgrade,periodic-ci-openshift-release-master-ci-4.11-upgrade-from-stable-4.10-e2e-aws-ovn-upgrade=/logs/openshift-machine-config-operator-3028-ci-4.11-upgrade-from-stable-4.10-e2e-aws-ovn-upgrade
	*/
	cmd := &cobra.Command{
		Use: "analyze-test-case",
		Long: `Analyze status of a test case of certain group to make sure they meet minimum criteria specified.
The goal is to analyze test results across job runs of multiple different jobs. This enhances
the functionality of current aggregator, which analyzes job runs of the same job. The result
of the analysis can be used to gate nightly or CI payloads.

The candidate job runs can be a subset of jobs started by nightly or CI payload runs. They can
also be a subset of jobs started by PR payload command. For nightly or CI payload jobs, payload-tag 
is used to select jobs that belong to the particular payload run. For PR payload jobs, we use 
payload-invocation-id to select the jobs.

Each group is matched to a subset of known tests. Currently only 'install' group is supported. Other 
groups like 'upgrade' can be added in the future.
`,
		SilenceUsage: true,

		Example: `To make sure there are at least 10 successful installs for all aws sdn ipi jobs for
payload 4.11.0-0.nightly-2022-04-28-102605, run this command:

./job-run-aggregator analyze-test-case
--google-service-account-credential-file=credential.json
--test-group=install
--platform=aws
--network=sdn
--infrastructure=ipi
--payload-tag=4.11.0-0.nightly-2022-04-28-102605
--job-start-time=2022-04-28T10:28:48Z
--minimum-successful-count=10
`,

		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if err := f.Validate(); err != nil {
				logrus.WithError(err).Fatal("Flags are invalid")
			}
			o, err := f.ToOptions(ctx)
			if err != nil {
				logrus.WithError(err).Fatal("Failed to build runtime options")
			}

			if err := o.Run(ctx); err != nil {
				switch err {
				case jobrunaggregatorlib.ErrorNoRelatedJobs, jobrunaggregatorlib.ErrorTestCheckerFailed:
					logrus.WithError(err).Warning("Unable to perform test analysis")
				default:
					logrus.WithError(err).Fatal("Command failed")
				}
			}

			return nil
		},

		Args: jobrunaggregatorlib.NoArgs,
	}

	f.BindFlags(cmd.Flags())

	return cmd
}

// Validate checks to see if the user-input is likely to produce functional runtime options
func (f *JobRunsTestCaseAnalyzerFlags) Validate() error {
	if len(f.WorkingDir) == 0 {
		return fmt.Errorf("missing --working-dir: like test-analyzer-working-dir")
	}
	if _, err := time.Parse(kubeTimeSerializationLayout, f.EstimatedJobStartTimeString); err != nil {
		return err
	}
	if err := f.DataCoordinates.Validate(); err != nil {
		return err
	}
	if err := f.Authentication.Validate(); err != nil {
		return err
	}
	if len(f.TestGroups.values) == 0 {
		return fmt.Errorf("at least one test group has to be specified")
	}
	for _, group := range f.TestGroups.values {
		if _, ok := knownTestGroups[group]; !ok {
			return fmt.Errorf("unknown test group %s, valid values are: %+q", group, knownTestGroups.List())
		}
	}
	if len(f.PayloadTag) > 0 && len(f.PayloadInvocationID) > 0 {
		return fmt.Errorf("cannot specify both --payload-tag and --payload-invocation-id")
	}
	if len(f.PayloadTag) == 0 && len(f.PayloadInvocationID) == 0 {
		return fmt.Errorf("exactly one of --payload-tag or --payload-invocation-id must be specified")
	}
	if len(f.PayloadInvocationID) > 0 && len(f.JobGCSPrefixes) == 0 {
		return fmt.Errorf("if --payload-invocation-id is specified, you must specify --explicit-gcs-prefixes")
	}
	if len(f.PayloadInvocationID) > 0 && (len(f.Platform) > 0 || len(f.Network) > 0 || len(f.Infrastructure) > 0) {
		return fmt.Errorf("if --payload-invocation-id is specified, --platform, --network or --infrastructure cannot be specified")
	}

	if len(f.Platform) > 0 {
		if _, ok := knownPlatforms[f.Platform]; !ok {
			return fmt.Errorf("unknown platform %s, valid values are: %+q", f.Platform, knownPlatforms)
		}
	}

	if len(f.Network) > 0 {
		if _, ok := knownNetworks[f.Network]; !ok {
			return fmt.Errorf("unknown network %s, valid values are: %+q", f.Network, knownNetworks)
		}
	}

	if len(f.Infrastructure) > 0 {

		if _, ok := knownInfrastructures[f.Infrastructure]; !ok {
			return fmt.Errorf("unknown infrastructure %s, valid values are: %+q", f.Infrastructure, knownInfrastructures.List())
		}
	}

	if f.Timeout > maxTimeout {
		return fmt.Errorf("timeout value of %s is out of range, valid value should be less than %s", f.Timeout, maxTimeout)
	}

	return nil
}

// testNameSuffix allows TestCaseCheckers to append filter parameters to test names for easy categorization
func (f *JobRunsTestCaseAnalyzerFlags) testNameSuffix() string {
	suffix := ""
	if len(f.Platform) > 0 {
		suffix += fmt.Sprintf("plaftorm:%s ", f.Platform)
	}
	if len(f.Network) > 0 {
		suffix += fmt.Sprintf("network:%s ", f.Network)
	}
	if len(f.Infrastructure) > 0 {
		suffix += fmt.Sprintf("infrastructure:%s", f.Infrastructure)
	}
	suffix = strings.TrimSpace(suffix)
	return suffix
}

// ToOptions creates a new JobRunTestCaseAnalyzerOptions struct
func (f *JobRunsTestCaseAnalyzerFlags) ToOptions(ctx context.Context) (*JobRunTestCaseAnalyzerOptions, error) {
	estimatedStartTime, err := time.Parse(kubeTimeSerializationLayout, f.EstimatedJobStartTimeString)
	if err != nil {
		return nil, err
	}

	bigQueryClient, err := f.Authentication.NewBigQueryClient(ctx, f.DataCoordinates.ProjectID)
	if err != nil {
		return nil, err
	}
	ciDataClient := jobrunaggregatorlib.NewRetryingCIDataClient(
		jobrunaggregatorlib.NewCIDataClient(*f.DataCoordinates, bigQueryClient),
	)

	gcsClient, err := f.Authentication.NewGCSClient(ctx)
	if err != nil {
		return nil, err
	}
	ciGCSClient, err := f.Authentication.NewCIGCSClient(ctx, "origin-ci-test")
	if err != nil {
		return nil, err
	}

	jobGetter := &testCaseAnalyzerJobGetter{
		platform:       f.Platform,
		infrastructure: f.Infrastructure,
		network:        f.Network,
		jobGCSPrefixes: &f.JobGCSPrefixes,
		ciDataClient:   ciDataClient,
	}

	if f.ExcludeJobNames != nil && len(f.ExcludeJobNames) > 0 {
		jobGetter.excludeJobNames = sets.String{}
		jobGetter.excludeJobNames.Insert(f.ExcludeJobNames...)
	}

	return &JobRunTestCaseAnalyzerOptions{
		payloadTag:          f.PayloadTag,
		workingDir:          f.WorkingDir,
		jobRunStartEstimate: estimatedStartTime,
		timeout:             f.Timeout,
		ciDataClient:        ciDataClient,
		ciGCSClient:         ciGCSClient,
		gcsClient:           gcsClient,
		testCaseCheckers:    []TestCaseChecker{minimumRequiredPassesTestCaseChecker{installTestIdentifier, f.testNameSuffix(), f.MinimumSuccessfulTestCount}},
		payloadInvocationID: f.PayloadInvocationID,
		jobGCSPrefixes:      &f.JobGCSPrefixes,
		jobGetter:           jobGetter,
	}, nil
}
