package jobrunaggregatorlib

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
)

type CIGCSClient interface {
	ReadJobRunFromGCS(ctx context.Context, jobGCSRootLocation, jobName, jobRunID string) (jobrunaggregatorapi.JobRunInfo, error)

	// ListJobRunNames returns a string channel for jobRunNames, an error channel for reporting errors during listing,
	// and an error if the listing cannot begin.
	ListJobRunNamesOlderThanFourHours(ctx context.Context, jobName, startingID string) (chan string, chan error, error)
}

type ciGCSClient struct {
	gcsClient     *storage.Client
	gcsBucketName string
}

func (o *ciGCSClient) ListJobRunNamesOlderThanFourHours(ctx context.Context, jobName, startingID string) (chan string, chan error, error) {
	query := &storage.Query{
		// This ends up being the equivalent of:
		// https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs/origin-ci-test/logs/periodic-ci-openshift-release-master-nightly-4.9-upgrade-from-stable-4.8-e2e-metal-ipi-upgrade
		Prefix: "logs/" + jobName,

		// TODO this field is apparently missing from this level of go/storage
		// Omit owner and ACL fields for performance
		//Projection: storage.ProjectionNoACL,
	}

	// Only retrieve the name and creation time for performance
	if err := query.SetAttrSelection([]string{"Name", "Created"}); err != nil {
		return nil, nil, err
	}

	// Instead of starting at startingID=0, run this and see where we are skipping and how
	// old the jobs are.  Then pick a job that is just before where you want to be so you
	// can see skips, place that jobrunid in the StartOffset.
	//query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, startingID)
	query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, "1475614363518767104")
	fmt.Printf("  starting from %v\n", query.StartOffset)

	now := time.Now()

	// Returns an iterator which iterates over the bucket query results.
	// Unfortunately, this will list *all* files with the query prefix.
	bkt := o.gcsClient.Bucket(o.gcsBucketName)
	it := bkt.Objects(ctx, query)

	// DP: 100 refers to the max number of jobRuns we'll buffer.  If
	// the number of jobs to process exceeds 100, the go routine will
	// block until a job is finished getting processed on the thing
	// consuming the jobsRuns.
	errorCh := make(chan error, 100)
	jobRunProcessingCh := make(chan string, 100)

	// Find the query results we're the most interested in. In this case, we're interested in files called prowjob.json
	// so that we only get each jobrun once and we queue them in a channel
	go func() {
		var it_count, prow_count int
		var jobRunId string
		defer close(jobRunProcessingCh)

		for {
			it_count++
			if ctx.Err() != nil {
				return
			}

			attrs, err := it.Next()
			if err == iterator.Done {
				// we're done adding values, so close the channel
				fmt.Printf("%4s: it_count = %d; prow_count = %d/%d, %s\n", "Done", it_count, prow_count, len(jobRunProcessingCh), jobName)
				return
			}
			if err != nil {
				errorCh <- err
				return
			}

			// TODO if it's more than 100 days old, we don't need it
			// DP: If we change 100 here to 2, then we can get jobs up to 2 days old
			// This will cut down the number of jobs to upload during a dryrun.
			if now.Sub(attrs.Created) > (1 * 17 * time.Hour) {
				fmt.Printf("%4s: it_count = %d; prow_count = %d/%d, %s/%s\n", ">100", it_count, prow_count, len(jobRunProcessingCh),
					jobName, attrs.Name)
				if strings.HasSuffix(attrs.Name, "latest-build.txt") {
					// Every bucket contains a latest-build.txt file -- ignore it
					continue
				}
				switch {
				case strings.HasSuffix(attrs.Name, ".json"):
					jobRunId = strings.Split(attrs.Name, "/")[2]
					fmt.Printf("%5s: %s/%s, Age=%v\n", "JSkip", jobName, jobRunId, now.Sub(attrs.Created))
					query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
					it = bkt.Objects(ctx, query)
				case strings.HasSuffix(attrs.Name, "prowjob.json"):
					jobRunId = filepath.Base(filepath.Dir(attrs.Name))
					fmt.Printf("%5s: %s/%s, Age=%v\n", "PSkip", jobName, jobRunId, now.Sub(attrs.Created))
					query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
					it = bkt.Objects(ctx, query)
				default:
					fmt.Printf("%5s: %s/%s, Age=%v, %s\n", "MSkip", jobName, jobRunId, now.Sub(attrs.Created), attrs.Name)
				}
				continue
			}

			// chosen because CI jobs only take four hours max (so far), so we only get completed jobs
			// DP: this doesn't make sense because if a CI job takes 4 hours max, then we are skipping
			// jobs that take just under four hours.  We should look for jobs that are greater than the
			// minimum time it takes for a job to complete.
			if now.Sub(attrs.Created) < (4 * time.Hour) {
				fmt.Printf("%4s: it_count = %d; prow_count = %d/%d, %s\n", "<  4", it_count, prow_count, len(jobRunProcessingCh), jobName)
				if strings.HasSuffix(attrs.Name, "latest-build.txt") {
					// Every bucket contains a latest-build.txt file -- ignore it
					continue
				}
				switch {
				case strings.HasSuffix(attrs.Name, "build-log.txt"):
					jobRunId = strings.Split(attrs.Name, "/")[2]
					fmt.Printf("%5s: %s/%s, Age=%v\n", "BSkip", jobName, jobRunId, now.Sub(attrs.Created))
					query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
					it = bkt.Objects(ctx, query)
				case strings.HasSuffix(attrs.Name, ".json"):
					jobRunId = strings.Split(attrs.Name, "/")[2]
					fmt.Printf("%5s: %s/%s, Age=%v\n", "4Skip", jobName, jobRunId, now.Sub(attrs.Created))
					query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
					it = bkt.Objects(ctx, query)
				case strings.HasSuffix(attrs.Name, "prowjob.json"):
					jobRunId = filepath.Base(filepath.Dir(attrs.Name))
					fmt.Printf("%5s: %s/%s, Age=%v\n", "5Skip", jobName, jobRunId, now.Sub(attrs.Created))
					query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
					it = bkt.Objects(ctx, query)
				default:
					fmt.Printf("%5s: %s/%s, Age=%v, %s\n", "LSkip", jobName, jobRunId, now.Sub(attrs.Created), attrs.Name)
				}
				continue
			}

			switch {
			case strings.HasSuffix(attrs.Name, "prowjob.json"):
				jobRunId = filepath.Base(filepath.Dir(attrs.Name))
				fmt.Printf("Queued jobrun/%q/%q\n", jobName, jobRunId)
				prow_count++
				fmt.Printf("%4s: it_count = %d; prow_count = %d/%d, %s/%s\n", "Foun", it_count, prow_count, len(jobRunProcessingCh),
					jobName, jobRunId)

				jobRunProcessingCh <- jobRunId

				query.StartOffset = fmt.Sprintf("logs/%s/%s", jobName, NextJobRunID(jobRunId))
				it = bkt.Objects(ctx, query)
				continue
			default:
				fmt.Printf("%4s: %d  %s %s %s\n", "Chec", it_count, jobName, jobRunId, attrs.Name)
			}
		}
	}()

	return jobRunProcessingCh, errorCh, nil
}

func (o *ciGCSClient) ReadJobRunFromGCS(ctx context.Context, jobGCSRootLocation, jobName, jobRunID string) (jobrunaggregatorapi.JobRunInfo, error) {
	fmt.Printf("reading job run %v/%v.\n", jobGCSRootLocation, jobRunID)

	query := &storage.Query{
		// This ends up being the equivalent of:
		// https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs/origin-ci-test/logs/periodic-ci-openshift-release-master-nightly-4.9-upgrade-from-stable-4.8-e2e-metal-ipi-upgrade
		Prefix: jobGCSRootLocation,

		// TODO this field is apparently missing from this level of go/storage
		// Omit owner and ACL fields for performance
		//Projection: storage.ProjectionNoACL,
	}

	// Only retrieve the name and creation time for performance
	if err := query.SetAttrSelection([]string{"Name", "Created", "Generation"}); err != nil {
		return nil, err
	}
	// start reading for this jobrun bucket
	query.StartOffset = fmt.Sprintf("%s/%s", jobGCSRootLocation, jobRunID)
	// end reading after this jobrun bucket
	query.EndOffset = fmt.Sprintf("%s/%s", jobGCSRootLocation, NextJobRunID(jobRunID))

	// Returns an iterator which iterates over the bucket query results.
	// Unfortunately, this will list *all* files with the query prefix.
	bkt := o.gcsClient.Bucket(o.gcsBucketName)
	it := bkt.Objects(ctx, query)

	// Find the query results we're the most interested in. In this case, we're
	// interested in files called prowjob.json that were created less than 24
	// hours ago.
	var jobRun jobrunaggregatorapi.JobRunInfo
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		switch {
		case strings.HasSuffix(attrs.Name, "prowjob.json"):
			fmt.Printf("  found %s\n", attrs.Name)
			jobRunId := filepath.Base(filepath.Dir(attrs.Name))
			if jobRun == nil {
				jobRun = jobrunaggregatorapi.NewGCSJobRun(bkt, jobGCSRootLocation, jobName, jobRunId)
			}
			jobRun.SetGCSProwJobPath(attrs.Name)

		case strings.HasSuffix(attrs.Name, ".xml") && strings.Contains(attrs.Name, "/junit"):
			fmt.Printf("  found %s\n", attrs.Name)
			nameParts := strings.Split(attrs.Name, "/")
			if len(nameParts) < 4 {
				continue
			}
			jobRunId := nameParts[2]
			if jobRun == nil {
				jobRun = jobrunaggregatorapi.NewGCSJobRun(bkt, jobGCSRootLocation, jobName, jobRunId)
			}
			jobRun.AddGCSJunitPaths(attrs.Name)

		default:
			//fmt.Printf("checking %q\n", attrs.Name)
		}
	}

	// eliminate items without prowjob.json and ones that aren't finished
	if jobRun == nil {
		fmt.Printf("  removing %q/%q because it doesn't have a prowjob.json\n", jobName, jobRunID)
		return nil, nil
	}
	if len(jobRun.GetGCSProwJobPath()) == 0 {
		fmt.Printf("  removing %q/%q because it doesn't have a prowjob.json but does have junit\n", jobName, jobRunID)
		return nil, nil
	}
	_, err := jobRun.GetProwJob(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get prowjob for %q/%q: %w", jobName, jobRunID, err)
	}

	return jobRun, nil
}

func NextJobRunID(curr string) string {
	if len(curr) == 0 {
		return "0"
	}
	idAsInt, err := strconv.ParseInt(curr, 10, 64)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%d", idAsInt+1)
}
