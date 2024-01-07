package jobrunaggregatorlib

import (
	"context"
	"fmt"
	"time"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
)

func htmlForJobRuns(ctx context.Context, finishedJobsToAggregate, unfinishedJobsToAggregate []jobrunaggregatorapi.JobRunInfo, jobSummaryInfo string) string {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
<title>
job-run-summary for %s
</title>
<style>
a {
	color: #ff8caa;
}
a:visited {
	color: #ff8caa;
}
a:hover {
	color: #ffffff;
}
body {
	background-color: rgba(0,0,0,.54);
	color: #ffffff;
}
</style>
</head>
<body>`, jobSummaryInfo)

	if len(unfinishedJobsToAggregate) > 0 {
		html += fmt.Sprintf(`
<h2>Unfinished Jobs %s</h2>
<ol>
`, jobSummaryInfo)
		for _, job := range unfinishedJobsToAggregate {
			html += `<li>`
			html += fmt.Sprintf(`<a target="_blank" href="%s">%s/%s</a>`, job.GetHumanURL(), job.GetJobName(), job.GetJobRunID())
			prowJob, err := job.GetProwJob(ctx)
			if err != nil {
				html += fmt.Sprintf(" unable to get prowjob: %v\n", err)
			}
			if prowJob != nil {
				html += fmt.Sprintf("%v did not finish since %v\n", prowJob.Spec.Cluster, prowJob.CreationTimestamp)
			}
			html += "</li>\n"
		}
		html += `
</ol>
<br/>
`
	}

	if len(finishedJobsToAggregate) > 0 {
		html += fmt.Sprintf(`
<h2>Finished Jobs %s</h2>
<ol>
`, jobSummaryInfo)
		for _, job := range finishedJobsToAggregate {
			html += `<li>`
			html += fmt.Sprintf(`<a target="_blank" href="%s">%s/%s</a>`, job.GetHumanURL(), job.GetJobName(), job.GetJobRunID())
			prowJob, err := job.GetProwJob(ctx)
			if err != nil {
				html += fmt.Sprintf(" unable to get prowjob: %v\n", err)
			}
			if prowJob != nil {
				duration := 0 * time.Second
				if prowJob.Status.CompletionTime != nil {
					duration = prowJob.Status.CompletionTime.Sub(prowJob.Status.StartTime.Time)
				}
				// Create a string of asterisks proportional to the number of seconds (scaled down by 1000)
				// so we can visually ascertain relative job time.
				scaleSeconds := int(duration.Seconds() / 1000.0)
				var stars string
				for j := 0; j < scaleSeconds; j++ {
					stars = stars + "*"
				}
				html += fmt.Sprintf(" %v %v after %v %v\n", prowJob.Spec.Cluster, prowJob.Status.State, duration, stars)
			}
			html += "</li>\n"
		}
		html += `
</ol>
<br/>
`
	}

	html += `
</body>
</html>`

	return html
}
