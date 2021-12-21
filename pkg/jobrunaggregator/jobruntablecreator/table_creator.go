package jobruntablecreator

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"

	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorapi"
	"github.com/openshift/ci-tools/pkg/jobrunaggregator/jobrunaggregatorlib"
)

type allJobRunTableCreatorOptions struct {
	ciDataClient jobrunaggregatorlib.CIDataClient
	ciDataSet    *bigquery.Dataset
}

func (r *allJobRunTableCreatorOptions) Run(ctx context.Context) error {

	// Create JobRunTable
	jobRunTable := r.ciDataSet.Table(jobrunaggregatorlib.JobsTableName)
	_, err := jobRunTable.Metadata(ctx)
	if err != nil {
		schema, err := bigquery.InferSchema(jobrunaggregatorapi.JobRow{})
		if err != nil {
			return err
		}
		if err := jobRunTable.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(os.Stderr, "table already exists: %s\n", jobrunaggregatorlib.JobRunTableName)
	}

	return nil
}
