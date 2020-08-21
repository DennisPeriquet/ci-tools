package steps

import (
	"bytes"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiimagev1 "github.com/openshift/api/image/v1"
	fakeimageclientset "github.com/openshift/client-go/image/clientset/versioned/fake"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/ci-tools/pkg/api"
)

var subs = []api.PullSpecSubstitution{
	{
		PullSpec: "quay.io/openshift/origin-metering-ansible-operator:4.6",
		With:     "pipeline:metering-ansible-operator",
	},
	{
		PullSpec: "quay.io/openshift/origin-metering-reporting-operator:4.6",
		With:     "pipeline:metering-reporting-operator",
	},
	{
		PullSpec: "quay.io/openshift/origin-metering-presto:4.6",
		With:     "stable:metering-presto",
	},
	{
		PullSpec: "quay.io/openshift/origin-metering-hive:4.6",
		With:     "stable:metering-hive",
	},
	{
		PullSpec: "quay.io/openshift/origin-metering-hadoop:4.6",
		With:     "stable:metering-hadoop",
	},
	{
		PullSpec: "quay.io/openshift/origin-ghostunnel:4.6",
		With:     "stable:ghostunnel",
	},
}

func TestGetPipelineSubs(t *testing.T) {
	expected := []api.StepLink{
		api.InternalImageLink(api.PipelineImageStreamTagReference("metering-ansible-operator")),
		api.InternalImageLink(api.PipelineImageStreamTagReference("metering-reporting-operator")),
	}
	actual := getPipelineSubs(subs)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected pipeline subs not equal to return pipeline subs: %s", cmp.Diff(expected, actual))
	}
}

func TestReplaceCommand(t *testing.T) {
	temp, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Failed to create temporary directory for unit test: %v", err)
	}
	if err := exec.Command("cp", "-a", "testdata/4.6", temp).Run(); err != nil {
		t.Fatalf("Failed to copy testdata to tempdir: %v", err)
	}
	for _, sub := range subs {
		// the dockerfile generated by ResolvePullSpec uses exact image tag substitution, which uses @sha as the suffix
		// substitute the `:` with a `@` to verify the sed command works with it
		replace := replaceCommand(sub.PullSpec, strings.Replace(sub.With, ":", "@", 1))
		cmd := exec.Command(replace[0], replace[1:]...)
		cmd.Dir = temp
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to run replace command  \"%s\": %v", replaceCommand(sub.PullSpec, sub.With), err)
		}
	}
	files, err := ioutil.ReadDir(filepath.Join(temp, "4.6"))
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}
	for _, file := range files {
		updatedFilename := filepath.Join(temp, "4.6", file.Name())
		updated, err := ioutil.ReadFile(updatedFilename)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", updatedFilename, err)
		}
		expectedFilename := filepath.Join("testdata/4.6-expected", file.Name())
		expected, err := ioutil.ReadFile(expectedFilename)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", expectedFilename, err)
		}
		if !bytes.Equal(updated, expected) {
			t.Errorf("Updated file %s not equal to expected file %s;\nValue of updated file: %s", updatedFilename, expectedFilename, string(updated))
		}
	}
}

func TestBundleSourceDockerfile(t *testing.T) {
	var expectedDockerfile = `FROM pipeline:src
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-metering-ansible-operator:4.6?some-reg/target-namespace/pipeline@metering-ansible-operator?g" "{}" "+"
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-metering-reporting-operator:4.6?some-reg/target-namespace/pipeline@metering-reporting-operator?g" "{}" "+"
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-metering-presto:4.6?some-reg/target-namespace/stable@metering-presto?g" "{}" "+"
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-metering-hive:4.6?some-reg/target-namespace/stable@metering-hive?g" "{}" "+"
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-metering-hadoop:4.6?some-reg/target-namespace/stable@metering-hadoop?g" "{}" "+"
RUN "find" "." "-type" "f" "-regex" ".*\.\(yaml\|yml\)" "-exec" "sed" "-i" "s?quay.io/openshift/origin-ghostunnel:4.6?some-reg/target-namespace/stable@ghostunnel?g" "{}" "+"`

	fakeClientSet := ciopTestingClient{
		imagecs: fakeimageclientset.NewSimpleClientset(&apiimagev1.ImageStream{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "target-namespace",
				Name:      api.StableImageStream,
			},
			Status: apiimagev1.ImageStreamStatus{
				PublicDockerImageRepository: "some-reg/target-namespace/stable",
				Tags: []apiimagev1.NamedTagEventList{{
					Tag: "metering-presto",
					Items: []apiimagev1.TagEvent{{
						Image: "metering-presto",
					}},
				}, {
					Tag: "metering-hive",
					Items: []apiimagev1.TagEvent{{
						Image: "metering-hive",
					}},
				}, {
					Tag: "metering-hadoop",
					Items: []apiimagev1.TagEvent{{
						Image: "metering-hadoop",
					}},
				}, {
					Tag: "ghostunnel",
					Items: []apiimagev1.TagEvent{{
						Image: "ghostunnel",
					}},
				}},
			},
		}, &apiimagev1.ImageStream{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "target-namespace",
				Name:      api.PipelineImageStream,
			},
			Status: apiimagev1.ImageStreamStatus{
				PublicDockerImageRepository: "some-reg/target-namespace/pipeline",
				Tags: []apiimagev1.NamedTagEventList{{
					Tag: "metering-ansible-operator",
					Items: []apiimagev1.TagEvent{{
						Image: "metering-ansible-operator",
					}},
				}, {
					Tag: "metering-reporting-operator",
					Items: []apiimagev1.TagEvent{{
						Image: "metering-reporting-operator",
					}},
				}},
			},
		}),
		t: t,
	}

	s := bundleSourceStep{
		config: api.BundleSourceStepConfiguration{
			Substitutions: subs,
		},
		jobSpec:     &api.JobSpec{},
		imageClient: fakeClientSet.ImageV1(),
	}
	s.jobSpec.SetNamespace("target-namespace")
	generatedDockerfile, err := s.bundleSourceDockerfile()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if expectedDockerfile != generatedDockerfile {
		t.Errorf("Generated bundle source dockerfile does not equal expected; generated dockerfile: %s", generatedDockerfile)
	}
}
