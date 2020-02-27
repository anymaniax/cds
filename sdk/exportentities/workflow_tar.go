package exportentities

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/ovh/cds/sdk"
	"github.com/ovh/cds/sdk/log"
	"gopkg.in/yaml.v2"
)

// Tar returns a tar containing all files for a pulled workflow.
func (w WorkflowPulled) Tar(ctx context.Context, writer io.Writer) error {
	tw := tar.NewWriter(writer)
	defer func() {
		if err := tw.Close(); err != nil {
			log.Error(ctx, "%v", sdk.WrapError(err, "unable to close tar writer"))
		}
	}()

	if w.Workflow.Value != "" {
		bs, err := base64.StdEncoding.DecodeString(w.Workflow.Value)
		if err != nil {
			return sdk.WithStack(err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf(PullWorkflowName, w.Workflow.Name),
			Mode: 0644,
			Size: int64(len(bs)),
		}); err != nil {
			return sdk.WrapError(err, "unable to write workflow header for %s", w.Workflow.Name)
		}
		if _, err := tw.Write(bs); err != nil {
			return sdk.WrapError(err, "unable to write workflow value")
		}
	}

	for _, a := range w.Applications {
		bs, err := base64.StdEncoding.DecodeString(a.Value)
		if err != nil {
			return sdk.WithStack(err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf(PullApplicationName, a.Name),
			Mode: 0644,
			Size: int64(len(bs)),
		}); err != nil {
			return sdk.WrapError(err, "unable to write application header for %s", a.Name)
		}
		if _, err := tw.Write(bs); err != nil {
			return sdk.WrapError(err, "unable to write application value")
		}
	}

	for _, e := range w.Environments {
		bs, err := base64.StdEncoding.DecodeString(e.Value)
		if err != nil {
			return sdk.WithStack(err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf(PullEnvironmentName, e.Name),
			Mode: 0644,
			Size: int64(len(bs)),
		}); err != nil {
			return sdk.WrapError(err, "unable to write env header for %s", e.Name)
		}
		if _, err := tw.Write(bs); err != nil {
			return sdk.WrapError(err, "unable to copy env buffer")
		}
	}

	for _, p := range w.Pipelines {
		bs, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			return sdk.WithStack(err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf(PullPipelineName, p.Name),
			Mode: 0644,
			Size: int64(len(bs)),
		}); err != nil {
			return sdk.WrapError(err, "unable to write pipeline header for %s", p.Name)
		}
		if _, err := tw.Write(bs); err != nil {
			return sdk.WrapError(err, "unable to write pipeline value")
		}
	}

	return nil
}

type ExtractedWorkflowFromTar struct {
	Workflow     Workflow
	Applications map[string]Application
	Pipelines    map[string]PipelineV1
	Environments map[string]Environment
}

func ExtractWorkflowFromTar(ctx context.Context, tr *tar.Reader) (ExtractedWorkflowFromTar, error) {
	var res = ExtractedWorkflowFromTar{
		Applications: make(map[string]Application),
		Pipelines:    make(map[string]PipelineV1),
		Environments: make(map[string]Environment),
	}

	mError := new(sdk.MultiError)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return res, sdk.NewErrorWithStack(err, sdk.NewErrorFrom(sdk.ErrWrongRequest, "unable to read tar file"))
		}

		log.Debug("ExtractWorkflowFromTar> Reading %s", hdr.Name)

		buff := new(bytes.Buffer)
		if _, err := io.Copy(buff, tr); err != nil {
			return res, sdk.NewErrorWithStack(err, sdk.NewErrorFrom(sdk.ErrWrongRequest, "unable to read tar file"))
		}

		var workflowFileName string
		b := buff.Bytes()
		switch {
		case strings.Contains(hdr.Name, ".app."):
			var app Application
			if err := yaml.Unmarshal(b, &app); err != nil {
				log.Error(ctx, "ExtractWorkflowFromTar> Unable to unmarshal application %s: %v", hdr.Name, err)
				mError.Append(fmt.Errorf("unable to unmarshal application %s: %v", hdr.Name, err))
				continue
			}
			res.Applications[hdr.Name] = app
		case strings.Contains(hdr.Name, ".pip."):
			var pip PipelineV1
			if err := yaml.Unmarshal(b, &pip); err != nil {
				log.Error(ctx, "ExtractWorkflowFromTar> Unable to unmarshal pipeline %s: %v", hdr.Name, err)
				mError.Append(fmt.Errorf("unable to unmarshal pipeline %s: %v", hdr.Name, err))
				continue
			}
			res.Pipelines[hdr.Name] = pip
		case strings.Contains(hdr.Name, ".env."):
			var env Environment
			if err := yaml.Unmarshal(b, &env); err != nil {
				log.Error(ctx, "ExtractWorkflowFromTar> Unable to unmarshal environment %s: %v", hdr.Name, err)
				mError.Append(fmt.Errorf("unable to unmarshal environment %s: %v", hdr.Name, err))
				continue
			}
			res.Environments[hdr.Name] = env
		default:
			// if a workflow was already found, it's a mistake
			if workflowFileName != "" {
				log.Error(ctx, "two workflow files found: %s and %s", workflowFileName, hdr.Name)
				mError.Append(fmt.Errorf("two workflows files found: %s and %s", workflowFileName, hdr.Name))
				break
			}
			var err error
			res.Workflow, err = UnmarshalWorkflow(b)
			if err != nil {
				log.Error(ctx, "Push> Unable to unmarshal workflow %s: %v", hdr.Name, err)
				mError.Append(fmt.Errorf("unable to unmarshal workflow %s: %v", hdr.Name, err))
				continue
			}
		}
	}

	// We only use the multiError during unmarshalling steps.
	// When a DB transaction has been started, just return at the first error
	// because transaction may have to be aborted
	if !mError.IsEmpty() {
		return res, sdk.NewError(sdk.ErrWorkflowInvalid, mError)
	}

	return res, nil
}
