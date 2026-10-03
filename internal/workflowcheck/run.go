package workflowcheck

import (
	"fmt"
	"net/http/httptest"
	"os"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

func Run(name string) error {
	dataDir, err := os.MkdirTemp("", "cellar-run-check-*")
	if err != nil {
		return fmt.Errorf("create check directory: %w", err)
	}
	defer os.RemoveAll(dataDir)

	store, err := storage.Open(dataDir)
	if err != nil {
		return err
	}
	// The observation batch check pins the clock just before a UTC midnight
	// so file records can cross the day boundary deterministically.
	var timeSource clock.Clock = clock.System{}
	if name == "observations-batch" {
		timeSource = clock.NewFixed(observationBatchNow)
	}
	app := service.New(store, timeSource)
	server := httptest.NewServer(httpapi.NewHandler(app))
	defer server.Close()
	api := newClient(server.URL)

	switch name {
	case "formula-approval":
		if err := formulaApproval(api); err != nil {
			return err
		}
	case "vessel-run-lifecycle":
		if err := vesselRunLifecycle(api); err != nil {
			return err
		}
	case "observations-completion":
		if err := observationsCompletion(api); err != nil {
			return err
		}
	case "observations-batch":
		if err := observationsBatch(api); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown workflow check %q", name)
	}
	fmt.Printf("workflow check passed: %s\n", name)
	return nil
}
