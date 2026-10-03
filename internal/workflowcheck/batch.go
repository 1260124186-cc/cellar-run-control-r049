package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// observationBatch verifies the atomic batch observation endpoint: file order,
// contiguous sequences, strictly increasing times across a UTC day boundary,
// all-or-nothing rejection, deterministic duplicate imports and retries, and
// equivalence with sequential single-record writes.
func observationBatch(api *client, fixed *clock.Fixed) error {
	formula, err := ensureApprovedFormula(api, "Batch Formula")
	if err != nil {
		return err
	}
	vesselA, err := createCheckVessel(api, "VSL-301", 300)
	if err != nil {
		return err
	}
	vesselB, err := createCheckVessel(api, "VSL-302", 300)
	if err != nil {
		return err
	}
	startedA, err := createStartedRun(api, formula.ID, vesselA.ID, "RUN-BATCH-A")
	if err != nil {
		return err
	}
	startedB, err := createStartedRun(api, formula.ID, vesselB.ID, "RUN-BATCH-B")
	if err != nil {
		return err
	}
	if startedA.Run.Version != startedB.Run.Version {
		return errInvalidWorkflow("parallel runs started with different versions")
	}
	baseVersion := startedA.Run.Version
	batchURL := "/v1/runs/" + startedA.Run.ID + "/observations/batch"

	// The fixed clock sits one minute before UTC midnight, so this batch
	// crosses the day boundary in file order.
	crossMidnight := []map[string]any{
		observationPayload(1, 1.050, 20.0, time.Date(2026, 10, 2, 23, 59, 20, 0, time.UTC), "batch 1"),
		observationPayload(2, 1.036, 20.4, time.Date(2026, 10, 2, 23, 59, 40, 0, time.UTC), "batch 2"),
		observationPayload(3, 1.024, 20.1, time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC), "batch 3"),
	}
	batchResponse, err := api.call("POST", batchURL,
		map[string]any{"observations": crossMidnight}, ptrInt64(baseVersion))
	if err != nil {
		return err
	}
	if err := requireStatus(batchResponse, 200, "append cross-midnight batch"); err != nil {
		return err
	}
	var batched runView
	if err := batchResponse.decode(&batched); err != nil {
		return err
	}
	if batched.Version != baseVersion+3 || len(batched.Observations) != 3 {
		return errInvalidWorkflow("batch append did not advance version and observations together")
	}
	for index, observation := range batched.Observations {
		if observation.Sequence != index+1 {
			return errInvalidWorkflow("batch did not preserve contiguous sequences")
		}
		if index > 0 && !observation.ObservedAt.After(batched.Observations[index-1].ObservedAt) {
			return errInvalidWorkflow("batch did not preserve strictly increasing times")
		}
	}

	// The same records written one by one must produce identical run details.
	singleVersion := startedB.Run.Version
	for _, payload := range crossMidnight {
		response, err := api.call("POST", "/v1/runs/"+startedB.Run.ID+"/observations",
			payload, ptrInt64(singleVersion))
		if err != nil {
			return err
		}
		if err := requireStatus(response, 200, "append single observation"); err != nil {
			return err
		}
		var run runView
		if err := response.decode(&run); err != nil {
			return err
		}
		singleVersion = run.Version
	}
	if singleVersion != batched.Version {
		return errInvalidWorkflow("single writes and batch write produced different versions")
	}
	readB, err := readRun(api, startedB.Run.ID, "read single-write run")
	if err != nil {
		return err
	}
	if err := requireSameObservations(batched.Observations, readB.Observations); err != nil {
		return err
	}

	// Move the fixed clock forward so further observations fit the window.
	fixed.Advance(24 * time.Hour)
	dayThree := func(hour, minute int) time.Time {
		return time.Date(2026, 10, 3, hour, minute, 0, 0, time.UTC)
	}

	requireRunUnchanged := func(expectedVersion int64, expectedCount int) error {
		run, err := readRun(api, startedA.Run.ID, "read run after rejected batch")
		if err != nil {
			return err
		}
		if run.Version != expectedVersion || len(run.Observations) != expectedCount {
			return errInvalidWorkflow("rejected batch changed the persisted run")
		}
		return nil
	}

	rejections := []struct {
		name    string
		status  int
		code    domain.Code
		records []map[string]any
	}{
		{
			name:   "reject batch with out-of-range gravity",
			status: 400, code: domain.CodeInvalidInput,
			records: []map[string]any{
				observationPayload(4, 1.018, 20.0, dayThree(12, 0), "valid head"),
				observationPayload(5, 1.300, 20.0, dayThree(12, 10), "gravity too high"),
			},
		},
		{
			name:   "reject batch with out-of-range temperature",
			status: 400, code: domain.CodeInvalidInput,
			records: []map[string]any{
				observationPayload(4, 1.018, 20.0, dayThree(12, 0), "valid head"),
				observationPayload(5, 1.014, 70.0, dayThree(12, 10), "temperature too high"),
			},
		},
		{
			name:   "reject batch with rising gravity",
			status: 409, code: domain.CodeObservationConflict,
			records: []map[string]any{
				observationPayload(4, 1.018, 20.0, dayThree(12, 0), "valid head"),
				observationPayload(5, 1.030, 20.0, dayThree(12, 10), "gravity rise above 0.010"),
			},
		},
		{
			name:   "reject batch with time moving back across midnight",
			status: 409, code: domain.CodeObservationConflict,
			records: []map[string]any{
				observationPayload(4, 1.018, 20.0, time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC), "midnight"),
				observationPayload(5, 1.014, 20.0, time.Date(2026, 10, 3, 23, 59, 50, 0, time.UTC), "before midnight"),
			},
		},
		{
			name:   "reject batch with a sequence gap",
			status: 409, code: domain.CodeObservationConflict,
			records: []map[string]any{
				observationPayload(5, 1.018, 20.0, dayThree(12, 0), "skips sequence 4"),
			},
		},
		{
			name:    "reject an empty batch",
			status:  400,
			code:    domain.CodeInvalidInput,
			records: []map[string]any{},
		},
	}
	for _, rejection := range rejections {
		response, err := api.call("POST", batchURL,
			map[string]any{"observations": rejection.records}, ptrInt64(batched.Version))
		if err != nil {
			return err
		}
		if err := requireCode(response, rejection.status, string(rejection.code), rejection.name); err != nil {
			return err
		}
		if err := requireRunUnchanged(batched.Version, 3); err != nil {
			return err
		}
	}

	stale, err := api.call("POST", batchURL, map[string]any{
		"observations": []map[string]any{
			observationPayload(4, 1.018, 20.0, dayThree(12, 0), "stale version"),
		},
	}, ptrInt64(batched.Version+100))
	if err != nil {
		return err
	}
	if err := requireCode(stale, 412, string(domain.CodeRevisionConflict),
		"reject batch with stale version"); err != nil {
		return err
	}
	if err := requireRunUnchanged(batched.Version, 3); err != nil {
		return err
	}

	// A corrected retry after the rejected batches applies cleanly because no
	// partial write consumed the sequences.
	retry := []map[string]any{
		observationPayload(4, 1.018, 20.0, dayThree(12, 0), "retry 4"),
		observationPayload(5, 1.014, 20.0, dayThree(12, 10), "retry 5"),
	}
	retryResponse, err := api.call("POST", batchURL,
		map[string]any{"observations": retry}, ptrInt64(batched.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(retryResponse, 200, "retry corrected batch"); err != nil {
		return err
	}
	var retried runView
	if err := retryResponse.decode(&retried); err != nil {
		return err
	}
	if retried.Version != batched.Version+2 || len(retried.Observations) != 5 {
		return errInvalidWorkflow("corrected retry did not extend the run exactly once")
	}

	// Re-importing the same file is rejected deterministically and changes
	// nothing.
	duplicate, err := api.call("POST", batchURL,
		map[string]any{"observations": retry}, ptrInt64(retried.Version))
	if err != nil {
		return err
	}
	if err := requireCode(duplicate, 409, string(domain.CodeObservationConflict),
		"reject duplicate batch import"); err != nil {
		return err
	}
	if err := requireRunUnchanged(retried.Version, 5); err != nil {
		return err
	}

	// Mirror the retried records into the single-write run.
	for _, payload := range retry {
		response, err := api.call("POST", "/v1/runs/"+startedB.Run.ID+"/observations",
			payload, ptrInt64(singleVersion))
		if err != nil {
			return err
		}
		if err := requireStatus(response, 200, "append retried record singly"); err != nil {
			return err
		}
		var run runView
		if err := response.decode(&run); err != nil {
			return err
		}
		singleVersion = run.Version
	}
	if singleVersion != retried.Version {
		return errInvalidWorkflow("mirrored single writes diverged from the batch run")
	}

	finalA, err := completeCheckedRun(api, startedA.Run.ID, retried.Version)
	if err != nil {
		return err
	}
	finalB, err := completeCheckedRun(api, startedB.Run.ID, singleVersion)
	if err != nil {
		return err
	}
	if finalA.Run.Version != finalB.Run.Version {
		return errInvalidWorkflow("completed runs ended with different versions")
	}
	if err := requireRunState(finalA.Run, domain.RunCompleted); err != nil {
		return err
	}
	if err := requireVesselState(finalA.Vessel, domain.VesselCleaning); err != nil {
		return err
	}
	if err := requireVesselState(finalB.Vessel, domain.VesselCleaning); err != nil {
		return err
	}
	if err := requireSameObservations(finalA.Run.Observations, finalB.Run.Observations); err != nil {
		return err
	}
	if err := requireSameMetrics(finalA.Run.Metrics, finalB.Run.Metrics); err != nil {
		return err
	}
	if finalA.Run.Metrics.ObservationCount != 5 ||
		finalA.Run.Metrics.ApparentAttenuation <= 0 ||
		finalA.Run.Metrics.EstimatedAlcoholABV <= 0 {
		return errInvalidWorkflow("batch completion metrics were not calculated")
	}

	readBack, err := readRun(api, startedA.Run.ID, "read completed batch run")
	if err != nil {
		return err
	}
	if readBack.Version != finalA.Run.Version || len(readBack.Observations) != 5 ||
		readBack.Metrics == nil || *readBack.Metrics != *finalA.Run.Metrics {
		return errInvalidWorkflow("completed batch run lost observations or metrics")
	}
	return nil
}

func observationPayload(
	sequence int,
	gravity float64,
	temperature float64,
	observedAt time.Time,
	note string,
) map[string]any {
	return map[string]any{
		"sequence":      sequence,
		"gravity":       gravity,
		"temperature_c": temperature,
		"observed_at":   observedAt,
		"note":          note,
	}
}

func readRun(api *client, id string, operation string) (runView, error) {
	response, err := api.call("GET", "/v1/runs/"+id, nil, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, 200, operation); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func completeCheckedRun(api *client, id string, version int64) (pairView, error) {
	conditioningResponse, err := api.call("POST", "/v1/runs/"+id+"/conditioning",
		nil, ptrInt64(version))
	if err != nil {
		return pairView{}, err
	}
	if err := requireStatus(conditioningResponse, 200, "begin conditioning"); err != nil {
		return pairView{}, err
	}
	var conditioning runView
	if err := conditioningResponse.decode(&conditioning); err != nil {
		return pairView{}, err
	}
	if err := requireRunState(conditioning, domain.RunConditioning); err != nil {
		return pairView{}, err
	}
	completeResponse, err := api.call("POST", "/v1/runs/"+id+"/complete",
		nil, ptrInt64(conditioning.Version))
	if err != nil {
		return pairView{}, err
	}
	if err := requireStatus(completeResponse, 200, "complete run"); err != nil {
		return pairView{}, err
	}
	var completed pairView
	if err := completeResponse.decode(&completed); err != nil {
		return pairView{}, err
	}
	return completed, nil
}

func requireSameObservations(a, b []domain.GravityObservation) error {
	if len(a) != len(b) {
		return errInvalidWorkflow("observation counts differ between batch and single writes")
	}
	for index := range a {
		left, right := a[index], b[index]
		if left.Sequence != right.Sequence ||
			left.Gravity != right.Gravity ||
			left.TemperatureC != right.TemperatureC ||
			!left.ObservedAt.Equal(right.ObservedAt) ||
			left.Note != right.Note ||
			!left.ReceivedAt.Equal(right.ReceivedAt) {
			return fmt.Errorf("observation %d differs between batch and single writes", index+1)
		}
	}
	return nil
}

func requireSameMetrics(a, b *domain.RunMetrics) error {
	if a == nil || b == nil {
		return errInvalidWorkflow("completion metrics were not calculated")
	}
	if *a != *b {
		return errInvalidWorkflow("completion metrics differ between batch and single writes")
	}
	return nil
}
