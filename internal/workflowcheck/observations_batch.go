package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// observationBatchNow pins the service clock just before a UTC midnight so
// the batch check files records across the day boundary deterministically.
var observationBatchNow = time.Date(2026, 10, 2, 23, 59, 30, 0, time.UTC)

// batchTime returns the instant offset seconds after observationBatchNow.
// Offsets from 0 to 60 stay inside the accepted observation window, and
// offsets past 30 land on the following UTC day.
func batchTime(offset int) time.Time {
	return observationBatchNow.Add(time.Duration(offset) * time.Second)
}

type batchRecord struct {
	sequence    int
	gravity     float64
	temperature float64
	observedAt  time.Time
	note        string
}

func batchPayload(records []batchRecord) map[string]any {
	items := make([]map[string]any, 0, len(records))
	for _, record := range records {
		items = append(items, map[string]any{
			"sequence":      record.sequence,
			"gravity":       record.gravity,
			"temperature_c": record.temperature,
			"observed_at":   record.observedAt,
			"note":          record.note,
		})
	}
	return map[string]any{"observations": items}
}

func observationsBatch(api *client) error {
	formula, err := ensureApprovedFormula(api, "Batch Formula")
	if err != nil {
		return err
	}

	// The same three file rows are imported once as a single batch and once
	// record by record. Row three lands after the UTC midnight and carries a
	// non-UTC offset in the batch file to prove that ordering and storage
	// normalize to UTC.
	rows := []batchRecord{
		{sequence: 1, gravity: 1.050, temperature: 20.0, observedAt: batchTime(5), note: "file row 1"},
		{sequence: 2, gravity: 1.028, temperature: 20.5, observedAt: batchTime(28), note: "file row 2"},
		{sequence: 3, gravity: 1.014, temperature: 19.8, observedAt: batchTime(45), note: "file row 3"},
	}
	offsetRows := append([]batchRecord(nil), rows...)
	offsetRows[2].observedAt = rows[2].observedAt.In(time.FixedZone("UTC+02:00", 2*60*60))

	batchVessel, err := createCheckVessel(api, "VSL-301", 300)
	if err != nil {
		return err
	}
	batchStarted, err := createStartedRun(api, formula.ID, batchVessel.ID, "RUN-BATCH-FILE")
	if err != nil {
		return err
	}
	batchResponse, err := api.call("POST", "/v1/runs/"+batchStarted.Run.ID+"/observations/batch",
		batchPayload(offsetRows), ptrInt64(batchStarted.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(batchResponse, 200, "import observation batch"); err != nil {
		return err
	}
	var batchRun runView
	if err := batchResponse.decode(&batchRun); err != nil {
		return err
	}
	if batchRun.Version != batchStarted.Run.Version+int64(len(rows)) {
		return errInvalidWorkflow("batch import did not advance the run version per record")
	}
	if len(batchRun.Observations) != len(rows) {
		return errInvalidWorkflow("batch import did not store every record")
	}
	beforeMidnight := batchRun.Observations[1].ObservedAt.UTC()
	afterMidnight := batchRun.Observations[2].ObservedAt.UTC()
	if beforeMidnight.Day() == afterMidnight.Day() || !afterMidnight.After(beforeMidnight) {
		return errInvalidWorkflow("batch import did not preserve the cross-midnight order")
	}
	if !afterMidnight.Equal(batchTime(45)) {
		return errInvalidWorkflow("batch import did not normalize the offset timestamp to UTC")
	}

	singleVessel, err := createCheckVessel(api, "VSL-302", 300)
	if err != nil {
		return err
	}
	singleStarted, err := createStartedRun(api, formula.ID, singleVessel.ID, "RUN-BATCH-SINGLE")
	if err != nil {
		return err
	}
	singleRun := singleStarted.Run
	for _, row := range rows {
		response, err := api.call("POST", "/v1/runs/"+singleStarted.Run.ID+"/observations",
			map[string]any{
				"sequence":      row.sequence,
				"gravity":       row.gravity,
				"temperature_c": row.temperature,
				"observed_at":   row.observedAt,
				"note":          row.note,
			}, nil)
		if err != nil {
			return err
		}
		if err := requireStatus(response, 200, "append single observation"); err != nil {
			return err
		}
		if err := response.decode(&singleRun); err != nil {
			return err
		}
	}
	if err := requireSameRunResult(batchRun, singleRun); err != nil {
		return err
	}

	batchFinal, err := finishCheckedRun(api, batchRun)
	if err != nil {
		return err
	}
	singleFinal, err := finishCheckedRun(api, singleRun)
	if err != nil {
		return err
	}
	if batchFinal.Version != singleFinal.Version {
		return errInvalidWorkflow("batch and single imports ended on different run versions")
	}
	if batchFinal.Metrics == nil || singleFinal.Metrics == nil ||
		*batchFinal.Metrics != *singleFinal.Metrics {
		return errInvalidWorkflow("batch and single imports produced different metrics")
	}
	if batchFinal.Metrics.ObservationCount != 3 || batchFinal.Metrics.FinalGravity != 1.01 {
		return errInvalidWorkflow("unexpected completion metrics after batch import")
	}

	// A guard run proves that a rejected batch is all-or-nothing: every
	// failing file leaves the stored run untouched, a corrected file retries
	// cleanly, and re-importing a committed file fails deterministically.
	guardVessel, err := createCheckVessel(api, "VSL-303", 300)
	if err != nil {
		return err
	}
	guardStarted, err := createStartedRun(api, formula.ID, guardVessel.ID, "RUN-BATCH-GUARD")
	if err != nil {
		return err
	}
	committed := []batchRecord{
		{sequence: 1, gravity: 1.048, temperature: 19.5, observedAt: batchTime(3), note: "guard row 1"},
		{sequence: 2, gravity: 1.036, temperature: 19.2, observedAt: batchTime(20), note: "guard row 2"},
		{sequence: 3, gravity: 1.030, temperature: 19.0, observedAt: batchTime(38), note: "guard row 3"},
	}
	guardResponse, err := api.call("POST", "/v1/runs/"+guardStarted.Run.ID+"/observations/batch",
		batchPayload(committed), ptrInt64(guardStarted.Run.Version))
	if err != nil {
		return err
	}
	if err := requireStatus(guardResponse, 200, "import guard batch"); err != nil {
		return err
	}
	var guardRun runView
	if err := guardResponse.decode(&guardRun); err != nil {
		return err
	}
	if guardRun.Version != guardStarted.Run.Version+int64(len(committed)) {
		return errInvalidWorkflow("guard batch did not advance the run version per record")
	}

	rejections := []struct {
		name       string
		payload    map[string]any
		version    *int64
		wantStatus int
		wantCode   domain.Code
	}{
		{"out-of-range gravity in the middle record", batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.028, temperature: 19.0, observedAt: batchTime(40)},
			{sequence: 5, gravity: 1.400, temperature: 19.0, observedAt: batchTime(44)},
			{sequence: 6, gravity: 1.026, temperature: 19.0, observedAt: batchTime(48)},
		}), nil, 400, domain.CodeInvalidInput},
		{"out-of-range temperature", batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.028, temperature: 75.0, observedAt: batchTime(40)},
		}), nil, 400, domain.CodeInvalidInput},
		{"stale expected version", batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.028, temperature: 19.0, observedAt: batchTime(40)},
		}), ptrInt64(guardRun.Version - 1), 412, domain.CodeRevisionConflict},
		{"non-contiguous sequence", batchPayload([]batchRecord{
			{sequence: 5, gravity: 1.028, temperature: 19.0, observedAt: batchTime(40)},
		}), nil, 409, domain.CodeObservationConflict},
		{"repeated timestamp inside the batch", batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.028, temperature: 19.0, observedAt: batchTime(40)},
			{sequence: 5, gravity: 1.027, temperature: 19.0, observedAt: batchTime(40)},
		}), nil, 409, domain.CodeObservationConflict},
		{"gravity rising above the committed value", batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.045, temperature: 19.0, observedAt: batchTime(40)},
		}), nil, 409, domain.CodeObservationConflict},
		{"empty file", map[string]any{"observations": []any{}}, nil, 400, domain.CodeInvalidInput},
	}
	for _, rejection := range rejections {
		response, err := api.call("POST", "/v1/runs/"+guardRun.ID+"/observations/batch",
			rejection.payload, rejection.version)
		if err != nil {
			return err
		}
		if err := requireCode(response, rejection.wantStatus, string(rejection.wantCode),
			"reject batch with "+rejection.name); err != nil {
			return err
		}
	}

	afterRejections, err := readCheckedRun(api, guardRun.ID)
	if err != nil {
		return err
	}
	if afterRejections.Version != guardRun.Version ||
		len(afterRejections.Observations) != len(committed) {
		return errInvalidWorkflow("a rejected batch changed the stored run")
	}

	corrected := []batchRecord{
		{sequence: 4, gravity: 1.028, temperature: 19.0, observedAt: batchTime(40), note: "corrected row 4"},
		{sequence: 5, gravity: 1.027, temperature: 19.0, observedAt: batchTime(44), note: "corrected row 5"},
		{sequence: 6, gravity: 1.026, temperature: 19.0, observedAt: batchTime(48), note: "corrected row 6"},
	}
	retryResponse, err := api.call("POST", "/v1/runs/"+guardRun.ID+"/observations/batch",
		batchPayload(corrected), ptrInt64(guardRun.Version))
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
	if retried.Version != guardRun.Version+int64(len(corrected)) ||
		len(retried.Observations) != len(committed)+len(corrected) {
		return errInvalidWorkflow("corrected retry did not append exactly its own records")
	}
	for index, row := range corrected {
		stored := retried.Observations[len(committed)+index]
		if stored.Sequence != row.sequence || stored.Gravity != row.gravity ||
			!stored.ObservedAt.Equal(row.observedAt) {
			return fmt.Errorf("retried batch stored record %d out of file order", row.sequence)
		}
	}

	repeatResponse, err := api.call("POST", "/v1/runs/"+guardRun.ID+"/observations/batch",
		batchPayload(corrected), nil)
	if err != nil {
		return err
	}
	if err := requireCode(repeatResponse, 409, string(domain.CodeObservationConflict),
		"re-import the same file"); err != nil {
		return err
	}
	staleRepeat, err := api.call("POST", "/v1/runs/"+guardRun.ID+"/observations/batch",
		batchPayload(corrected), ptrInt64(guardRun.Version))
	if err != nil {
		return err
	}
	if err := requireCode(staleRepeat, 412, string(domain.CodeRevisionConflict),
		"re-import with a stale version"); err != nil {
		return err
	}
	afterRepeats, err := readCheckedRun(api, guardRun.ID)
	if err != nil {
		return err
	}
	if afterRepeats.Version != retried.Version ||
		len(afterRepeats.Observations) != len(retried.Observations) {
		return errInvalidWorkflow("a repeated import changed the stored run")
	}

	missingResponse, err := api.call("POST", "/v1/runs/run_missing/observations/batch",
		batchPayload(corrected), nil)
	if err != nil {
		return err
	}
	if err := requireCode(missingResponse, 404, string(domain.CodeRunNotFound),
		"import into a missing run"); err != nil {
		return err
	}
	closedResponse, err := api.call("POST", "/v1/runs/"+batchFinal.ID+"/observations/batch",
		batchPayload([]batchRecord{
			{sequence: 4, gravity: 1.012, temperature: 19.0, observedAt: batchTime(50)},
		}), nil)
	if err != nil {
		return err
	}
	if err := requireCode(closedResponse, 409, string(domain.CodeRunState),
		"import into a completed run"); err != nil {
		return err
	}
	return nil
}

func requireSameRunResult(batchRun, singleRun runView) error {
	if batchRun.Version != singleRun.Version || batchRun.State != singleRun.State {
		return errInvalidWorkflow("batch and single imports produced different runs")
	}
	if len(batchRun.Observations) != len(singleRun.Observations) {
		return errInvalidWorkflow("batch and single imports stored different observation counts")
	}
	for index := range batchRun.Observations {
		fromBatch := batchRun.Observations[index]
		fromSingle := singleRun.Observations[index]
		if fromBatch.Sequence != fromSingle.Sequence ||
			fromBatch.Gravity != fromSingle.Gravity ||
			fromBatch.TemperatureC != fromSingle.TemperatureC ||
			!fromBatch.ObservedAt.Equal(fromSingle.ObservedAt) ||
			fromBatch.Note != fromSingle.Note {
			return fmt.Errorf("observation %d differs between batch and single imports", index+1)
		}
	}
	return nil
}

func finishCheckedRun(api *client, run runView) (runView, error) {
	conditioningResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/conditioning",
		nil, ptrInt64(run.Version))
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(conditioningResponse, 200, "begin conditioning"); err != nil {
		return runView{}, err
	}
	var conditioning runView
	if err := conditioningResponse.decode(&conditioning); err != nil {
		return runView{}, err
	}
	completeResponse, err := api.call("POST", "/v1/runs/"+run.ID+"/complete",
		nil, ptrInt64(conditioning.Version))
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(completeResponse, 200, "complete run"); err != nil {
		return runView{}, err
	}
	var completed pairView
	if err := completeResponse.decode(&completed); err != nil {
		return runView{}, err
	}
	if err := requireRunState(completed.Run, domain.RunCompleted); err != nil {
		return runView{}, err
	}
	return completed.Run, nil
}

func readCheckedRun(api *client, id string) (runView, error) {
	response, err := api.call("GET", "/v1/runs/"+id, nil, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, 200, "read run"); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}
