package service

import "github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"

func (s *Service) AppendObservation(
	id string,
	input ObservationInput,
) (domain.FermentationRun, error) {
	return s.AppendObservationBatch(id, ObservationBatchInput{
		Observations: []ObservationInput{input},
	})
}

// AppendObservationBatch applies a file of observations in file order as one
// atomic write. Every record follows the same domain rules as a single
// append, so a rejected record discards the whole batch and a committed batch
// leaves the run exactly as record-by-record writes would.
func (s *Service) AppendObservationBatch(
	id string,
	input ObservationBatchInput,
) (domain.FermentationRun, error) {
	if len(input.Observations) == 0 {
		return domain.FermentationRun{}, domain.FieldError(domain.CodeInvalidInput,
			"observations", "must contain at least one record")
	}
	var updated domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		receivedAt := s.clock.Now()
		for _, record := range input.Observations {
			observation := domain.GravityObservation{
				RunID:        run.ID,
				Sequence:     record.Sequence,
				Gravity:      record.Gravity,
				TemperatureC: record.TemperatureC,
				ObservedAt:   record.ObservedAt.UTC(),
				Note:         record.Note,
				ReceivedAt:   receivedAt,
			}
			if err := run.AddObservation(observation); err != nil {
				return err
			}
		}
		snapshot.Runs[id] = run.Clone()
		updated = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return updated, nil
}

func (s *Service) BeginConditioning(
	id string,
	input VersionInput,
) (domain.FermentationRun, error) {
	var updated domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if err := run.BeginConditioning(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		updated = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return updated, nil
}

func (s *Service) CompleteRun(
	id string,
	input VersionInput,
) (domain.FermentationRun, domain.Vessel, error) {
	var updatedRun domain.FermentationRun
	var updatedVessel domain.Vessel
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		formula, ok := snapshot.Formulas[run.FormulaID]
		if !ok {
			return domain.NewError(domain.CodeFormulaNotFound, "attached formula was not found")
		}
		if err := run.Complete(formula.TargetOriginalGravity, s.clock.Now()); err != nil {
			return err
		}
		if run.VesselID == nil {
			return domain.NewError(domain.CodeRunState, "run has no vessel")
		}
		vessel, ok := snapshot.Vessels[*run.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := vessel.StartCleaning(run.ID, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		updatedRun = run.Clone()
		updatedVessel = vessel
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, err
	}
	return updatedRun, updatedVessel, nil
}
