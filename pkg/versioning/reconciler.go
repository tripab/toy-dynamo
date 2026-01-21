package versioning

// Reconciler defines the interface for conflict resolution
type Reconciler interface {
	Resolve(values []VersionedValue) []byte
}

// LastWriteWinsReconciler uses timestamp-based resolution
type LastWriteWinsReconciler struct{}

func (r *LastWriteWinsReconciler) Resolve(values []VersionedValue) []byte {
	if len(values) == 0 {
		return nil
	}

	latest := values[0]
	for _, v := range values[1:] {
		if v.VectorClock.Timestamp.After(latest.VectorClock.Timestamp) {
			latest = v
		}
	}

	return latest.Data
}

// ApplicationReconciler allows custom application logic
type ApplicationReconciler struct {
	ResolveFn func([]VersionedValue) []byte
}

func (r *ApplicationReconciler) Resolve(values []VersionedValue) []byte {
	if r.ResolveFn != nil {
		return r.ResolveFn(values)
	}
	return values[0].Data
}
