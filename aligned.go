package eventorder

import (
	"slices"
	"sync"
	"time"
)

// Aligned combines the watermarks of several sources into one. Events from
// partitions, devices or upstream services arrive on separate streams, each
// with its own progress. Releasing events by the fastest stream's clock makes
// the slower streams' events late, so the safe watermark is the minimum over
// the sources: nothing earlier is expected from any of them.
//
// Report each source's watermark with Observe, for example the value of that
// source's Buffer.Watermark, and feed the result to the Buffer that orders the
// merged stream with AdvanceTo. A source that falls silent would hold the
// minimum forever, so it can be marked Idle by hand, or automatically with
// WithSourceIdleTimeout, and is left out until it reports again. Lag shows
// which source is holding the watermark back.
//
// The combined watermark never moves backwards. A source that rejoins behind
// it, or is added behind it, is not waited for: its events older than the
// combined watermark are late in the downstream Buffer, as for any late event.
// A source that has been registered but has not reported yet blocks the
// watermark until it reports, goes idle or is removed.
//
// Aligned is safe for concurrent use.
type Aligned[S comparable] struct {
	mu       sync.Mutex
	timeout  time.Duration
	now      func() time.Time
	sources  map[S]*alignedSource
	seq      uint64
	combined time.Time
	has      bool
}

type alignedSource struct {
	wm       time.Time
	has      bool
	idle     bool      // marked idle by hand
	lastSeen time.Time // clock time of the last report, or of registration
	seq      uint64
}

// AlignedOption configures an Aligned.
type AlignedOption func(*alignedOptions)

type alignedOptions struct {
	timeout time.Duration
	now     func() time.Time
}

// WithSourceIdleTimeout leaves a source out of the minimum once it has not
// reported for d of wall-clock time, so a stalled source does not hold the
// watermark. It counts from the source's last Observe, or from Register for a
// source that never reported. The default, zero, never times out.
func WithSourceIdleTimeout(d time.Duration) AlignedOption {
	return func(o *alignedOptions) { o.timeout = d }
}

// WithAlignedClock replaces time.Now as the wall clock used for the idle
// timeout, for tests.
func WithAlignedClock(now func() time.Time) AlignedOption {
	return func(o *alignedOptions) { o.now = now }
}

// SourceStatus is one source's state in the report from Lag.
type SourceStatus[S comparable] struct {
	Source S
	// Watermark is the source's latest reported watermark; zero when Reported
	// is false.
	Watermark time.Time
	Reported  bool
	// Behind is how far the source's watermark is behind the newest watermark
	// among the active sources.
	Behind time.Duration
	// Idle is true when the source is left out of the minimum, by hand or by
	// timeout.
	Idle bool
	// Holding is true when the source is what keeps the combined watermark
	// from advancing: active and either not yet reported or alone at the
	// minimum behind some other active source.
	Holding bool
}

// NewAligned returns an Aligned with no sources.
func NewAligned[S comparable](opts ...AlignedOption) *Aligned[S] {
	o := alignedOptions{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	if o.timeout < 0 {
		o.timeout = 0
	}
	return &Aligned[S]{timeout: o.timeout, now: o.now, sources: make(map[S]*alignedSource)}
}

// Register declares a source before it reports, so the combined watermark
// waits for it. Observe registers a source on first use, which is too late to
// make the others wait. Registering a known source does nothing.
func (a *Aligned[S]) Register(src S) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.get(src)
}

// Observe records watermark wm for src and returns the combined watermark.
// advanced is true when the combined watermark moved up because of this call;
// then pass the returned value to Buffer.AdvanceTo. ok is false while no
// combined watermark exists yet. A watermark lower than the source's previous
// one is ignored, and a report from an idle source makes it active again.
func (a *Aligned[S]) Observe(src S, wm time.Time) (combined time.Time, advanced, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.get(src)
	if !s.has || wm.After(s.wm) {
		s.wm, s.has = wm, true
	}
	s.idle = false
	s.lastSeen = a.now()
	return a.recompute()
}

// Idle leaves src out of the minimum until it next reports with Observe. It
// registers src if it is unknown. Returns as Observe does: leaving out a slow
// source can advance the combined watermark.
func (a *Aligned[S]) Idle(src S) (combined time.Time, advanced, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.get(src).idle = true
	return a.recompute()
}

// Remove forgets src, for a partition that was revoked or a device that left.
// Returns as Observe does.
func (a *Aligned[S]) Remove(src S) (combined time.Time, advanced, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sources, src)
	return a.recompute()
}

// Watermark returns the combined watermark. Idle timeouts are evaluated here,
// so a call after a quiet period can advance it.
func (a *Aligned[S]) Watermark() (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, _, ok := a.recompute()
	return w, ok
}

// Len is the number of sources known, active or not.
func (a *Aligned[S]) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.sources)
}

// Lag reports every source, the ones holding the watermark back first, then by
// watermark from lowest to highest.
func (a *Aligned[S]) Lag() []SourceStatus[S] {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	newest, min, blocked := a.bounds(now)
	type row struct {
		st  SourceStatus[S]
		seq uint64
	}
	rows := make([]row, 0, len(a.sources))
	for k, s := range a.sources {
		act := a.active(s, now)
		st := SourceStatus[S]{Source: k, Watermark: s.wm, Reported: s.has, Idle: !act}
		switch {
		case act && !s.has:
			st.Holding = true
		case act:
			st.Behind = newest.Sub(s.wm)
			st.Holding = !blocked && s.wm.Equal(min) && st.Behind > 0
		}
		rows = append(rows, row{st, s.seq})
	}
	slices.SortFunc(rows, func(x, y row) int {
		if c := lagOrder(x.st, y.st); c != 0 {
			return c
		}
		return cmpSeq(x.seq, y.seq)
	})
	out := make([]SourceStatus[S], len(rows))
	for i, r := range rows {
		out[i] = r.st
	}
	return out
}

// bounds returns the newest and the lowest watermark among the active sources
// that have reported, and whether an active source has not reported yet.
func (a *Aligned[S]) bounds(now time.Time) (newest, min time.Time, blocked bool) {
	var haveNewest, haveMin bool
	for _, s := range a.sources {
		if !a.active(s, now) {
			continue
		}
		if !s.has {
			blocked = true
			continue
		}
		if !haveNewest || s.wm.After(newest) {
			newest, haveNewest = s.wm, true
		}
		if !haveMin || s.wm.Before(min) {
			min, haveMin = s.wm, true
		}
	}
	return newest, min, blocked
}

// lagOrder puts the sources holding the watermark back first, then the ones
// that have not reported, then by watermark from lowest to highest.
func lagOrder[S comparable](x, y SourceStatus[S]) int {
	if x.Holding != y.Holding {
		if x.Holding {
			return -1
		}
		return 1
	}
	if x.Reported != y.Reported {
		if !x.Reported {
			return -1
		}
		return 1
	}
	return x.Watermark.Compare(y.Watermark)
}

func cmpSeq(x, y uint64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

func (a *Aligned[S]) get(src S) *alignedSource {
	s, ok := a.sources[src]
	if !ok {
		a.seq++
		s = &alignedSource{lastSeen: a.now(), seq: a.seq}
		a.sources[src] = s
	}
	return s
}

func (a *Aligned[S]) active(s *alignedSource, now time.Time) bool {
	if s.idle {
		return false
	}
	return a.timeout == 0 || now.Sub(s.lastSeen) <= a.timeout
}

// recompute takes the minimum over the active sources. It does not move while
// an active source has not reported, or when no source is active, and never
// moves down.
func (a *Aligned[S]) recompute() (time.Time, bool, bool) {
	now := a.now()
	var min time.Time
	have := false
	for _, s := range a.sources {
		if !a.active(s, now) {
			continue
		}
		if !s.has {
			return a.combined, false, a.has
		}
		if !have || s.wm.Before(min) {
			min, have = s.wm, true
		}
	}
	if have && (!a.has || min.After(a.combined)) {
		a.combined, a.has = min, true
		return a.combined, true, true
	}
	return a.combined, false, a.has
}
