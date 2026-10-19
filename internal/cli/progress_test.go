package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestCaptureStderr verifies the stray-fd capture: lines written straight to
// os.Stderr (as kustomize does for its deprecation warnings) are delivered to
// emit one per line — including a final line with no trailing newline, flushed
// at EOF — and os.Stderr is restored afterward. Not parallel: it swaps the
// process-wide os.Stderr.
func TestCaptureStderr(t *testing.T) {
	orig := os.Stderr
	var mu sync.Mutex
	var got []string
	restore := captureStderr(func(line string) {
		mu.Lock()
		got = append(got, line)
		mu.Unlock()
	})
	fmt.Fprintln(os.Stderr, "# Warning: 'commonLabels' is deprecated.")
	fmt.Fprint(os.Stderr, "tail-without-newline")
	restore()

	if os.Stderr != orig {
		t.Error("captureStderr did not restore os.Stderr")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"# Warning: 'commonLabels' is deprecated.", "tail-without-newline"}
	if !slices.Equal(got, want) {
		t.Errorf("captured lines = %q, want %q", got, want)
	}
}

// TestBarWriter_NoProgramWritesThrough: with no Bubble Tea program attached, the
// slog adapter is a plain passthrough to the underlying stderr (the non-TTY /
// pre-bar / post-bar path) — no stray control bytes.
func TestBarWriter_NoProgramWritesThrough(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := &barWriter{out: &buf}
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello\n" {
		t.Errorf("passthrough Write = %q, want bare line", got)
	}
}

// TestQueryFilterFile: the bar's output wrapper strips Bubble Tea's startup
// terminal probes (mode 2026/2027 DECRQM and the renderer's Kitty keyboard
// query) — whose replies nothing reads, since the program runs input-less —
// while passing every other byte through and reporting the full input length
// as written.
func TestQueryFilterFile(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "bar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := &queryFilterFile{File: f}

	// The Kitty *set* sequence (\x1b[=0;1u) rides in the same frame flush as
	// the query (\x1b[?u) and must survive the strip.
	in := []byte("\x1b[?2026$p\x1b[?2027$pframe \x1b[?2026h\x1b[=0;1u\x1b[?u+content")
	n, err := w.Write(in)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(in) {
		t.Errorf("Write returned n=%d, want full length %d", n, len(in))
	}
	// A flush that is nothing but probes writes no bytes at all.
	if _, err := w.Write([]byte("\x1b[?2026$p\x1b[?u")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	// The set sequences (2026h, =0;1u) and frame text survive; only the
	// query probes are dropped.
	if want := "frame \x1b[?2026h\x1b[=0;1u+content"; string(got) != want {
		t.Errorf("filtered output = %q, want %q", got, want)
	}
}

// TestWriterIsTTY_NonFile: buffers (the e2e harness, pipes-as-buffers) are
// never TTYs, so the bar stays off without a flag.
func TestWriterIsTTY_NonFile(t *testing.T) {
	t.Parallel()
	if writerIsTTY(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as a TTY")
	}
}

func TestProgressBarEnabled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                  string
		noProgress, stderrTTY bool
		want                  bool
	}{
		{"interactive", false, true, true},
		{"redirected stderr", false, false, false},
		{"disabled interactive", true, true, false},
		{"disabled redirected", true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := progressBarEnabled(tc.noProgress, tc.stderrTTY); got != tc.want {
				t.Errorf("progressBarEnabled(%v, %v) = %v, want %v", tc.noProgress, tc.stderrTTY, got, tc.want)
			}
		})
	}
}

type progressOutput struct {
	mu                              sync.Mutex
	data                            bytes.Buffer
	probe, release                  chan struct{}
	blocked, allowWrite             chan struct{}
	painted                         chan struct{}
	probeOnce, writeOnce, paintOnce sync.Once
	fail                            bool
}

func (o *progressOutput) Write(p []byte) (int, error) {
	if o.painted != nil {
		o.paintOnce.Do(func() { close(o.painted) })
	}
	if o.blocked != nil && bytes.Contains(p, []byte("\x1b[?25h")) {
		o.writeOnce.Do(func() { close(o.blocked); <-o.allowWrite })
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.data.Write(p)
}
func (o *progressOutput) String() string           { o.mu.Lock(); defer o.mu.Unlock(); return o.data.String() }
func (o *progressOutput) Read([]byte) (int, error) { return 0, io.EOF }
func (o *progressOutput) Close() error             { return nil }
func (o *progressOutput) Fd() uintptr {
	if o.probe != nil {
		o.probeOnce.Do(func() {
			close(o.probe)
			<-o.release
			if o.fail {
				panic("progress output initialization failed")
			}
		})
	}
	return ^uintptr(0)
}

func progressProgram(t *testing.T, w *barWriter) *tea.Program {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.prog
}

func TestBarWriter_ProgressLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		at                      time.Duration
		cancel, before, overlap bool
	}{
		{name: "short", at: 100 * time.Millisecond},
		{name: "canceled-before-start", cancel: true, before: true},
		{name: "canceled-during-delay", at: 100 * time.Millisecond, cancel: true},
		{name: "completion-overlap", at: progressDelay, overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{})}
				w := &barWriter{out: out}
				completed, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				complete := sync.OnceFunc(func() { close(completed) })
				shutdown := sync.OnceFunc(func() { close(finish) })
				release := sync.OnceFunc(func() { close(out.release) })
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.before {
					cancel()
				}
				m := newBarModel(false)
				start := time.Now()
				go func() { defer close(done); w.runProgress(ctx, m, completed, finish) }()
				defer func() { complete(); shutdown(); release(); <-done; time.Sleep(time.Second) }()
				time.Sleep(tc.at)
				wantOutput := ""
				if tc.name == "short" {
					assert.Equal(t, out.String(), "")
					_, err := w.Write([]byte("explicit-during-delay\n"))
					assert.NoError(t, err)
					wantOutput = "explicit-during-delay\n"
				}
				if tc.cancel {
					cancel()
				} else {
					complete()
				}
				shutdown()
				release()
				<-done
				assert.Equal(t, time.Since(start), tc.at)
				if progressProgram(t, w) != nil {
					t.Error("program survived lifecycle return")
				}
				if !tc.overlap {
					select {
					case <-out.probe:
						t.Error("suppressed run initialized terminal output")
					default:
					}
					assert.Equal(t, out.String(), wantOutput)
				}
				before := out.String()
				time.Sleep(progressDelay + time.Second)
				assert.Equal(t, out.String(), before)
				if progressProgram(t, w) != nil {
					t.Error("program started after lifecycle return")
				}
			})
		})
	}
	t.Run("long", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{})}
			w := &barWriter{out: out}
			m := newBarModel(false)
			a, b, c := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c")
			m.events.publish(a, store.StatusPending, time.Now())
			m.events.publish(a, store.StatusReady, time.Now())
			m.events.publish(b, store.StatusPending, time.Now())
			completed, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			complete := sync.OnceFunc(func() { close(completed) })
			shutdown := sync.OnceFunc(func() { close(finish) })
			release := sync.OnceFunc(func() { close(out.release) })
			go func() { defer close(done); w.runProgress(t.Context(), m, completed, finish) }()
			defer func() { complete(); shutdown(); release(); <-done; time.Sleep(time.Second) }()
			time.Sleep(progressDelay - time.Nanosecond)
			synctest.Wait()
			if progressProgram(t, w) != nil {
				t.Error("program attached before deadline")
			}
			assert.Equal(t, out.String(), "")
			time.Sleep(time.Nanosecond)
			<-out.probe
			p := progressProgram(t, w)
			if p == nil {
				t.Fatal("long run never attached a program")
			}
			assert.Equal(t, len(m.first), 2)
			assert.Equal(t, len(m.printed), 1)
			if m.events.head.Load() != nil {
				t.Fatal("program constructed before initial status drain")
			}
			m.events.publish(c, store.StatusPending, time.Now())
			release()
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			if _, seen := m.first[c]; !seen || m.events.head.Load() != nil {
				t.Error("active spinner did not drain new statuses")
			}
			if progressProgram(t, w) != p {
				t.Error("long run replaced its program")
			}
			complete()
			shutdown()
			<-done
			if progressProgram(t, w) != nil {
				t.Error("long run did not detach")
			}
			before := out.String()
			time.Sleep(time.Second)
			assert.Equal(t, out.String(), before)
		})
	})
	t.Run("cleanup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{}), blocked: make(chan struct{}), allowWrite: make(chan struct{}), painted: make(chan struct{})}
			w := &barWriter{out: out}
			m := newBarModel(false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			completed, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			complete := sync.OnceFunc(func() { close(completed) })
			shutdown := sync.OnceFunc(func() { close(finish) })
			release := sync.OnceFunc(func() { close(out.release) })
			write := sync.OnceFunc(func() { close(out.allowWrite) })
			go func() { defer close(done); w.runProgress(ctx, m, completed, finish) }()
			defer func() {
				complete()
				shutdown()
				release()
				write()
				<-done
				time.Sleep(time.Second)
			}()
			time.Sleep(progressDelay)
			<-out.probe
			p := progressProgram(t, w)
			release()
			<-out.painted
			cancel()
			synctest.Wait()
			if progressProgram(t, w) != p {
				t.Error("cancellation killed progress before render cleanup")
			}
			complete()
			shutdown()
			<-out.blocked
			synctest.Wait()
			select {
			case <-done:
				t.Error("lifecycle returned while program output was blocked")
			default:
			}
			write()
			<-done
			if progressProgram(t, w) != nil {
				t.Error("cleanup left the program attached")
			}
			before := out.String()
			time.Sleep(time.Second)
			assert.Equal(t, out.String(), before)
		})
	})
	t.Run("log-tail", func(t *testing.T) {
		out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{})}
		w := &barWriter{out: out}
		tail := make(chan bool, 1)
		oldLogger := slog.Default()
		defer slog.SetDefault(oldLogger)
		slog.SetDefault(slog.New(slog.NewTextHandler(progressLogWriter{bar: w, tail: tail}, &slog.HandlerOptions{Level: slog.LevelDebug})))
		original := os.Stderr
		st := store.New()
		cleanup := w.progress(t.Context(), st)
		release := sync.OnceFunc(func() { close(out.release) })
		defer release()
		<-out.probe
		id := testutil.KustomizationID("ns", "render")
		st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		st.UpdateStatus(id, store.StatusReady, "during initialization")
		release()
		slog.Debug("explicit-active")
		var writers sync.WaitGroup
		for _, line := range []string{"dependency-a", "dependency-b"} {
			writers.Go(func() { fmt.Fprintln(os.Stderr, line) })
		}
		writers.Wait()
		fmt.Fprint(os.Stderr, "dependency-tail")
		cleanup()
		if os.Stderr != original {
			t.Error("cleanup did not restore stderr")
		}
		if !<-tail {
			t.Error("capture tail drained after program detach")
		}
		if progressProgram(t, w) != nil {
			t.Error("capture cleanup left a program")
		}
		for _, line := range []string{"explicit-active", "dependency-a", "dependency-b", "dependency-tail"} {
			if !strings.Contains(out.String(), line) {
				t.Errorf("log output missing %q: %q", line, out.String())
			}
		}
		st.UpdateStatus(id, store.StatusFailed, "after cleanup")
	})
}

type progressLogWriter struct {
	bar  *barWriter
	tail chan<- bool
}

func (w progressLogWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("dependency-tail")) {
		w.bar.mu.Lock()
		active := w.bar.prog != nil
		w.bar.mu.Unlock()
		w.tail <- active
	}
	return w.bar.Write(p)
}

func TestBarWriter_ProgramExitLogging(t *testing.T) {
	t.Setenv("TEA_DEBUG", "false")
	t.Run("stale-pointer", func(t *testing.T) {
		original := os.Stderr
		quiet, err := os.CreateTemp(t.TempDir(), "program-error")
		assert.NoError(t, err)
		os.Stderr = quiet
		defer func() { os.Stderr = original; _ = quiet.Close() }()
		synctest.Test(t, func(t *testing.T) {
			out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{}), fail: true}
			w := &barWriter{out: out}
			m := newBarModel(false)
			m.startedAt = time.Now().Add(-progressDelay)
			completed, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			complete := sync.OnceFunc(func() { close(completed) })
			shutdown := sync.OnceFunc(func() { close(finish) })
			release := sync.OnceFunc(func() { close(out.release) })
			go func() { defer close(done); w.runProgress(t.Context(), m, completed, finish) }()
			defer func() { complete(); shutdown(); release(); <-done; time.Sleep(time.Second) }()
			<-out.probe
			written := make(chan struct{})
			go func() { defer close(written); _, _ = w.Write([]byte("captured-pointer\n")) }()
			synctest.Wait()
			select {
			case <-written:
				t.Error("pre-Run send did not wait for initialization")
			default:
			}
			release()
			<-done
			<-written
			if progressProgram(t, w) != nil {
				t.Fatal("early Run exit did not detach")
			}
			_, err := w.Write([]byte("explicit-after-exit\n"))
			assert.NoError(t, err)
			if !strings.Contains(out.String(), "explicit-after-exit") {
				t.Fatal("post-exit log did not write through")
			}
		})
	})
	t.Run("captured-tail", func(t *testing.T) {
		out := &progressOutput{probe: make(chan struct{}), release: make(chan struct{}), fail: true}
		w := &barWriter{out: out}
		oldLogger := slog.Default()
		defer slog.SetDefault(oldLogger)
		slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})))
		original := os.Stderr
		restore := captureStderr(func(line string) { slog.Debug(line) })
		m := newBarModel(false)
		m.startedAt = time.Now().Add(-progressDelay)
		completed, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() { defer close(done); w.runProgress(t.Context(), m, completed, finish) }()
		<-out.probe
		close(out.release)
		<-done
		if progressProgram(t, w) != nil {
			t.Error("program remained attached after Run exit")
		}
		slog.Debug("explicit-after-exit")
		fmt.Fprint(os.Stderr, "dependency-tail")
		close(completed)
		restore()
		close(finish)
		if os.Stderr != original {
			t.Error("failed program prevented stderr restoration")
		}
		for _, line := range []string{"explicit-after-exit", "dependency-tail"} {
			if !strings.Contains(out.String(), line) {
				t.Errorf("post-exit log missing %q", line)
			}
		}
	})
}

func TestProgressEvents_ConcurrentDrain(t *testing.T) {
	t.Parallel()
	var events progressEvents
	start, overlap := make(chan struct{}), make(chan struct{})
	firstDone, secondDone := make(chan struct{}, 2), make(chan struct{}, 2)
	first := time.Unix(1, 0)
	for _, name := range []string{"a", "b"} {
		go func() {
			<-start
			id := testutil.KustomizationID("ns", name)
			events.publish(id, store.StatusPending, first)
			events.publish(id, store.StatusReady, first.Add(time.Second))
			firstDone <- struct{}{}
			<-overlap
			id.Name += "-next"
			events.publish(id, store.StatusPending, first.Add(2*time.Second))
			events.publish(id, store.StatusFailed, first.Add(3*time.Second))
			secondDone <- struct{}{}
		}()
	}
	close(start)
	for range 2 {
		<-firstDone
	}
	firstBatch := events.take()
	detached := make(chan *progressEvent, 1)
	go func() { <-overlap; detached <- events.take() }()
	close(overlap)
	for range 2 {
		<-secondDone
	}
	secondBatch := <-detached
	lastBatch := events.take()
	statuses := map[string][]store.Status{}
	seen := map[*progressEvent]bool{}
	for _, batch := range []*progressEvent{firstBatch, secondBatch, lastBatch} {
		for event := batch; event != nil; event = event.next {
			if seen[event] {
				t.Fatal("event reused or detached links corrupted")
			}
			seen[event] = true
			statuses[event.id.Name] = append(statuses[event.id.Name], event.status)
		}
	}
	assert.Diff(t, statuses, map[string][]store.Status{
		"a": {store.StatusPending, store.StatusReady}, "b": {store.StatusPending, store.StatusReady},
		"a-next": {store.StatusPending, store.StatusFailed}, "b-next": {store.StatusPending, store.StatusFailed},
	})
	if events.take() != nil {
		t.Error("drained chain retained events")
	}
}
