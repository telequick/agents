package agents

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"strconv"
	"time"
)

// AgentConfig is everything the worker needs to reach the engine and identify
// itself.
type AgentConfig struct {
	Host       string
	AppKey     string
	AppSecret  string
	AgentName  string
	Port       int  // default 443
	SampleRate int  // default 8000 — PSTN legs are 8 kHz
	Verify     bool // default true; false skips engine cert validation (dev only)
}

// ConfigFromEnv builds an AgentConfig from TELEQUICK_HOST / TELEQUICK_MEDIA_KEY /
// TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT (and optional TELEQUICK_PORT).
// Any field set on `override` wins.
func ConfigFromEnv(override AgentConfig) (AgentConfig, error) {
	c := AgentConfig{
		Host:       os.Getenv("TELEQUICK_HOST"),
		AppKey:     os.Getenv("TELEQUICK_MEDIA_KEY"),
		AppSecret:  os.Getenv("TELEQUICK_MEDIA_SECRET"),
		AgentName:  os.Getenv("TELEQUICK_AGENT"),
		Port:       443,
		SampleRate: 8000,
		Verify:     true,
	}
	if p, err := strconv.Atoi(os.Getenv("TELEQUICK_PORT")); err == nil && p > 0 {
		c.Port = p
	}
	if override.Host != "" {
		c.Host = override.Host
	}
	if override.AppKey != "" {
		c.AppKey = override.AppKey
	}
	if override.AppSecret != "" {
		c.AppSecret = override.AppSecret
	}
	if override.AgentName != "" {
		c.AgentName = override.AgentName
	}
	if override.Port != 0 {
		c.Port = override.Port
	}
	if override.SampleRate != 0 {
		c.SampleRate = override.SampleRate
	}
	var missing []string
	for _, kv := range [][2]string{{"host", c.Host}, {"app key", c.AppKey}, {"app secret", c.AppSecret}, {"agent name", c.AgentName}} {
		if kv[1] == "" {
			missing = append(missing, kv[0])
		}
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("AgentConfig missing %v — set TELEQUICK_HOST / TELEQUICK_MEDIA_KEY / TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT or pass them explicitly", missing)
	}
	return c, nil
}

func (c AgentConfig) wt() WebTransportConfig {
	return WebTransportConfig{
		EngineURL:   fmt.Sprintf("https://%s:%d", c.Host, c.Port),
		AppKey:      c.AppKey,
		AppSecret:   c.AppSecret,
		AgentName:   c.AgentName,
		SampleRate:  c.SampleRate,
		NumChannels: 1,
	}
}

// Call is one live phone call handed to your handler. Audio in both
// directions is 16-bit PCM at SampleRate (8 kHz for telephony). Inbound
// arrives as 20 ms frames; outbound accepts any chunk size and is paced onto
// the wire as steady 20 ms frames.
type Call struct {
	assign JobAssign
	sess   *WebTransportSession
}

func (c *Call) CallID() uint16                   { return c.assign.CallID }
func (c *Call) Room() string                     { return c.assign.RoomName }
func (c *Call) CallerNumber() string             { return c.assign.CallerNumber } // ANI
func (c *Call) CalledNumber() string             { return c.assign.CalledNumber } // DNIS
func (c *Call) TrunkID() string                  { return c.assign.TrunkID }
func (c *Call) Attributes() map[string]any       { return c.assign.Attributes }
func (c *Call) Metadata() map[string]string      { return c.assign.Metadata }
func (c *Call) SampleRate() int                  { return c.sess.SampleRate() }
func (c *Call) Ended() bool                      { return c.sess.Closed() }

// Tags are the agent's effective resource tags ({key: value}) — the
// platform's governed cost-allocation / ownership / environment labels,
// inherited down the parent chain. Route, bill, or branch per tenant without
// a second lookup.
func (c *Call) Tags() map[string]string { return c.assign.Tags }

// RecvAudio returns the next caller frame (pcm16); ok=false once the call ends.
func (c *Call) RecvAudio() ([]byte, bool) { return c.sess.RecvFrame() }

// Audio iterates caller audio until hangup: `for pcm := range call.Audio() { … }`.
func (c *Call) Audio() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for {
			pcm, ok := c.sess.RecvFrame()
			if !ok || !yield(pcm) {
				return
			}
		}
	}
}

// SendAudio queues agent audio for the caller (pcm16 @ SampleRate).
func (c *Call) SendAudio(pcm []byte) { c.sess.SendFrame(pcm) }

// Flush marks the current outbound segment complete (enables playout reports).
func (c *Call) Flush() { c.sess.SignalFlush() }

// Clear is barge-in: drop buffered agent audio so playout stops immediately.
func (c *Call) Clear() { c.sess.SignalClear() }

// NextPlayout blocks for the next playout report.
func (c *Call) NextPlayout() PlayoutReport { return c.sess.NextPlayout() }

// SendTranscript forwards a turn ("user"/"assistant") into platform transcript
// storage, analytics, and voice.transcript.ready webhooks.
func (c *Call) SendTranscript(role, text string, isFinal bool) {
	c.sess.SendTranscript(role, text, isFinal)
}

// Close ends the call from the agent side.
func (c *Call) Close() { c.sess.Close() }

// Handler serves one call; return when the call is done.
type Handler func(ctx context.Context, call *Call) error

// ServeOptions carries the connection-lifecycle hooks.
type ServeOptions struct {
	// OnReady fires once the worker is connected, authenticated and registered
	// — present to the platform and awaiting calls — and again after every
	// automatic reconnect. Drive a readiness probe / health check off it.
	OnReady func()
	// OnDisconnected fires when the connection drops (engine restart,
	// network); Serve then reconnects with backoff and OnReady fires again.
	OnDisconnected func(error)
	// Logger defaults to the standard logger.
	Logger *log.Logger
}

// Serve connects, authenticates, registers, and runs handler once per assigned
// call. It reconnects with exponential backoff on any drop so presence recovers
// without operator action, and returns only when ctx is cancelled.
func Serve(ctx context.Context, handler Handler, cfg AgentConfig, opts ServeOptions) error {
	lg := opts.Logger
	if lg == nil {
		lg = log.Default()
	}
	backoff := time.Second
	for {
		err := serveOnce(ctx, handler, cfg, opts, lg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			err = errClosed
		}
		if opts.OnDisconnected != nil {
			opts.OnDisconnected(err)
		}
		lg.Printf("[telequick] worker connection lost (%v) — reconnecting in %s", err, backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func serveOnce(ctx context.Context, handler Handler, cfg AgentConfig, opts ServeOptions, lg *log.Logger) error {
	conn, err := OpenMediaConnection(ctx, cfg.wt(), cfg.Host, cfg.Port, cfg.Verify)
	if err != nil {
		return err
	}
	defer conn.Close()
	lg.Printf("[telequick] worker ready (agent=%s) — awaiting calls", cfg.AgentName)
	if opts.OnReady != nil {
		opts.OnReady()
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-conn.Closed:
			cancel()
		case <-cctx.Done():
		}
	}()

	for {
		assign, sess, err := conn.MC.NextCall(cctx)
		if err != nil {
			select {
			case <-conn.Closed:
				return errClosed
			default:
				return err
			}
		}
		lg.Printf("[telequick] call_id=%d from=%s to=%s → handler", assign.CallID, assign.CallerNumber, assign.CalledNumber)
		go func(assign JobAssign, sess *WebTransportSession) {
			call := &Call{assign: assign, sess: sess}
			defer func() {
				if r := recover(); r != nil {
					lg.Printf("[telequick] handler panicked for call_id=%d: %v", assign.CallID, r)
				}
				if !sess.Closed() {
					call.Close()
				}
			}()
			if err := handler(cctx, call); err != nil && !errors.Is(err, context.Canceled) {
				lg.Printf("[telequick] handler failed for call_id=%d: %v", assign.CallID, err)
			}
		}(assign, sess)
	}
}
