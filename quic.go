package agents

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
)

// The QUIC socket under MediaConnection, via quic-go's webtransport-go:
//   - extended CONNECT (:protocol webtransport) to https://<host>:<port>/media/<app_key>
//   - control on ONE client-opened bidi WT stream (the engine replies on it)
//   - media on QUIC datagrams (EnableDatagrams is mandatory or every frame is dropped)
//   - app-level ping every 20 s + an RX watchdog: no packet for > 45 s ⇒ peer gone
//     (engine restart) ⇒ Closed fires and Serve reconnects.

const (
	keepaliveEvery = 20 * time.Second
	rxDeadline     = 45 * time.Second
)

// Opened is a live, authenticated media connection.
type Opened struct {
	MC     *MediaConnection
	Closed <-chan struct{} // closed when the connection is dead
	Close  func()
}

// OpenMediaConnection dials, authenticates and returns the connection.
// verify=false skips engine certificate validation — dev only.
func OpenMediaConnection(ctx context.Context, cfg WebTransportConfig, host string, port int, verify bool) (*Opened, error) {
	if port == 0 {
		port = 443
	}
	url := fmt.Sprintf("https://%s:%d/media/%s", host, port, cfg.AppKey)
	d := webtransport.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !verify, NextProtos: []string{"h3"}}, //nolint:gosec — verify=false is explicit dev-only opt-in
		QUICConfig:      &quic.Config{EnableDatagrams: true, MaxIdleTimeout: 60 * time.Second},
	}
	resp, sess, err := d.Dial(ctx, url, http.Header{})
	if err != nil {
		return nil, fmt.Errorf("webtransport dial %s: %w", url, err)
	}
	if resp.StatusCode/100 != 2 {
		sess.CloseWithError(0, "bad status")
		return nil, fmt.Errorf("webtransport dial %s: HTTP %d", url, resp.StatusCode)
	}

	ctrl, err := sess.OpenStream()
	if err != nil {
		sess.CloseWithError(0, "no control stream")
		return nil, fmt.Errorf("open control stream: %w", err)
	}

	var lastRx atomic.Int64
	touch := func() { lastRx.Store(time.Now().UnixNano()) }
	touch()

	closed := make(chan struct{})
	var closeOnce sync.Once
	markClosed := func() { closeOnce.Do(func() { close(closed) }) }

	var wmu sync.Mutex
	sendStream := func(b []byte) {
		wmu.Lock()
		defer wmu.Unlock()
		if _, err := ctrl.Write(b); err != nil {
			markClosed()
		}
	}
	sendDatagram := func(b []byte) {
		if err := sess.SendDatagram(b); err != nil {
			// a single lost datagram is a lost 20 ms; only a dead session matters
			select {
			case <-sess.Context().Done():
				markClosed()
			default:
			}
		}
	}

	mc := newMediaConnection(cfg, sendDatagram, sendStream)

	// Reader loops.
	go func() {
		var buf []byte
		chunk := make([]byte, 16*1024)
		for {
			n, err := ctrl.Read(chunk)
			if n > 0 {
				touch()
				buf = append(buf, chunk[:n]...)
				var msgs []CtrlMsg
				msgs, buf = DecodeCtrlStream(buf)
				for _, m := range msgs {
					mc.onControl(m)
				}
			}
			if err != nil {
				if err != io.EOF {
					_ = err
				}
				markClosed()
				return
			}
		}
	}()
	go func() {
		for {
			b, err := sess.ReceiveDatagram(context.Background())
			if err != nil {
				markClosed()
				return
			}
			touch()
			mc.onDatagram(b)
		}
	}()

	if err := mc.Authenticate(10 * time.Second); err != nil {
		sess.CloseWithError(0, "auth failed")
		return nil, err
	}

	// Keepalive + RX watchdog.
	go func() {
		ka := time.NewTicker(keepaliveEvery)
		wd := time.NewTicker(10 * time.Second)
		defer ka.Stop()
		defer wd.Stop()
		for {
			select {
			case <-ka.C:
				if err := mc.Ping(); err != nil {
					markClosed()
					return
				}
			case <-wd.C:
				if time.Since(time.Unix(0, lastRx.Load())) > rxDeadline {
					markClosed()
					return
				}
			case <-closed:
				return
			case <-sess.Context().Done():
				markClosed()
				return
			}
		}
	}()

	closeFn := func() {
		markClosed()
		sess.CloseWithError(0, "")
	}
	go func() { <-closed; sess.CloseWithError(0, "") }()

	return &Opened{MC: mc, Closed: closed, Close: closeFn}, nil
}
