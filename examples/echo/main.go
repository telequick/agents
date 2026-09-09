// Minimal external agent: echo the caller's audio straight back.
//
// Proves the whole media path — caller → engine → QUIC → your process → back
// onto the call — with no STT/LLM/TTS. Swap the handler body for your own
// pipeline (see the sketch below).
//
//	export TELEQUICK_HOST=engine.telequick.dev
//	export TELEQUICK_MEDIA_KEY=mk_...
//	export TELEQUICK_MEDIA_SECRET=...
//	export TELEQUICK_AGENT=salesbot
//	go run ./examples/echo
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	agents "github.com/telequick/agents"
)

func handler(ctx context.Context, call *agents.Call) error {
	log.Printf("call %d from %s → %s (trunk=%s tags=%v)", call.CallID(), call.CallerNumber(), call.CalledNumber(), call.TrunkID(), call.Tags())
	frames := 0
	for pcm := range call.Audio() { // 20 ms pcm16 @ 8 kHz
		call.SendAudio(pcm) // echo caller audio back
		if frames++; frames%250 == 0 {
			log.Printf("  %d: %d frames", call.CallID(), frames)
		}
	}
	log.Printf("call %d ended after %d frames", call.CallID(), frames)
	return nil
}

// ── Your real agent goes here instead ──────────────────────────────────────
// func voiceBot(ctx context.Context, call *agents.Call) error {
//     for pcm := range call.Audio() {
//         text := yourSTT(pcm, call.SampleRate())      // pcm16 @ call.SampleRate()
//         if text == "" { continue }
//         call.SendTranscript("user", text, true)      // lands in TeleQuick history
//         reply := yourLLM(text)
//         call.SendTranscript("assistant", reply, true)
//         for chunk := range yourTTS(reply, call.SampleRate()) {
//             call.SendAudio(chunk)                    // pcm16 @ call.SampleRate()
//         }
//         call.Flush()
//     }
//     return nil
// }

func main() {
	cfg, err := agents.ConfigFromEnv(agents.AgentConfig{})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	err = agents.Serve(ctx, handler, cfg, agents.ServeOptions{
		OnReady:        func() { log.Println("[echo] connected + registered — awaiting calls") },
		OnDisconnected: func(e error) { log.Printf("[echo] dropped, reconnecting: %v", e) },
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
