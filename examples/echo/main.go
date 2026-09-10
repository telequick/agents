// Minimal external agent: echo the caller's audio straight back.
//
// Proves the whole media path — caller → engine → QUIC → your process → back
// onto the call — with no STT/LLM/TTS. Swap the handler body for your own
// pipeline (see the sketch below).
//
//	export TELEQUICK_HOST=engine.telequick.dev
//	export TELEQUICK_MEDIA_KEY=ck_...
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
// You run STT/LLM/TTS on your OWN keys, so the platform only sees what you
// forward. SendTranscript/SendUsage/SendToolCall give an external agent the
// same history, token analytics and tool timeline a native agent gets.
//
// func voiceBot(ctx context.Context, call *agents.Call) error {
//     for pcm := range call.Audio() {
//         text, secs := yourSTT(pcm, call.SampleRate()) // pcm16 @ call.SampleRate()
//         if text == "" { continue }
//         call.SendTranscript("user", text, true)       // lands in TeleQuick history
//         call.SendUsage("stt", map[string]any{"audio_seconds": secs}, "deepgram", "nova-2")
//
//         reply, in, out, tools := yourLLM(text)
//         call.SendUsage("llm",
//             map[string]any{"prompt_tokens": in, "completion_tokens": out},
//             "openai", "gpt-4o-mini")
//         for _, t := range tools {
//             // REDACT sensitive values — args/result are forwarded verbatim.
//             call.SendToolCall(t.Name, t.Args, t.Result)
//         }
//
//         call.SendTranscript("assistant", reply, true)
//         for chunk := range yourTTS(reply, call.SampleRate()) {
//             call.SendAudio(chunk)                     // pcm16 @ call.SampleRate()
//         }
//         call.SendUsage("tts", map[string]any{"characters": len(reply)}, "elevenlabs", "")
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
