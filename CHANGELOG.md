# Changelog

## [0.0.2](https://github.com/bbmumford/whisper/compare/v0.0.1...v0.0.2) (2026-06-03)


### Features

* 8-phase mesh convergence redesign — G3-ACK / G4 IBLT / G5 snapshot / adaptive policy / hypercube extensions (v0.0.10) ([170077e](https://github.com/bbmumford/whisper/commit/170077e7593891b9c5cf2b474cea1a9e14147759))
* CPU gate + pull-based gossip + adaptive tests (v0.0.4) ([bc9b6de](https://github.com/bbmumford/whisper/commit/bc9b6def6e0cdb63ddc5e94d7e559bb64982741b))
* Engine responder loop with pluggable FrameHandler dispatch ([2eecdfc](https://github.com/bbmumford/whisper/commit/2eecdfcceac99d384a2870c4cc1f44a62be95ba3))
* **memory:** bounded rumor queue, collapsed delta state, LRU seen cache (v0.0.5) ([06bab41](https://github.com/bbmumford/whisper/commit/06bab41f2ccb64605fff8ae1ec460ccd2b19de7e))
* native G1 handler + G1Codec + immutable hypercube selfID ([28863af](https://github.com/bbmumford/whisper/commit/28863af7b2a058b7eef51642a461acbdab5142f1))
* observability + adaptive cadence across gossip subsystems (v0.0.3) ([b923f7f](https://github.com/bbmumford/whisper/commit/b923f7f1f9526d93dd8837eea80fdbe636fac240))


### Bug Fixes

* backpressure defer accumulation in gossip Run loop ([dbe42c6](https://github.com/bbmumford/whisper/commit/dbe42c60fd5d1809a0be054db9c6fb60f88decab))
* **g1:** remove mid-frame SetDeadline call that flushed read buffer ([d3aa2f7](https://github.com/bbmumford/whisper/commit/d3aa2f7bad9122b4c9d3130b9b50bc1e337a43d5))
* guard built-in magics in RegisterFrameKind; SetSelfID on Hypercube ([0416443](https://github.com/bbmumford/whisper/commit/041644309efb3da6e0d9a046619f58e3feb35284))
* **reconcile:** bound responder follow-up read with 10s deadline (v0.0.11) ([cbeea79](https://github.com/bbmumford/whisper/commit/cbeea797703c382f6690f414c55036ca3ebee0f9))
* **reconcile:** event-driven followup — never block the engine frame loop (v0.0.13) ([9b6f45f](https://github.com/bbmumford/whisper/commit/9b6f45f3bce56049bd539e3781e21c227e4de701))
* **reconcile:** remove SetReadDeadline calls that flush aether stream buffer (v0.0.12) ([76e201d](https://github.com/bbmumford/whisper/commit/76e201d64e68c3c960d09977f7c63df4ea499dee))
