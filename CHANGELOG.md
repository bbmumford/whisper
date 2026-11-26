# Changelog

## [0.0.20](https://github.com/bbmumford/whisper/compare/v0.0.19...v0.0.20) (2026-11-26)


### Bug Fixes

* a snapshot record batch's count is bounded by its bytes and by one frame before anything is allocated ([a42a01e](https://github.com/bbmumford/whisper/commit/a42a01e8beb7273e7f6a553bef4d08de11860f29))
* go.mod requires aether v0.0.118, the release holding the aether code this module builds against ([f5ab318](https://github.com/bbmumford/whisper/commit/f5ab318f8bf56825a9ac133bf869d1fd87a8f1bf))
* one rumor payload bound on every path, and an oversize frame ends the stream ([cd4c5e2](https://github.com/bbmumford/whisper/commit/cd4c5e2fc3dccbeab822065430a44158edfdd8f3))
* the snapshot initiator reads the magic every responder frame begins with ([66804bb](https://github.com/bbmumford/whisper/commit/66804bb028cd46c11c05d399a4a907bec815d078))

## [0.0.19](https://github.com/bbmumford/whisper/compare/v0.0.18...v0.0.19) (2026-11-15)


### Bug Fixes

* build against the published aether module ([e4a46e6](https://github.com/bbmumford/whisper/commit/e4a46e67a3a95aaf07bd32cdffe2367af631242e))
* check every returned error and drop unused code, so the release lint passes ([28dbe3d](https://github.com/bbmumford/whisper/commit/28dbe3d3d4d749c3042dddbe7ad2451515489a34))
