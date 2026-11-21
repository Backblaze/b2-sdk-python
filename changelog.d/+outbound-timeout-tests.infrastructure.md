Add unit tests proving that every outbound HTTP entry point of `B2Http` stops waiting on an unresponsive local server within its timeout and raises `B2RequestTimeout`.
