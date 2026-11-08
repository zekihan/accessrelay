//go:build !linux

package renderer

import "os"

// Without procfs the child's read offset is unobservable, so replays are
// never streamed and GoAccess starts after the replay is complete.
const feedSupported = false

func startThrottle(*os.Process, <-chan struct{}, string, *Feed) *throttle { return nil }
