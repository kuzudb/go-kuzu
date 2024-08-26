package kuzu

/*
#cgo darwin LDFLAGS: -lc++ -L${SRCDIR}/lib/dynamic/darwin -lkuzu -Wl,-rpath,${SRCDIR}/lib/dynamic/darwin
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/dynamic/linux-amd64 -lkuzu -Wl,-rpath,${SRCDIR}/lib/dynamic/linux-amd64
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/dynamic/linux-arm64 -lkuzu -Wl,-rpath,${SRCDIR}/lib/dynamic/linux-arm64
#include "kuzu.h"
*/
import "C"
