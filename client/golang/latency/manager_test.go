// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {

	// Start a few servers
	server1 := serve(2001)
	defer server1.Stop()
	server2 := serve(2002)
	defer server2.Stop()
	server3 := serve(2003)
	defer server3.Stop()

	m := newManager([]int{0, 10, 20, 30, 40, 50, 10000}, 3)
	defer m.Close()
	m.watch = newWatcher(m.onPing, 1*time.Millisecond)
	m.Watch("127.0.0.1:2001")
	m.Watch("127.0.0.1:2002")
	m.Watch("127.0.0.1:2003")

	time.Sleep(500 * time.Millisecond)
	m.Unwatch("127.0.0.1:2001")
	m.Unwatch("127.0.0.1:2002")
	m.Unwatch("127.0.0.1:2003")
	assert.NoError(t, m.watch.Close())

	/* example
	[271 1 0 0 0 0]
	[270 1 1 0 0 0]
	[273 0 0 0 0 0]
	*/
	m.update(3)

	assert.NotZero(t, m.WeightOf("127.0.0.1:2001"))
	assert.NotZero(t, m.WeightOf("127.0.0.1:2002"))
	assert.NotZero(t, m.WeightOf("127.0.0.1:2003"))

}
