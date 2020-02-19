// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package latency

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {

	// Start a few servers
	server1, port1 := serve()
	addr1 := fmt.Sprintf("127.0.0.1:%v", port1)
	defer server1.Stop()

	server2, port2 := serve()
	addr2 := fmt.Sprintf("127.0.0.1:%v", port2)
	defer server2.Stop()

	server3, port3 := serve()
	addr3 := fmt.Sprintf("127.0.0.1:%v", port3)
	defer server3.Stop()

	m := newManager([]int{0, 10, 20, 30, 40, 50, 10000}, 3)
	defer m.Close()
	m.watch = newWatcher(m.onPing, 1*time.Millisecond)
	m.Watch(addr1)
	m.Watch(addr2)
	m.Watch(addr3)

	time.Sleep(500 * time.Millisecond)
	m.Unwatch(addr1)
	m.Unwatch(addr2)
	m.Unwatch(addr3)
	assert.NoError(t, m.watch.Close())

	/* example
	[271 1 0 0 0 0]
	[270 1 1 0 0 0]
	[273 0 0 0 0 0]
	*/
	m.update(3)

	assert.NotZero(t, m.WeightOf(addr1))
	assert.NotZero(t, m.WeightOf(addr2))
	assert.NotZero(t, m.WeightOf(addr3))

}
