/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestParseQueueMetrics(t *testing.T) {
	// Read the input data from a file
	file, err := os.Open("test_data/squeue.txt")
	if err != nil {
		t.Fatalf("Can not open test data: %v", err)
	}
	data, err := ioutil.ReadAll(file)
	metrics := ParseQueueMetrics(data)

	// Verify expected counts (terminal states should be ignored)
	if metrics.pending != 4 {
		t.Errorf("Expected 4 pending jobs, got %f", metrics.pending)
	}
	if metrics.pending_dep != 0 {
		t.Errorf("Expected 0 pending_dep jobs, got %f", metrics.pending_dep)
	}
	if metrics.running != 28 {
		t.Errorf("Expected 28 running jobs, got %f", metrics.running)
	}
	if metrics.suspended != 1 {
		t.Errorf("Expected 1 suspended job, got %f", metrics.suspended)
	}
	if metrics.completing != 2 {
		t.Errorf("Expected 2 completing jobs, got %f", metrics.completing)
	}
	if metrics.configuring != 1 {
		t.Errorf("Expected 1 configuring job, got %f", metrics.configuring)
	}

	t.Logf("%+v", metrics)
}

func TestQueueGetMetrics(t *testing.T) {
	t.Logf("%+v", QueueGetMetrics())
}
