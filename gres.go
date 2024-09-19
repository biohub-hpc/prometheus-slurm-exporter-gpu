package main

import (
	// Add this line to import the fmt package

	"log"
	"os/exec"
	"sort"
	"strconv" // Add this line to import the strconv package
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// NodeMetrics stores metrics for each node
type GresMetrics struct {
	gresTotal int64
	gresUsed  int64

	nodeState string
}

func GresGetMetrics() map[string]*GresMetrics {

	return ParseGresMetrics(GresData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseGresMetrics(input []byte) map[string]*GresMetrics {

	nodes := make(map[string]*GresMetrics)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		node := strings.Fields(line)
		nodeName := node[0]
		stateLong := node[3] // mixed, allocated, etc.
		// stateLong := strings.ReplaceAll(node[3], "*", "")

		nodes[nodeName] = &GresMetrics{0, 0, ""}
		nodes[nodeName].nodeState = stateLong
		gresTotal := strings.Split(node[1], ":")
		gresUsed := strings.Split(node[2], ":")

		gresTotalInt, _ := strconv.ParseInt(gresTotal[1], 10, 64)
		gresUsedInt, _ := strconv.ParseInt(gresUsed[1], 10, 64)

		nodes[nodeName].gresTotal = gresTotalInt
		nodes[nodeName].gresUsed = gresUsedInt

	}

	return nodes
}

// NodeData executes the sinfo command to get data for each node
// It returns the output of the sinfo command
func GresData() []byte {
	cmd := exec.Command("sinfo", "-h", "-N", "-O", "NodeHost,Gres,GresUsed,StateLong")
	// cmd := exec.Command("cat", "test_data/sinfo_arc_gres.txt")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type GresCollector struct {
	gresTotal *prometheus.Desc
	gresUsed  *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewGresCollector() *GresCollector {
	log.Println("Function NewGresCollector called")
	labels := []string{"node", "state"}

	return &GresCollector{
		gresTotal: prometheus.NewDesc("slurm_node_gres_total", "Total GRES per node", labels, nil),
		gresUsed:  prometheus.NewDesc("slurm_node_gres_used", "Used GRES per node", labels, nil),
	}
}

// Send all metric descriptions
func (nc *GresCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.gresTotal
	ch <- nc.gresUsed
}

func (nc *GresCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := GresGetMetrics()
	for node := range nodes {
		ch <- prometheus.MustNewConstMetric(nc.gresTotal, prometheus.GaugeValue, float64(nodes[node].gresTotal), node, nodes[node].nodeState)
		ch <- prometheus.MustNewConstMetric(nc.gresUsed, prometheus.GaugeValue, float64(nodes[node].gresUsed), node, nodes[node].nodeState)
	}
}
