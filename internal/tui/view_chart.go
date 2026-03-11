package tui

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/psrth/mori/internal/logging"
)

// Braille dot offsets for a 2×4 grid per character.
// Left column: dots 1,2,3,7  Right column: dots 4,5,6,8
var brailleDots = [4][2]rune{
	{0x01, 0x08}, // row 0
	{0x02, 0x10}, // row 1
	{0x04, 0x20}, // row 2
	{0x40, 0x80}, // row 3
}

const brailleBase = 0x2800

// renderBrailleArea renders data series as filled area charts (bottom-up fill).
// For multi-series: pass largest-area series first; later series overwrite the
// lower portion, creating a layered look.
func renderBrailleArea(series [][]float64, colors []lipgloss.Color, width, height int) string {
	if width < 2 || height < 1 {
		return ""
	}

	pixW := width * 2
	pixH := height * 4

	resampled := make([][]float64, len(series))
	for i, s := range series {
		resampled[i] = resample(s, pixW)
	}

	yMin, yMax := math.Inf(1), math.Inf(-1)
	for _, s := range resampled {
		for _, v := range s {
			if v < yMin {
				yMin = v
			}
			if v > yMax {
				yMax = v
			}
		}
	}
	if math.IsInf(yMin, 1) {
		yMin, yMax = 0, 1
	}
	if yMax <= yMin {
		yMax = yMin + 1
	}

	grid := make([][]int, pixH)
	for r := range grid {
		grid[r] = make([]int, pixW)
		for c := range grid[r] {
			grid[r][c] = -1
		}
	}

	for si, s := range resampled {
		for x, v := range s {
			if x >= pixW {
				break
			}
			frac := (v - yMin) / (yMax - yMin)
			py := pixH - 1 - int(frac*float64(pixH-1)+0.5)
			if py < 0 {
				py = 0
			}
			if py >= pixH {
				py = pixH - 1
			}
			// Fill from data point down to bottom.
			for y := py; y < pixH; y++ {
				grid[y][x] = si
			}
		}
	}

	return renderGrid(grid, colors, width, height, pixW, pixH)
}

// renderBrailleLines renders data series as thick lines (3px vertical stroke).
// Later series draw on top of earlier ones.
func renderBrailleLines(series [][]float64, colors []lipgloss.Color, width, height int) string {
	if width < 2 || height < 1 {
		return ""
	}

	pixW := width * 2
	pixH := height * 4

	resampled := make([][]float64, len(series))
	for i, s := range series {
		resampled[i] = resample(s, pixW)
	}

	yMin, yMax := math.Inf(1), math.Inf(-1)
	for _, s := range resampled {
		for _, v := range s {
			if v < yMin {
				yMin = v
			}
			if v > yMax {
				yMax = v
			}
		}
	}
	if math.IsInf(yMin, 1) {
		yMin, yMax = 0, 1
	}
	if yMax <= yMin {
		yMax = yMin + 1
	}

	grid := make([][]int, pixH)
	for r := range grid {
		grid[r] = make([]int, pixW)
		for c := range grid[r] {
			grid[r][c] = -1
		}
	}

	for si, s := range resampled {
		for x, v := range s {
			if x >= pixW {
				break
			}
			frac := (v - yMin) / (yMax - yMin)
			py := pixH - 1 - int(frac*float64(pixH-1)+0.5)
			if py < 0 {
				py = 0
			}
			if py >= pixH {
				py = pixH - 1
			}
			// 3px thick line centered on data point.
			for dy := -1; dy <= 1; dy++ {
				y := py + dy
				if y >= 0 && y < pixH {
					grid[y][x] = si
				}
			}
		}
	}

	return renderGrid(grid, colors, width, height, pixW, pixH)
}

// renderGrid converts a pixel grid into braille characters with colors.
func renderGrid(grid [][]int, colors []lipgloss.Color, width, height, pixW, pixH int) string {
	var rows []string
	for charRow := 0; charRow < height; charRow++ {
		var sb strings.Builder
		for charCol := 0; charCol < width; charCol++ {
			var pattern rune
			dominant := -1
			for dotRow := 0; dotRow < 4; dotRow++ {
				for dotCol := 0; dotCol < 2; dotCol++ {
					pr := charRow*4 + dotRow
					pc := charCol*2 + dotCol
					if pr < pixH && pc < pixW && grid[pr][pc] >= 0 {
						pattern |= brailleDots[dotRow][dotCol]
						if grid[pr][pc] > dominant {
							dominant = grid[pr][pc]
						}
					}
				}
			}
			ch := string(brailleBase + pattern)
			if dominant >= 0 && dominant < len(colors) {
				sb.WriteString(lipgloss.NewStyle().Foreground(colors[dominant]).Render(ch))
			} else {
				sb.WriteString(ChartAxisStyle.Render(ch))
			}
		}
		rows = append(rows, sb.String())
	}

	return strings.Join(rows, "\n")
}

// computeTimeSeries buckets log entries into 1-second windows and computes
// p50, p95, p99 latency and queries-per-second for each bucket.
func computeTimeSeries(entries []logging.LogEntry) (p50s, p95s, p99s, qps []float64) {
	if len(entries) == 0 {
		return nil, nil, nil, nil
	}

	type sample struct {
		sec      int64
		duration float64
	}

	var samples []sample
	for _, e := range entries {
		if e.Event == "query" && e.DurationMs > 0 {
			sec := e.Timestamp.Unix()
			samples = append(samples, sample{sec, e.DurationMs})
		}
	}

	if len(samples) == 0 {
		return nil, nil, nil, nil
	}

	minSec, maxSec := samples[0].sec, samples[0].sec
	for _, s := range samples {
		if s.sec < minSec {
			minSec = s.sec
		}
		if s.sec > maxSec {
			maxSec = s.sec
		}
	}

	if maxSec == minSec {
		minSec -= 30
	}

	if maxSec-minSec > 120 {
		minSec = maxSec - 120
	}

	bucketCount := int(maxSec-minSec) + 1
	if bucketCount < 1 {
		bucketCount = 1
	}

	buckets := make([][]float64, bucketCount)
	for _, s := range samples {
		idx := int(s.sec - minSec)
		if idx < 0 || idx >= bucketCount {
			continue
		}
		buckets[idx] = append(buckets[idx], s.duration)
	}

	p50s = make([]float64, bucketCount)
	p95s = make([]float64, bucketCount)
	p99s = make([]float64, bucketCount)
	qps = make([]float64, bucketCount)

	for i, b := range buckets {
		qps[i] = float64(len(b))
		if len(b) == 0 {
			continue
		}
		sort.Float64s(b)
		p50s[i] = percentile(b, 0.50)
		p95s[i] = percentile(b, 0.95)
		p99s[i] = percentile(b, 0.99)
	}

	return p50s, p95s, p99s, qps
}

// resample resamples data to targetLen points using linear interpolation.
func resample(data []float64, targetLen int) []float64 {
	if targetLen <= 0 {
		return nil
	}
	if len(data) == 0 {
		return make([]float64, targetLen)
	}
	if len(data) == 1 {
		out := make([]float64, targetLen)
		for i := range out {
			out[i] = data[0]
		}
		return out
	}
	if len(data) == targetLen {
		dst := make([]float64, targetLen)
		copy(dst, data)
		return dst
	}

	out := make([]float64, targetLen)
	ratio := float64(len(data)-1) / float64(targetLen-1)
	for i := 0; i < targetLen; i++ {
		srcIdx := float64(i) * ratio
		lo := int(srcIdx)
		hi := lo + 1
		if hi >= len(data) {
			hi = len(data) - 1
		}
		frac := srcIdx - float64(lo)
		out[i] = data[lo]*(1-frac) + data[hi]*frac
	}
	return out
}

// percentile computes the p-th percentile from a sorted slice.
func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := pct * float64(len(sorted)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// RenderLatencyChart renders the latency braille graph with legend.
// Uses thick lines so all three percentile series are distinguishable.
func RenderLatencyChart(p50s, p95s, p99s []float64, width, height int) string {
	graphH := height - 1 // reserve 1 line for legend
	if graphH < 1 {
		graphH = 1
	}

	if len(p50s) == 0 && len(p95s) == 0 && len(p99s) == 0 {
		var rows []string
		for i := 0; i < graphH; i++ {
			rows = append(rows, "")
		}
		rows = append(rows, ChartAxisStyle.Render(" (no latency data)"))
		return strings.Join(rows, "\n")
	}

	// p50 drawn first (underlay), p95 over it, p99 on top.
	graph := renderBrailleLines(
		[][]float64{p50s, p95s, p99s},
		[]lipgloss.Color{ColorLatencyP50, ColorLatencyP95, ColorLatencyP99},
		width, graphH,
	)

	legend := " " + LegendP50.Render("◆") + ChartAxisStyle.Render(" p50") +
		"  " + LegendP95.Render("■") + ChartAxisStyle.Render(" p95") +
		"  " + LegendP99.Render("●") + ChartAxisStyle.Render(" p99")

	return graph + "\n" + legend
}

// RenderThroughputChart renders the throughput braille graph with legend.
// Uses area fill for a solid, readable single-series chart.
func RenderThroughputChart(qps []float64, width, height int) string {
	graphH := height - 1
	if graphH < 1 {
		graphH = 1
	}

	if len(qps) == 0 {
		var rows []string
		for i := 0; i < graphH; i++ {
			rows = append(rows, "")
		}
		rows = append(rows, ChartAxisStyle.Render(" (no throughput data)"))
		return strings.Join(rows, "\n")
	}

	graph := renderBrailleArea(
		[][]float64{qps},
		[]lipgloss.Color{ColorThroughput},
		width, graphH,
	)

	legend := " " + LegendThroughput.Render("●") + ChartAxisStyle.Render(" q/s")

	return graph + "\n" + legend
}

// lastNSeconds filters entries to the most recent N seconds.
func lastNSeconds(entries []logging.LogEntry, n int) []logging.LogEntry {
	cutoff := time.Now().Add(-time.Duration(n) * time.Second)
	start := 0
	for i, e := range entries {
		if !e.Timestamp.Before(cutoff) {
			start = i
			break
		}
	}
	return entries[start:]
}
