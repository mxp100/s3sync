package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type streamProgress struct {
	Name        string
	Total       int
	Done        int
	Status      string
	LastError   string
	Completed   bool
	startedPlan bool
}

type ProgressUI struct {
	mu      sync.Mutex
	streams []*streamProgress
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewProgressUI создает рендерер прогресса для N параллельных потоков.
func NewProgressUI(num int) *ProgressUI {
	p := &ProgressUI{
		streams: make([]*streamProgress, num),
		stopCh:  make(chan struct{}),
	}
	for i := 0; i < num; i++ {
		p.streams[i] = &streamProgress{
			Name:   fmt.Sprintf("job-%d", i+1),
			Status: "planning...",
		}
	}
	return p
}

// SetName задает имя потока.
func (p *ProgressUI) SetName(idx int, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streams[idx].Name = name
}

// SetPlanTotal сообщает общее количество операций в плане.
func (p *ProgressUI) SetPlanTotal(idx int, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streams[idx].Total = total
	p.streams[idx].startedPlan = true
	if total == 0 {
		p.streams[idx].Status = "up to date"
		p.streams[idx].Completed = true
	}
}

// IncDone увеличивает счетчик выполненных операций.
func (p *ProgressUI) IncDone(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.streams[idx].Done < p.streams[idx].Total {
		p.streams[idx].Done++
	}
	if p.streams[idx].Done == p.streams[idx].Total && p.streams[idx].startedPlan {
		p.streams[idx].Completed = true
		if p.streams[idx].Status == "" || strings.Contains(p.streams[idx].Status, "running") {
			p.streams[idx].Status = "completed"
		}
	}
}

// SetStatus обновляет строку статуса.
func (p *ProgressUI) SetStatus(idx int, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streams[idx].Status = status
}

// SetError фиксирует последнюю ошибку.
func (p *ProgressUI) SetError(idx int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.streams[idx].LastError = err.Error()
	}
}

// Start запускает цикл отрисовки.
func (p *ProgressUI) Start() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		clearScreen := func() {
			// ANSI очистка экрана и позиционирование курсора в (0,0)
			fmt.Fprint(os.Stdout, "\033[H\033[2J")
		}
		render := func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			var b strings.Builder
			b.WriteString("S3 Sync Progress\n")
			b.WriteString(strings.Repeat("=", 60) + "\n")
			for _, s := range p.streams {
				total := s.Total
				done := s.Done
				percent := 0
				if total > 0 {
					percent = int(float64(done) / float64(total) * 100.0)
				}
				if percent > 100 {
					percent = 100
				}
				barWidth := 30
				fill := 0
				if total > 0 {
					fill = int(float64(barWidth) * float64(percent) / 100.0)
				}
				if fill > barWidth {
					fill = barWidth
				}
				bar := "[" + strings.Repeat("#", fill) + strings.Repeat(".", barWidth-fill) + "]"
				line := fmt.Sprintf("%-16s %s %3d%%  (%d/%d)  %s",
					s.Name, bar, percent, done, total, s.Status)
				b.WriteString(line + "\n")
				if s.LastError != "" {
					b.WriteString("  last error: " + s.LastError + "\n")
				}
			}
			b.WriteString(strings.Repeat("=", 60) + "\n")
			fmt.Fprint(os.Stdout, b.String())
		}

		for {
			select {
			case <-p.stopCh:
				clearScreen()
				render()
				return
			case <-ticker.C:
				clearScreen()
				render()
			}
		}
	}()
}

// Stop останавливает рендер и дожидается завершения.
func (p *ProgressUI) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}
