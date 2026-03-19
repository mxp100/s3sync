package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config.yaml (array of sync jobs)")
	flag.Parse()

	jobs, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ui := NewProgressUI(len(jobs))
	ui.Start()
	defer ui.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		cancel()
	}()

	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		go func(idx int, job SyncJob) {
			defer wg.Done()
			s := NewSyncer(idx, job, ui)
			if err := s.Run(ctx); err != nil {
				// Ошибка уже отрендерена в UI, но продублируем в лог.
				log.Printf("[%s] sync failed: %v", job.Name, err)
			}
		}(i, j)
	}

	wg.Wait()
}
