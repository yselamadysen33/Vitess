package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Simulate the VReplication copier and player behavior during a reparent event.

type DatabaseClient struct {
	ReadOnly bool
}

func (db *DatabaseClient) Begin() error {
	if db.ReadOnly {
		return errors.New("database is read-only")
	}
	return nil
}

func (db *DatabaseClient) Commit(ctx context.Context) error {
	// Check context before committing
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if db.ReadOnly {
		return errors.New("database is read-only during commit")
	}
	return nil
}

type Copier struct {
	dbClient *DatabaseClient
}

func (c *Copier) CopyTable(ctx context.Context) error {
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("Copying chunk %d...\n", i)
		time.Sleep(100 * time.Millisecond) // Simulate work

		if err := c.dbClient.Begin(); err != nil {
			return fmt.Errorf("begin failed: %w", err)
		}

		// Simulate transaction commit with context propagation
		if err := c.dbClient.Commit(ctx); err != nil {
			return fmt.Errorf("commit failed: %w", err)
		}
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	db := &DatabaseClient{ReadOnly: false}
	copier := &Copier{dbClient: db}

	// Simulate PlannedReparentShard (PRS) after 250ms
	go func() {
		time.Sleep(250 * time.Millisecond)
		fmt.Println("[PRS Event] Promoting new primary, setting old primary to read-only and canceling context...")
		db.ReadOnly = true
		cancel()
	}()

	err := copier.CopyTable(ctx)
	if err != nil {
		fmt.Printf("Copier exited with error: %v (Controller will restart and reconcile)\n", err)
	} else {
		fmt.Println("Copy completed successfully.")
	}
}
