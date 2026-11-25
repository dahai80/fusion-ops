package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// requiredTables lists every table the app role must fully own at startup.
var requiredTables = []string{"ops_events", "ops_prompts", "mined_issues", "feedback", "drop_counters", "device_registry", "task_mailbox"}

// ProbePrivileges fails closed: returns error unless the connected role holds
// SELECT/INSERT/UPDATE/DELETE on every required table in the current
// database. Consuming services call this at boot and refuse to start when it
// fails (issue #1 req 3).
func ProbePrivileges(ctx context.Context, conn *pgxpool.Conn) error {
	for _, table := range requiredTables {
		row := conn.QueryRow(ctx, `
			SELECT
				BOOL_OR(privilege_type = 'SELECT'),
				BOOL_OR(privilege_type = 'INSERT'),
				BOOL_OR(privilege_type = 'UPDATE'),
				BOOL_OR(privilege_type = 'DELETE')
			FROM information_schema.table_privileges
			WHERE grantee = current_user AND table_name = $1`, table)
		var sel, ins, upd, del bool
		if err := row.Scan(&sel, &ins, &upd, &del); err != nil {
			if err == pgx.ErrNoRows {
				return fmt.Errorf("privilege probe: no privileges recorded for table %s", table)
			}
			return fmt.Errorf("privilege probe %s: %w", table, err)
		}
		if !sel || !ins || !upd || !del {
			return fmt.Errorf("privilege probe: role %s missing privileges on table %s (select=%v insert=%v update=%v delete=%v)",
				conn.Conn().Config().User, table, sel, ins, upd, del)
		}
	}
	return nil
}

// ProbeCanary goes beyond catalog checks: runs a real INSERT+DELETE roundtrip
// on ops_events inside a rolled-back transaction, so table-level GRANTs that
// exist on paper but not in practice fail closed too.
func ProbeCanary(ctx context.Context, conn *pgxpool.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("privilege canary begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO ops_events (source, event_type, payload) VALUES ('gateway', 'probe_canary', '{}'::jsonb) RETURNING id`,
	).Scan(&id); err != nil {
		return fmt.Errorf("privilege canary insert: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM ops_events WHERE id = $1`, id); err != nil {
		return fmt.Errorf("privilege canary delete: %w", err)
	}
	return nil
}
