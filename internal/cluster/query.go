package cluster

import "github.com/jackc/pgx/v5"

func scanIssues(rows pgx.Rows) ([]Issue, error) {
	defer rows.Close()
	out := []Issue{}
	for rows.Next() {
		var i Issue
		if err := rows.Scan(&i.ClusterKey, &i.Source, &i.Signature, &i.Severity, &i.Count,
			&i.FirstSeen, &i.LastSeen, &i.SampleEventIDs, &i.IssueURL, &i.State); err != nil {
			return nil, err
		}
		if i.SampleEventIDs == nil {
			i.SampleEventIDs = []int64{}
		}
		out = append(out, i)
	}
	return out, rows.Err()
}
