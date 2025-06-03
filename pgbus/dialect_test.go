package pgbus

import (
	"database/sql"
	"testing"

	"github.com/qor5/go-bus"
	"github.com/stretchr/testify/assert"
)

func TestParsePatternTokens(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected [bus.MaxPatternTokens]*string
	}{
		{
			name:    "simple pattern",
			pattern: "events.user.created",
			expected: func() [bus.MaxPatternTokens]*string {
				var result [bus.MaxPatternTokens]*string
				parts := []string{"events", "user", "created"}
				for i, part := range parts {
					result[i] = &part
				}
				return result
			}(),
		},
		{
			name:    "pattern with wildcard",
			pattern: "events.*.created",
			expected: func() [bus.MaxPatternTokens]*string {
				var result [bus.MaxPatternTokens]*string
				parts := []string{"events", "*", "created"}
				for i, part := range parts {
					result[i] = &part
				}
				return result
			}(),
		},
		{
			name:    "pattern with multi-level wildcard",
			pattern: "events.>",
			expected: func() [bus.MaxPatternTokens]*string {
				var result [bus.MaxPatternTokens]*string
				events := "events"
				wildcard := ">"
				result[0] = &events
				// ">" fills all remaining positions
				for i := 1; i < bus.MaxPatternTokens; i++ {
					result[i] = &wildcard
				}
				return result
			}(),
		},
		{
			name:    "single token",
			pattern: "events",
			expected: func() [bus.MaxPatternTokens]*string {
				var result [bus.MaxPatternTokens]*string
				part := "events"
				result[0] = &part
				return result
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePatternTokens(tt.pattern)

			// Compare each element manually since array comparison with pointers is tricky
			for i := 0; i < bus.MaxPatternTokens; i++ {
				if tt.expected[i] == nil && result[i] == nil {
					continue
				}
				if tt.expected[i] == nil || result[i] == nil {
					t.Errorf("Position %d: expected %v, got %v", i, tt.expected[i], result[i])
					continue
				}
				if *tt.expected[i] != *result[i] {
					t.Errorf("Position %d: expected %q, got %q", i, *tt.expected[i], *result[i])
				}
			}
		})
	}
}

func TestBuildIndexedWhereClause(t *testing.T) {
	tests := []struct {
		name          string
		subject       string
		expectedWhere string
		expectedArgs  []any
	}{
		{
			name:    "simple subject with 3 tokens",
			subject: "events.user.created",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 = $2 OR token_1 = '*' OR token_1 = '>') AND " +
				"(token_2 = $3 OR token_2 = '*' OR token_2 = '>') AND " +
				"(token_3 IS NULL OR token_2 = '>')",
			expectedArgs: []any{"events", "user", "created"},
		},
		{
			name:    "single token subject",
			subject: "events",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 IS NULL OR token_0 = '>')",
			expectedArgs: []any{"events"},
		},
		{
			name:    "two token subject",
			subject: "events.user",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 = $2 OR token_1 = '*' OR token_1 = '>') AND " +
				"(token_2 IS NULL OR token_1 = '>')",
			expectedArgs: []any{"events", "user"},
		},
		{
			name:    "five token subject",
			subject: "events.user.profile.updated.notification",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 = $2 OR token_1 = '*' OR token_1 = '>') AND " +
				"(token_2 = $3 OR token_2 = '*' OR token_2 = '>') AND " +
				"(token_3 = $4 OR token_3 = '*' OR token_3 = '>') AND " +
				"(token_4 = $5 OR token_4 = '*' OR token_4 = '>') AND " +
				"(token_5 IS NULL OR token_4 = '>')",
			expectedArgs: []any{"events", "user", "profile", "updated", "notification"},
		},
		{
			name:    "maximum tokens subject",
			subject: "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 = $2 OR token_1 = '*' OR token_1 = '>') AND " +
				"(token_2 = $3 OR token_2 = '*' OR token_2 = '>') AND " +
				"(token_3 = $4 OR token_3 = '*' OR token_3 = '>') AND " +
				"(token_4 = $5 OR token_4 = '*' OR token_4 = '>') AND " +
				"(token_5 = $6 OR token_5 = '*' OR token_5 = '>') AND " +
				"(token_6 = $7 OR token_6 = '*' OR token_6 = '>') AND " +
				"(token_7 = $8 OR token_7 = '*' OR token_7 = '>') AND " +
				"(token_8 = $9 OR token_8 = '*' OR token_8 = '>') AND " +
				"(token_9 = $10 OR token_9 = '*' OR token_9 = '>') AND " +
				"(token_10 = $11 OR token_10 = '*' OR token_10 = '>') AND " +
				"(token_11 = $12 OR token_11 = '*' OR token_11 = '>') AND " +
				"(token_12 = $13 OR token_12 = '*' OR token_12 = '>') AND " +
				"(token_13 = $14 OR token_13 = '*' OR token_13 = '>') AND " +
				"(token_14 = $15 OR token_14 = '*' OR token_14 = '>') AND " +
				"(token_15 = $16 OR token_15 = '*' OR token_15 = '>')",
			expectedArgs: []any{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
		},
		{
			name:    "subject exceeding maximum tokens",
			subject: "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s",
			expectedWhere: "(token_0 = $1 OR token_0 = '*' OR token_0 = '>') AND " +
				"(token_1 = $2 OR token_1 = '*' OR token_1 = '>') AND " +
				"(token_2 = $3 OR token_2 = '*' OR token_2 = '>') AND " +
				"(token_3 = $4 OR token_3 = '*' OR token_3 = '>') AND " +
				"(token_4 = $5 OR token_4 = '*' OR token_4 = '>') AND " +
				"(token_5 = $6 OR token_5 = '*' OR token_5 = '>') AND " +
				"(token_6 = $7 OR token_6 = '*' OR token_6 = '>') AND " +
				"(token_7 = $8 OR token_7 = '*' OR token_7 = '>') AND " +
				"(token_8 = $9 OR token_8 = '*' OR token_8 = '>') AND " +
				"(token_9 = $10 OR token_9 = '*' OR token_9 = '>') AND " +
				"(token_10 = $11 OR token_10 = '*' OR token_10 = '>') AND " +
				"(token_11 = $12 OR token_11 = '*' OR token_11 = '>') AND " +
				"(token_12 = $13 OR token_12 = '*' OR token_12 = '>') AND " +
				"(token_13 = $14 OR token_13 = '*' OR token_13 = '>') AND " +
				"(token_14 = $15 OR token_14 = '*' OR token_14 = '>') AND " +
				"(token_15 = $16 OR token_15 = '*' OR token_15 = '>')",
			expectedArgs: []any{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			where, args := buildIndexedWhereClause(tt.subject)
			assert.Equal(t, tt.expectedWhere, where)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

func TestQueryStrategy(t *testing.T) {
	// Create a mock database connection
	db := &sql.DB{} // This is just for structure validation

	// Test NewDialect default strategy
	dialect, err := NewDialect(db)
	assert.NoError(t, err)
	assert.Equal(t, QueryStrategyIndexed, dialect.queryStrategy)

	// Test WithQueryStrategy
	dialect = dialect.WithQueryStrategy(QueryStrategyRegexp)
	assert.Equal(t, QueryStrategyRegexp, dialect.queryStrategy)

	// Test chaining
	dialect = dialect.WithQueryStrategy(QueryStrategyIndexed)
	assert.Equal(t, QueryStrategyIndexed, dialect.queryStrategy)
}
