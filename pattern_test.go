package bus

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToRegexPattern verifies the conversion of NATS-style wildcard patterns to regex patterns.
// It tests various patterns including exact matches, wildcards, and invalid patterns.
func TestToRegexPattern(t *testing.T) {
	tests := []struct {
		name             string
		pattern          string
		expected         string
		expectErrKeyword string   // Error message keyword, empty string means no error expected
		matches          []string // Subjects that should match
		nonMatches       []string // Subjects that should not match
	}{
		// Valid pattern test cases
		{
			name:             "exact_match_pattern",
			pattern:          "foo.bar.baz",
			expected:         `^foo\.bar\.baz$`,
			expectErrKeyword: "",
			matches:          []string{"foo.bar.baz"},
			nonMatches: []string{
				"foo.bar",
				"foo.bar.bazz",
				"foo.bar.ba",
				"afoo.bar.baz",
			},
		},
		{
			name:             "single_wildcard",
			pattern:          "foo.*.baz",
			expected:         `^foo\.[^.]+\.baz$`,
			expectErrKeyword: "",
			matches: []string{
				"foo.bar.baz",
				"foo.123.baz",
			},
			nonMatches: []string{
				"foo..baz", // Empty token not allowed
				"foo.bar",
				"foo.bar.baz.qux",
			},
		},
		{
			name:             "multiple_wildcards",
			pattern:          "foo.*.*.baz",
			expected:         `^foo\.[^.]+\.[^.]+\.baz$`,
			expectErrKeyword: "",
			matches: []string{
				"foo.hello.world.baz",
				"foo.a.b.baz",
				"foo.1.2.baz",
			},
			nonMatches: []string{
				"foo.hi..baz",          // Empty token not allowed
				"foo.hi.baz",           // Not enough tokens
				"foo.hi.bye.hello.baz", // Too many tokens
			},
		},
		{
			name:             "multi-level_wildcard_at_end",
			pattern:          "foo.bar.>",
			expected:         `^foo\.bar\..+$`,
			expectErrKeyword: "",
			matches: []string{
				"foo.bar.baz",
				"foo.bar.baz.qux",
				"foo.bar.1",
				"foo.bar.1.2.3",
			},
			nonMatches: []string{
				"foo.bar", // No trailing token
				"foo.baz.qux",
			},
		},
		{
			name:             "combined_wildcards",
			pattern:          "foo.*.baz.>",
			expected:         `^foo\.[^.]+\.baz\..+$`,
			expectErrKeyword: "",
			matches: []string{
				"foo.bar.baz.qux",
				"foo.123.baz.456",
				"foo.hi.baz.bye.xyz",
			},
			nonMatches: []string{
				"foo.bar.baz", // No trailing token for >
				"foo.baz.qux", // Missing wildcard token
			},
		},
		{
			name:             "just_wildcard",
			pattern:          "*",
			expected:         `^[^.]+$`,
			expectErrKeyword: "",
			matches: []string{
				"foo",
				"bar",
				"123",
			},
			nonMatches: []string{
				"",        // Empty string
				"foo.bar", // Contains dots
			},
		},
		{
			name:             "just_multi-level_wildcard",
			pattern:          ">",
			expected:         `^.+$`,
			expectErrKeyword: "",
			matches: []string{
				"foo",
				"foo.bar",
				"foo.bar.baz",
			},
			nonMatches: []string{
				"", // Empty string not matched
			},
		},
		{
			name:             "complex_pattern",
			pattern:          "a.*.b.*.c.>",
			expected:         `^a\.[^.]+\.b\.[^.]+\.c\..+$`,
			expectErrKeyword: "",
			matches: []string{
				"a.1.b.2.c.3",
				"a.foo.b.bar.c.baz",
				"a.foo.b.bar.c.baz.qux",
			},
			nonMatches: []string{
				"a.1.b.2.c",   // No trailing token for >
				"a.1.b.2.d.3", // Does not match 'c'
				"a.1.b.2.c",   // No trailing token for >
				"x.1.b.2.c.3", // Does not match 'a'
				"a..b.2.c.3",  // Empty token not allowed
			},
		},
		{
			name:             "pattern_with_hyphen",
			pattern:          "foo-bar.baz",
			expected:         `^foo-bar\.baz$`,
			expectErrKeyword: "",
			matches: []string{
				"foo-bar.baz",
			},
			nonMatches: []string{
				"foobar.baz",
				"foo_bar.baz",
			},
		},
		{
			name:             "pattern_with_underscore",
			pattern:          "foo_bar.baz",
			expected:         `^foo_bar\.baz$`,
			expectErrKeyword: "",
			matches: []string{
				"foo_bar.baz",
			},
			nonMatches: []string{
				"foobar.baz",
				"foo-bar.baz",
			},
		},
		{
			name:             "pattern_with_numbers",
			pattern:          "foo123.456baz",
			expected:         `^foo123\.456baz$`,
			expectErrKeyword: "",
			matches: []string{
				"foo123.456baz",
			},
			nonMatches: []string{
				"foo.baz",
				"foo123456baz",
			},
		},

		// Empty tokens test cases
		{
			name:             "invalid_pattern_with_empty_token",
			pattern:          "foo..bar",
			expected:         "",
			expectErrKeyword: "empty tokens",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "pattern_starting_with_dot",
			pattern:          ".foo.bar",
			expected:         "",
			expectErrKeyword: "empty tokens",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "pattern_ending_with_dot",
			pattern:          "foo.bar.",
			expected:         "",
			expectErrKeyword: "empty tokens",
			matches:          nil,
			nonMatches:       nil,
		},

		// Other error test cases
		{
			name:             "invalid_pattern_with_wildcard_in_middle",
			pattern:          "foo.>.bar",
			expected:         "",
			expectErrKeyword: "can only appear at the end",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "invalid_pattern_with_invalid_chars",
			pattern:          "foo#bar",
			expected:         "",
			expectErrKeyword: "invalid characters",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "invalid_pattern_with_wildcard_asterisk_in_token",
			pattern:          "foo.bar*baz.qux",
			expected:         "",
			expectErrKeyword: "invalid characters",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "invalid_pattern_with_wildcard_gt_in_token",
			pattern:          "foo.bar>baz.qux",
			expected:         "",
			expectErrKeyword: "invalid characters",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "invalid_pattern_with_uppercase_chars",
			pattern:          "foo.Bar.baz",
			expected:         "",
			expectErrKeyword: "invalid characters",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "empty_pattern",
			pattern:          "",
			expected:         "",
			expectErrKeyword: "cannot be empty",
			matches:          nil,
			nonMatches:       nil,
		},
		{
			name:             "pattern_exceeds_max_tokens",
			pattern:          "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u",
			expected:         "",
			expectErrKeyword: "cannot have more than",
			matches:          nil,
			nonMatches:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := ToRegexPattern(tt.pattern)

			// Check error condition
			if tt.expectErrKeyword != "" {
				require.Error(t, err, "ToRegexPattern(%q) should return an error", tt.pattern)
				assert.Contains(t, err.Error(), tt.expectErrKeyword,
					"ToRegexPattern(%q) error should contain %q but got: %v", tt.pattern, tt.expectErrKeyword, err)
				return
			}

			// No error expected
			require.NoError(t, err, "ToRegexPattern(%q) should not return an error", tt.pattern)
			assert.Equal(t, tt.expected, regex, "ToRegexPattern(%q) should return %q", tt.pattern, tt.expected)

			// Compile and test matches
			re, err := regexp.Compile(regex)
			require.NoError(t, err, "Failed to compile regex %q", regex)

			// Test pattern against matches
			for _, subj := range tt.matches {
				assert.True(t, re.MatchString(subj),
					"ToRegexPattern(%q) with regex %q should match %q", tt.pattern, regex, subj)
			}

			// Test pattern against non-matches
			for _, subj := range tt.nonMatches {
				assert.False(t, re.MatchString(subj),
					"ToRegexPattern(%q) with regex %q should NOT match %q", tt.pattern, regex, subj)
			}
		})
	}
}

// TestValidateSubject verifies the validation of NATS subjects.
// It tests various subjects including valid ones and those with invalid formats.
func TestValidateSubject(t *testing.T) {
	tests := []struct {
		name             string
		subject          string
		expectErrKeyword string // Error message keyword, empty string means no error expected
	}{
		// Valid subjects
		{
			name:             "valid_simple_subject",
			subject:          "foo",
			expectErrKeyword: "",
		},
		{
			name:             "valid_dotted_subject",
			subject:          "foo.bar.baz",
			expectErrKeyword: "",
		},
		{
			name:             "valid_subject_with_hyphen",
			subject:          "foo-bar.baz",
			expectErrKeyword: "",
		},
		{
			name:             "valid_subject_with_underscore",
			subject:          "foo_bar.baz",
			expectErrKeyword: "",
		},
		{
			name:             "valid_subject_with_numbers",
			subject:          "foo123.456baz",
			expectErrKeyword: "",
		},

		// Invalid subjects
		{
			name:             "empty_subject",
			subject:          "",
			expectErrKeyword: "cannot be empty",
		},
		{
			name:             "subject_with_empty_token",
			subject:          "foo..bar",
			expectErrKeyword: "empty tokens",
		},
		{
			name:             "subject_starting_with_dot",
			subject:          ".foo.bar",
			expectErrKeyword: "empty tokens",
		},
		{
			name:             "subject_ending_with_dot",
			subject:          "foo.bar.",
			expectErrKeyword: "empty tokens",
		},
		{
			name:             "subject_with_invalid_chars",
			subject:          "foo#bar",
			expectErrKeyword: "invalid characters",
		},
		{
			name:             "subject_with_uppercase_chars",
			subject:          "foo.Bar.baz",
			expectErrKeyword: "invalid characters",
		},
		{
			name:             "subject_with_asterisk_wildcard",
			subject:          "foo.*.bar",
			expectErrKeyword: "invalid characters",
		},
		{
			name:             "subject_with_gt_wildcard",
			subject:          "foo.>",
			expectErrKeyword: "invalid characters",
		},
		{
			name:             "pattern_exceeds_max_tokens",
			subject:          "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u",
			expectErrKeyword: "cannot have more than",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSubject(tt.subject)

			// Check error condition
			if tt.expectErrKeyword != "" {
				require.Error(t, err, "ValidateSubject(%q) should return an error", tt.subject)
				assert.Contains(t, err.Error(), tt.expectErrKeyword,
					"ValidateSubject(%q) error should contain %q but got: %v", tt.subject, tt.expectErrKeyword, err)
				return
			}

			// No error expected
			require.NoError(t, err, "ValidateSubject(%q) should not return an error", tt.subject)
		})
	}
}
