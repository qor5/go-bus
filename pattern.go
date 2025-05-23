package bus

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// MaxPatternTokens defines the maximum number of tokens allowed in a pattern
const MaxPatternTokens = 16

// reValidSubjectChars is a regex that matches valid characters for subjects.
var reValidSubjectChars = regexp.MustCompile(`^[a-z0-9_\-.]+$`)

// reValidTokenChars is a regex that matches valid characters for tokens.
var reValidTokenChars = regexp.MustCompile(`^[a-z0-9_\-]+$`)

// ValidateSubject validates that a subject follows NATS subject rules.
// It checks for empty subjects, invalid characters, subjects starting or ending with a dot,
// and empty tokens.
func ValidateSubject(subject string) error {
	if subject == "" {
		return errors.Wrap(ErrInvalidSubject, "subject cannot be empty")
	}

	if !reValidSubjectChars.MatchString(subject) {
		return errors.Wrap(ErrInvalidSubject, "subject contains invalid characters, only lowercase alphanumeric characters, '_', '-', and '.' are allowed")
	}

	tokens := strings.Split(subject, ".")
	if len(tokens) > MaxPatternTokens {
		return errors.Wrapf(ErrInvalidSubject, "subject cannot have more than %d tokens, got %d", MaxPatternTokens, len(tokens))
	}
	for _, token := range tokens {
		if token == "" {
			return errors.Wrap(ErrInvalidSubject, "subject contains empty tokens")
		}
	}

	return nil
}

// ValidatePattern validates that a pattern follows NATS wildcard rules.
// It checks for:
//   - Empty patterns are not allowed
//   - Empty tokens (parts between dots) are not allowed
//   - Only lowercase alphanumeric characters, '_', and '-' are allowed in non-wildcard tokens
//   - Wildcards can be '*' (single token) or '>' (multiple tokens)
//   - The '>' wildcard can only appear at the end of a pattern
//   - Pattern cannot exceed MaxPatternTokens tokens
func ValidatePattern(pattern string) error {
	if pattern == "" {
		return errors.Wrap(ErrInvalidPattern, "pattern cannot be empty")
	}

	tokens := strings.Split(pattern, ".")

	// Check token count limit
	if len(tokens) > MaxPatternTokens {
		return errors.Wrapf(ErrInvalidPattern, "pattern cannot have more than %d tokens, got %d", MaxPatternTokens, len(tokens))
	}

	for i, token := range tokens {
		switch token {
		case "":
			return errors.Wrap(ErrInvalidPattern, "pattern contains empty tokens")
		case "*":
		case ">":
			if i < len(tokens)-1 {
				return errors.Wrap(ErrInvalidPattern, "wildcard '>' can only appear at the end of a pattern")
			}
		default:
			if !reValidTokenChars.MatchString(token) {
				return errors.Wrapf(ErrInvalidPattern, "pattern contains invalid characters, only lowercase alphanumeric characters, '_', and '-' are allowed in token %q", token)
			}
		}
	}

	return nil
}

// ToRegexPattern converts a NATS-style wildcard pattern to a regex pattern
// that follows NATS specification for subject matching.
// It returns an error if the pattern contains invalid characters or structure.
func ToRegexPattern(pattern string) (string, error) {
	if err := ValidatePattern(pattern); err != nil {
		return "", err
	}

	tokens := strings.Split(pattern, ".")
	parts := make([]string, 0, len(tokens))

	for _, token := range tokens {
		switch token {
		case "*":
			parts = append(parts, "[^.]+")
		case ">":
			parts = append(parts, ".+")
		default:
			parts = append(parts, regexp.QuoteMeta(token))
		}
	}

	return "^" + strings.Join(parts, `\.`) + "$", nil
}
