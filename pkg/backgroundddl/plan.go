package backgroundddl

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type StatementKind string

type TargetKind string

const (
	StatementKindSet        StatementKind = "SET"
	StatementKindTrackedDDL StatementKind = "TRACKED_DDL"
	StatementKindDirect     StatementKind = "DIRECT"
)

const (
	TargetKindNone     TargetKind = "none"
	TargetKindRelation TargetKind = "relation"
	TargetKindFunction TargetKind = "function"
)

type StatementPlan struct {
	Statement            string
	Kind                 StatementKind
	TargetKind           TargetKind
	TargetType           string
	TargetSchema         string
	TargetName           string
	TargetIdentity       string
	ExpectRelationExists *bool
}

type sessionState struct {
	searchPath []string
}

var (
	txControlRe = regexp.MustCompile(`(?is)^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT|SET\s+TRANSACTION|SET\s+SESSION\s+CHARACTERISTICS\s+AS\s+TRANSACTION)\b`)
	setRe       = regexp.MustCompile(`(?is)^\s*SET\b`)

	createTablePrefix            = regexp.MustCompile(`(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	createMaterializedViewPrefix = regexp.MustCompile(`(?is)^\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	createSourcePrefix           = regexp.MustCompile(`(?is)^\s*CREATE\s+SOURCE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	createSinkPrefix             = regexp.MustCompile(`(?is)^\s*CREATE\s+SINK\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	createFunctionPrefix         = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+`)

	dropTablePrefix            = regexp.MustCompile(`(?is)^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?`)
	dropMaterializedViewPrefix = regexp.MustCompile(`(?is)^\s*DROP\s+MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?`)
	dropSourcePrefix           = regexp.MustCompile(`(?is)^\s*DROP\s+SOURCE\s+(?:IF\s+EXISTS\s+)?`)
	dropSinkPrefix             = regexp.MustCompile(`(?is)^\s*DROP\s+SINK\s+(?:IF\s+EXISTS\s+)?`)
	dropFunctionPrefix         = regexp.MustCompile(`(?is)^\s*DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?`)

	setSearchPathPrefix = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?search_path\s*(?:TO|=)\s*`)
	setSchemaPrefix     = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?SCHEMA\s+`)
	whitespaceRe        = regexp.MustCompile(`\s+`)
)

func PlanStatements(input string) ([]StatementPlan, error) {
	parts, err := pg_query.SplitWithScanner(input, true)
	if err != nil {
		return nil, err
	}

	state := sessionState{searchPath: []string{"public"}}
	plans := make([]StatementPlan, 0, len(parts))
	for _, part := range parts {
		statement := strings.TrimSpace(part)
		if statement == "" {
			continue
		}
		plan, err := analyzeStatement(statement, &state)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	if len(plans) == 0 {
		return nil, fmt.Errorf("at least one SQL statement is required")
	}
	return plans, nil
}

func ComposeStatements(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := trimStatementTerminator(part)
		if trimmed == "" {
			continue
		}
		cleaned = append(cleaned, trimmed)
	}
	return strings.Join(cleaned, ";\n")
}

func NormalizeStatement(statement string) string {
	trimmed := trimStatementTerminator(statement)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(whitespaceRe.ReplaceAllString(trimmed, " "))
}

func analyzeStatement(statement string, state *sessionState) (StatementPlan, error) {
	if txControlRe.MatchString(statement) {
		return StatementPlan{}, fmt.Errorf("transaction control statements are not supported in background DDL jobs")
	}

	if setRe.MatchString(statement) {
		if updatedSearchPath, ok := parseSearchPathUpdate(statement); ok {
			state.searchPath = updatedSearchPath
		}
		return StatementPlan{
			Statement:  statement,
			Kind:       StatementKindSet,
			TargetKind: TargetKindNone,
		}, nil
	}

	if remainder, ok := stripPrefix(createTablePrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "table", true)
	}
	if remainder, ok := stripPrefix(createMaterializedViewPrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "materialized view", true)
	}
	if remainder, ok := stripPrefix(createSourcePrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "source", true)
	}
	if remainder, ok := stripPrefix(createSinkPrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "sink", true)
	}
	if remainder, ok := stripPrefix(dropTablePrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "table", false)
	}
	if remainder, ok := stripPrefix(dropMaterializedViewPrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "materialized view", false)
	}
	if remainder, ok := stripPrefix(dropSourcePrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "source", false)
	}
	if remainder, ok := stripPrefix(dropSinkPrefix, statement); ok {
		return makeRelationPlan(statement, state, remainder, "sink", false)
	}
	if remainder, ok := stripPrefix(createFunctionPrefix, statement); ok {
		return makeFunctionPlan(statement, state, remainder)
	}
	if remainder, ok := stripPrefix(dropFunctionPrefix, statement); ok {
		return makeFunctionPlan(statement, state, remainder)
	}

	return StatementPlan{
		Statement:  statement,
		Kind:       StatementKindDirect,
		TargetKind: TargetKindNone,
	}, nil
}

func makeRelationPlan(statement string, state *sessionState, remainder string, relationType string, expectExists bool) (StatementPlan, error) {
	ident, ok := readQualifiedIdentifier(remainder)
	if !ok {
		return StatementPlan{}, fmt.Errorf("failed to parse relation name from statement %q", statement)
	}
	schema, name, err := resolveQualifiedIdentifier(ident, state.searchPath)
	if err != nil {
		return StatementPlan{}, err
	}
	return StatementPlan{
		Statement:            statement,
		Kind:                 StatementKindTrackedDDL,
		TargetKind:           TargetKindRelation,
		TargetType:           relationType,
		TargetSchema:         schema,
		TargetName:           name,
		ExpectRelationExists: boolPtr(expectExists),
	}, nil
}

func makeFunctionPlan(statement string, state *sessionState, remainder string) (StatementPlan, error) {
	ident, ok := readQualifiedIdentifier(remainder)
	if !ok {
		return StatementPlan{}, fmt.Errorf("failed to parse function name from statement %q", statement)
	}
	schema, name, err := resolveQualifiedIdentifier(ident, state.searchPath)
	if err != nil {
		return StatementPlan{}, err
	}
	return StatementPlan{
		Statement:    statement,
		Kind:         StatementKindDirect,
		TargetKind:   TargetKindFunction,
		TargetType:   "function",
		TargetSchema: schema,
		TargetName:   name,
	}, nil
}

func parseSearchPathUpdate(statement string) ([]string, bool) {
	if remainder, ok := stripPrefix(setSearchPathPrefix, statement); ok {
		path := parseSearchPathList(remainder)
		if len(path) == 0 {
			return []string{"public"}, true
		}
		return path, true
	}
	if remainder, ok := stripPrefix(setSchemaPrefix, statement); ok {
		ident, ok := readQualifiedIdentifier(remainder)
		if !ok {
			ident = firstCSVValue(remainder)
		}
		ident = strings.TrimSpace(ident)
		if ident == "" {
			return []string{"public"}, true
		}
		return []string{normalizeSearchPathEntry(ident)}, true
	}
	return nil, false
}

func parseSearchPathList(remainder string) []string {
	value := trimStatementTerminator(remainder)
	if strings.EqualFold(strings.TrimSpace(value), "DEFAULT") {
		return []string{"public"}
	}

	parts := splitCSV(value)
	searchPath := make([]string, 0, len(parts))
	for _, part := range parts {
		entry := normalizeSearchPathEntry(part)
		if entry == "" || entry == "$user" {
			continue
		}
		searchPath = append(searchPath, entry)
	}
	if len(searchPath) == 0 {
		return []string{"public"}
	}
	return searchPath
}

func normalizeSearchPathEntry(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimSuffix(trimmed, ";")
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "\"") {
		unquoted, err := unquoteIdentifier(trimmed)
		if err == nil {
			return unquoted
		}
	}
	if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") && len(trimmed) >= 2 {
		return strings.Trim(trimmed, "'")
	}
	return strings.TrimSpace(trimmed)
}

func resolveQualifiedIdentifier(qualified string, searchPath []string) (string, string, error) {
	parts, err := splitQualifiedIdentifier(qualified)
	if err != nil {
		return "", "", err
	}
	if len(parts) == 1 {
		schema := "public"
		if len(searchPath) > 0 && strings.TrimSpace(searchPath[0]) != "" {
			schema = searchPath[0]
		}
		return schema, parts[0], nil
	}
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "", "", fmt.Errorf("unsupported qualified identifier %q", qualified)
}

func readQualifiedIdentifier(input string) (string, bool) {
	i := 0
	for i < len(input) && isSpace(input[i]) {
		i++
	}
	start := i
	if start >= len(input) {
		return "", false
	}
	if _, next, ok := readIdentifierPart(input, i); !ok {
		return "", false
	} else {
		i = next
	}
	for {
		for i < len(input) && isSpace(input[i]) {
			i++
		}
		if i >= len(input) || input[i] != '.' {
			break
		}
		i++
		for i < len(input) && isSpace(input[i]) {
			i++
		}
		if _, next, ok := readIdentifierPart(input, i); !ok {
			return "", false
		} else {
			i = next
		}
	}
	return strings.TrimSpace(input[start:i]), true
}

func splitQualifiedIdentifier(qualified string) ([]string, error) {
	parts := make([]string, 0, 2)
	input := strings.TrimSpace(qualified)
	i := 0
	for i < len(input) {
		for i < len(input) && isSpace(input[i]) {
			i++
		}
		part, next, ok := readIdentifierPart(input, i)
		if !ok {
			return nil, fmt.Errorf("invalid identifier %q", qualified)
		}
		parts = append(parts, part)
		i = next
		for i < len(input) && isSpace(input[i]) {
			i++
		}
		if i >= len(input) {
			break
		}
		if input[i] != '.' {
			return nil, fmt.Errorf("invalid qualified identifier %q", qualified)
		}
		i++
	}
	return parts, nil
}

func readIdentifierPart(input string, start int) (string, int, bool) {
	if start >= len(input) {
		return "", start, false
	}
	if input[start] == '"' {
		end := start + 1
		for end < len(input) {
			if input[end] == '"' {
				if end+1 < len(input) && input[end+1] == '"' {
					end += 2
					continue
				}
				unquoted, err := unquoteIdentifier(input[start : end+1])
				if err != nil {
					return "", start, false
				}
				return unquoted, end + 1, true
			}
			end++
		}
		return "", start, false
	}
	end := start
	for end < len(input) {
		ch := input[end]
		if isSpace(ch) || ch == '.' || ch == '(' || ch == ',' || ch == ';' {
			break
		}
		end++
	}
	if end == start {
		return "", start, false
	}
	return input[start:end], end, true
}

func unquoteIdentifier(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		return "", fmt.Errorf("invalid quoted identifier %q", value)
	}
	inner := trimmed[1 : len(trimmed)-1]
	return strings.ReplaceAll(inner, `""`, `"`), nil
}

func splitCSV(input string) []string {
	parts := []string{}
	current := strings.Builder{}
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch ch {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			current.WriteByte(ch)
		case '"':
			if !inSingleQuote {
				if inDoubleQuote && i+1 < len(input) && input[i+1] == '"' {
					current.WriteByte(ch)
					current.WriteByte(input[i+1])
					i++
					continue
				}
				inDoubleQuote = !inDoubleQuote
			}
			current.WriteByte(ch)
		case ',':
			if inSingleQuote || inDoubleQuote {
				current.WriteByte(ch)
				continue
			}
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}

func firstCSVValue(input string) string {
	parts := splitCSV(input)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func stripPrefix(re *regexp.Regexp, input string) (string, bool) {
	loc := re.FindStringIndex(input)
	if loc == nil || loc[0] != 0 {
		return "", false
	}
	return input[loc[1]:], true
}

func trimStatementTerminator(statement string) string {
	trimmed := strings.TrimSpace(statement)
	trimmed = strings.TrimSuffix(trimmed, ";")
	return strings.TrimSpace(trimmed)
}

func isSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func boolPtr(value bool) *bool {
	return &value
}
