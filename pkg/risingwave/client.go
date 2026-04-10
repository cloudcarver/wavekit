package risingwave

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcarver/waitkit/pkg/backgroundddl"
	"github.com/jackc/pgx/v5"
)

const (
	defaultTimeout = 10 * time.Second
)

type ClusterConfig struct {
	SQLConnectionString string
	MetaNodeGrpcURL     string
	MetaNodeHTTPURL     string
}

type EndpointCheck struct {
	OK      bool
	Message string
}

type ConnectionStatus struct {
	SQL       EndpointCheck
	Meta      EndpointCheck
	CheckedAt time.Time
}

type RelationColumn struct {
	Name     string
	DataType string
}

type Relation struct {
	SchemaName   string
	RelationName string
	RelationType string
	Columns      []RelationColumn
}

type RelationSchema struct {
	SchemaName string
	Relations  []Relation
}

type RelationCategory struct {
	Name    string
	Schemas []RelationSchema
}

type SQLResult struct {
	Columns      []string
	Rows         [][]string
	CommandTag   string
	RowsAffected int64
}

type BackgroundJobProgress struct {
	Source    string
	JobID     int64
	Statement string
	Progress  *float64
}

type Client interface {
	ValidateCluster(ctx context.Context, cfg ClusterConfig) (*ConnectionStatus, error)
	ListDatabases(ctx context.Context, cfg ClusterConfig) ([]string, error)
	ListRelations(ctx context.Context, cfg ClusterConfig, database string) ([]RelationCategory, error)
	ExecuteSQL(ctx context.Context, cfg ClusterConfig, database, statement string) (*SQLResult, error)
	FindRelation(ctx context.Context, cfg ClusterConfig, database, schema, relationName, relationType string) (*Relation, error)
	ListBackgroundJobsByStatement(ctx context.Context, cfg ClusterConfig, database, statement string) ([]BackgroundJobProgress, error)
	CancelJobs(ctx context.Context, cfg ClusterConfig, database string, jobIDs []int64) error
}

type RWClient struct {
	httpClient *http.Client
}

func NewClient() Client {
	return &RWClient{
		httpClient: &http.Client{Timeout: defaultTimeout},
	}
}

func (c *RWClient) ValidateCluster(ctx context.Context, cfg ClusterConfig) (*ConnectionStatus, error) {
	checkedAt := time.Now().UTC()
	status := &ConnectionStatus{CheckedAt: checkedAt}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	conn, err := connect(ctx, cfg.SQLConnectionString, "")
	if err != nil {
		status.SQL = EndpointCheck{OK: false, Message: err.Error()}
		status.Meta = EndpointCheck{OK: false, Message: "skipped because SQL validation failed"}
		return status, nil
	}
	defer conn.Close(ctx)

	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		status.SQL = EndpointCheck{OK: false, Message: fmt.Sprintf("SELECT 1 failed: %v", err)}
		status.Meta = EndpointCheck{OK: false, Message: "skipped because SQL validation failed"}
		return status, nil
	}
	status.SQL = EndpointCheck{OK: true, Message: "validated with SELECT 1"}

	version, _ := detectVersion(ctx, conn)
	metaCheck := c.validateMeta(ctx, cfg, version)
	status.Meta = metaCheck

	return status, nil
}

func (c *RWClient) ListDatabases(ctx context.Context, cfg ClusterConfig) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	conn, err := connect(ctx, cfg.SQLConnectionString, "")
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			continue
		}
		databases = append(databases, valueToString(values[0]))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return databases, nil
}

func (c *RWClient) ListRelations(ctx context.Context, cfg ClusterConfig, database string) ([]RelationCategory, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	conn, err := connect(ctx, cfg.SQLConnectionString, database)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	relations, err := listRWRelations(ctx, conn)
	if err != nil {
		relations, err = listInformationSchemaRelations(ctx, conn)
		if err != nil {
			return nil, err
		}
	}
	return relations, nil
}

func (c *RWClient) ExecuteSQL(ctx context.Context, cfg ClusterConfig, database, statement string) (*SQLResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := connect(ctx, cfg.SQLConnectionString, database)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &SQLResult{}
	for _, field := range rows.FieldDescriptions() {
		result.Columns = append(result.Columns, field.Name)
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, 0, len(values))
		for _, value := range values {
			row = append(row, valueToString(value))
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result.CommandTag = rows.CommandTag().String()
	result.RowsAffected = rows.CommandTag().RowsAffected()
	return result, nil
}

func (c *RWClient) FindRelation(ctx context.Context, cfg ClusterConfig, database, schema, relationName, relationType string) (*Relation, error) {
	relations, err := c.ListRelations(ctx, cfg, database)
	if err != nil {
		return nil, err
	}
	for _, category := range relations {
		for _, relationSchema := range category.Schemas {
			for _, relation := range relationSchema.Relations {
				if relation.SchemaName != schema || relation.RelationName != relationName {
					continue
				}
				if relationType != "" && relation.RelationType != relationType {
					continue
				}
				relationCopy := relation
				return &relationCopy, nil
			}
		}
	}
	return nil, nil
}

func (c *RWClient) ListBackgroundJobsByStatement(ctx context.Context, cfg ClusterConfig, database, statement string) ([]BackgroundJobProgress, error) {
	target := backgroundddl.NormalizeStatement(statement)
	jobs := make([]BackgroundJobProgress, 0)
	for _, sourceStatement := range []struct {
		source    string
		statement string
	}{
		{source: "rw_ddl_progress", statement: "SELECT * FROM rw_ddl_progress LIMIT 100"},
		{source: "rw_cdc_progress", statement: "SELECT * FROM rw_cdc_progress LIMIT 100"},
	} {
		result, err := c.ExecuteSQL(ctx, cfg, database, sourceStatement.statement)
		if err != nil {
			if isMissingRelationError(err) {
				continue
			}
			return nil, err
		}
		jobs = append(jobs, extractBackgroundJobs(sourceStatement.source, target, result)...)
	}
	return jobs, nil
}

func (c *RWClient) CancelJobs(ctx context.Context, cfg ClusterConfig, database string, jobIDs []int64) error {
	if len(jobIDs) == 0 {
		return nil
	}
	parts := make([]string, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		parts = append(parts, strconv.FormatInt(jobID, 10))
	}
	_, err := c.ExecuteSQL(ctx, cfg, database, fmt.Sprintf("CANCEL JOBS %s", strings.Join(parts, ", ")))
	return err
}

func connect(ctx context.Context, dsn string, database string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if database != "" {
		config.Database = database
	}
	return pgx.ConnectConfig(ctx, config)
}

func detectVersion(ctx context.Context, conn *pgx.Conn) (string, error) {
	var versionString string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&versionString); err != nil {
		return "", err
	}
	re := regexp.MustCompile(`v?(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)`)
	match := re.FindStringSubmatch(versionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to detect version from %q", versionString)
	}
	return match[1], nil
}

func (c *RWClient) validateMeta(ctx context.Context, cfg ClusterConfig, version string) EndpointCheck {
	if cfg.MetaNodeGrpcURL == "" && cfg.MetaNodeHTTPURL == "" {
		return EndpointCheck{OK: false, Message: "meta endpoints not configured"}
	}

	var err error
	if cfg.MetaNodeGrpcURL != "" {
		binary, resolveErr := resolveRisectlBinary(version)
		if resolveErr == nil {
			commandCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
			cmd := exec.CommandContext(commandCtx, binary, "--meta-addr", cfg.MetaNodeGrpcURL, "meta", "cluster-info")
			output, runErr := cmd.CombinedOutput()
			if runErr == nil {
				return EndpointCheck{OK: true, Message: strings.TrimSpace(string(output))}
			}
			err = fmt.Errorf("risectl validation failed: %v (%s)", runErr, strings.TrimSpace(string(output)))
		} else {
			err = resolveErr
		}
	}

	if cfg.MetaNodeHTTPURL == "" {
		if err != nil {
			return EndpointCheck{OK: false, Message: err.Error()}
		}
		return EndpointCheck{OK: false, Message: "meta HTTP URL not configured"}
	}

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, cfg.MetaNodeHTTPURL, nil)
	if reqErr != nil {
		if err != nil {
			return EndpointCheck{OK: false, Message: err.Error()}
		}
		return EndpointCheck{OK: false, Message: reqErr.Error()}
	}
	resp, httpErr := c.httpClient.Do(req)
	if httpErr != nil {
		if err != nil {
			return EndpointCheck{OK: false, Message: fmt.Sprintf("%v; fallback HTTP validation failed: %v", err, httpErr)}
		}
		return EndpointCheck{OK: false, Message: httpErr.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		message := fmt.Sprintf("validated via HTTP fallback: %s", resp.Status)
		if err != nil {
			message = fmt.Sprintf("risectl unavailable, %s", message)
		}
		return EndpointCheck{OK: true, Message: message}
	}
	if err != nil {
		return EndpointCheck{OK: false, Message: fmt.Sprintf("%v; fallback HTTP status: %s", err, resp.Status)}
	}
	return EndpointCheck{OK: false, Message: fmt.Sprintf("unexpected HTTP status: %s", resp.Status)}
}

func resolveRisectlBinary(version string) (string, error) {
	if path, err := exec.LookPath("risectl"); err == nil {
		return path, nil
	}
	if version == "" {
		return "", errors.New("risectl not found in PATH and RisingWave version could not be detected")
	}

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	binaryName := "risectl"
	if runtime.GOOS == "windows" {
		binaryName = "risectl.exe"
	}
	binaryPath := filepath.Join(cacheDir, "wavekit", "risectl", version, binaryName)
	if info, statErr := os.Stat(binaryPath); statErr == nil && !info.IsDir() {
		return binaryPath, nil
	}

	if err := os.MkdirAll(filepath.Dir(binaryPath), 0o755); err != nil {
		return "", err
	}

	var lastErr error
	for _, downloadURL := range risectlDownloadCandidates(version) {
		if err := downloadAndExtract(downloadURL, binaryPath); err == nil {
			return binaryPath, nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no download candidates for GOOS=%s GOARCH=%s", runtime.GOOS, runtime.GOARCH)
	}
	return "", lastErr
}

func risectlDownloadCandidates(version string) []string {
	platforms := map[string][]string{
		"linux/amd64": {
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-v%s-x86_64-unknown-linux.tar.gz", version, version),
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-%s-x86_64-unknown-linux.tar.gz", version, version),
		},
		"linux/arm64": {
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-v%s-aarch64-unknown-linux.tar.gz", version, version),
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-%s-aarch64-unknown-linux.tar.gz", version, version),
		},
		"darwin/amd64": {
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-v%s-x86_64-apple-darwin.tar.gz", version, version),
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-%s-x86_64-apple-darwin.tar.gz", version, version),
		},
		"darwin/arm64": {
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-v%s-aarch64-apple-darwin.tar.gz", version, version),
			fmt.Sprintf("https://github.com/risingwavelabs/risingwave/releases/download/v%s/risingwave-%s-aarch64-apple-darwin.tar.gz", version, version),
		},
	}
	return platforms[runtime.GOOS+"/"+runtime.GOARCH]
}

func downloadAndExtract(downloadURL string, destination string) error {
	resp, err := http.Get(downloadURL) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: %s", resp.Status)
	}

	tmpFile, err := os.CreateTemp("", "wavekit-risectl-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return err
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	switch {
	case strings.HasSuffix(downloadURL, ".tar.gz"):
		return extractFromTarGz(tmpFile, destination)
	case strings.HasSuffix(downloadURL, ".zip"):
		return extractFromZip(tmpFile.Name(), destination)
	default:
		return fmt.Errorf("unsupported archive type: %s", downloadURL)
	}
}

func extractFromTarGz(file *os.File, destination string) error {
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		name := filepath.Base(header.Name)
		if name != "risectl" && name != "risectl.exe" {
			continue
		}
		out, err := os.Create(destination)
		if err != nil {
			return err
		}
		defer out.Close()
		if _, err := io.Copy(out, tarReader); err != nil {
			return err
		}
		return os.Chmod(destination, 0o755)
	}
	return errors.New("risectl binary not found in archive")
}

func extractFromZip(filePath string, destination string) error {
	reader, err := zip.OpenReader(filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, item := range reader.File {
		name := filepath.Base(item.Name)
		if name != "risectl" && name != "risectl.exe" {
			continue
		}
		src, err := item.Open()
		if err != nil {
			return err
		}
		defer src.Close()
		out, err := os.Create(destination)
		if err != nil {
			return err
		}
		defer out.Close()
		if _, err := io.Copy(out, src); err != nil {
			return err
		}
		return os.Chmod(destination, 0o755)
	}
	return errors.New("risectl binary not found in zip archive")
}

func listRWRelations(ctx context.Context, conn *pgx.Conn) ([]RelationCategory, error) {
	relationRows, err := conn.Query(ctx, `
		SELECT schema_name, relation_name, relation_type
		FROM rw_catalog.rw_relations
		ORDER BY schema_name, relation_type, relation_name
	`)
	if err != nil {
		return nil, err
	}
	defer relationRows.Close()

	relations := map[string]*Relation{}
	for relationRows.Next() {
		var schemaName, relationName, relationType string
		if err := relationRows.Scan(&schemaName, &relationName, &relationType); err != nil {
			return nil, err
		}
		normalized := normalizeRelationType(relationType)
		if normalized == "" {
			continue
		}
		key := schemaName + "." + relationName
		relations[key] = &Relation{
			SchemaName:   schemaName,
			RelationName: relationName,
			RelationType: normalized,
		}
	}
	if err := relationRows.Err(); err != nil {
		return nil, err
	}

	columnRows, err := conn.Query(ctx, `
		SELECT schema_name, relation_name, column_name, data_type
		FROM rw_catalog.rw_columns
		ORDER BY schema_name, relation_name, ordinal_position
	`)
	if err == nil {
		defer columnRows.Close()
		for columnRows.Next() {
			var schemaName, relationName, columnName, dataType string
			if err := columnRows.Scan(&schemaName, &relationName, &columnName, &dataType); err != nil {
				return nil, err
			}
			key := schemaName + "." + relationName
			if relation, ok := relations[key]; ok {
				relation.Columns = append(relation.Columns, RelationColumn{Name: columnName, DataType: dataType})
			}
		}
		if err := columnRows.Err(); err != nil {
			return nil, err
		}
	}

	return groupRelations(relations), nil
}

func listInformationSchemaRelations(ctx context.Context, conn *pgx.Conn) ([]RelationCategory, error) {
	relationRows, err := conn.Query(ctx, `
		SELECT table_schema, table_name,
		       CASE
		           WHEN table_type = 'BASE TABLE' THEN 'table'
		           WHEN table_type = 'VIEW' THEN 'materialized view'
		           ELSE lower(table_type)
		       END AS relation_type
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, relation_type, table_name
	`)
	if err != nil {
		return nil, err
	}
	defer relationRows.Close()

	relations := map[string]*Relation{}
	for relationRows.Next() {
		var schemaName, relationName, relationType string
		if err := relationRows.Scan(&schemaName, &relationName, &relationType); err != nil {
			return nil, err
		}
		normalized := normalizeRelationType(relationType)
		if normalized == "" {
			continue
		}
		key := schemaName + "." + relationName
		relations[key] = &Relation{
			SchemaName:   schemaName,
			RelationName: relationName,
			RelationType: normalized,
		}
	}
	if err := relationRows.Err(); err != nil {
		return nil, err
	}

	columnRows, err := conn.Query(ctx, `
		SELECT table_schema, table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name, ordinal_position
	`)
	if err != nil {
		return nil, err
	}
	defer columnRows.Close()

	for columnRows.Next() {
		var schemaName, relationName, columnName, dataType string
		if err := columnRows.Scan(&schemaName, &relationName, &columnName, &dataType); err != nil {
			return nil, err
		}
		key := schemaName + "." + relationName
		if relation, ok := relations[key]; ok {
			relation.Columns = append(relation.Columns, RelationColumn{Name: columnName, DataType: dataType})
		}
	}
	if err := columnRows.Err(); err != nil {
		return nil, err
	}

	return groupRelations(relations), nil
}

func groupRelations(relationMap map[string]*Relation) []RelationCategory {
	categoryOrder := []string{"source", "table", "materialized view", "sink"}
	categories := map[string]map[string][]Relation{}
	for _, relation := range relationMap {
		if _, ok := categories[relation.RelationType]; !ok {
			categories[relation.RelationType] = map[string][]Relation{}
		}
		categories[relation.RelationType][relation.SchemaName] = append(categories[relation.RelationType][relation.SchemaName], *relation)
	}

	var grouped []RelationCategory
	for _, name := range categoryOrder {
		schemasByName := categories[name]
		if len(schemasByName) == 0 {
			continue
		}

		schemaNames := make([]string, 0, len(schemasByName))
		for schemaName := range schemasByName {
			schemaNames = append(schemaNames, schemaName)
		}
		sort.Strings(schemaNames)

		schemas := make([]RelationSchema, 0, len(schemaNames))
		for _, schemaName := range schemaNames {
			relations := schemasByName[schemaName]
			sort.Slice(relations, func(i, j int) bool {
				return relations[i].RelationName < relations[j].RelationName
			})
			schemas = append(schemas, RelationSchema{
				SchemaName: schemaName,
				Relations:  relations,
			})
		}

		grouped = append(grouped, RelationCategory{Name: name, Schemas: schemas})
	}
	return grouped
}

func normalizeRelationType(relationType string) string {
	normalized := strings.ToLower(strings.TrimSpace(relationType))
	normalized = strings.ReplaceAll(normalized, "_", " ")
	switch {
	case strings.Contains(normalized, "source"):
		return "source"
	case strings.Contains(normalized, "materialized") && strings.Contains(normalized, "view"):
		return "materialized view"
	case normalized == "view":
		return "materialized view"
	case strings.Contains(normalized, "table"):
		return "table"
	case strings.Contains(normalized, "sink"):
		return "sink"
	default:
		return ""
	}
}

func extractBackgroundJobs(source string, targetStatement string, result *SQLResult) []BackgroundJobProgress {
	if result == nil {
		return nil
	}
	statementIdx := findColumnIndex(result.Columns, "statement")
	if statementIdx < 0 {
		return nil
	}
	jobIDIdx := findColumnIndex(result.Columns, "job_id", "id")
	progressIdx := findColumnIndex(result.Columns, "progress")

	jobs := make([]BackgroundJobProgress, 0)
	for _, row := range result.Rows {
		if statementIdx >= len(row) {
			continue
		}
		statement := row[statementIdx]
		if backgroundddl.NormalizeStatement(statement) != targetStatement {
			continue
		}
		job := BackgroundJobProgress{Source: source, Statement: statement}
		if jobIDIdx >= 0 && jobIDIdx < len(row) {
			if jobID, err := strconv.ParseInt(strings.TrimSpace(row[jobIDIdx]), 10, 64); err == nil {
				job.JobID = jobID
			}
		}
		if progressIdx >= 0 && progressIdx < len(row) {
			if progress, ok := parseProgressValue(row[progressIdx]); ok {
				job.Progress = &progress
			}
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func findColumnIndex(columns []string, names ...string) int {
	for idx, column := range columns {
		normalized := strings.ToLower(strings.TrimSpace(column))
		for _, name := range names {
			if normalized == strings.ToLower(name) {
				return idx
			}
		}
	}
	return -1
}

func parseProgressValue(value string) (float64, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(value, "%"))
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		return 0, false
	}
	progress, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, false
	}
	if progress <= 1 {
		progress = progress * 100
	}
	return progress, true
}

func isMissingRelationError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "does not exist") || strings.Contains(message, "unknown table")
}

func valueToString(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func withDatabase(dsn string, database string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + url.PathEscape(database)
	return parsed.String(), nil
}
