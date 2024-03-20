package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb" // Underlies database/sql
)

type DataType int

const (
	INVALID DataType = iota
	VARCHAR
	DOUBLE
	INTEGER
	BOOLEAN
)

func (k DataType) DBType() string {
	return map[DataType]string{
		INVALID: "",
		VARCHAR: "VARCHAR",
		DOUBLE:  "DOUBLE",
		INTEGER: "INTEGER",
		BOOLEAN: "BOOLEAN",
	}[k]
}

func NewDataType(in any) DataType {
	switch in.(type) {
	case float64, float32:
		return DOUBLE
	case int, int32, int64:
		return INTEGER
	case string:
		return VARCHAR
	case bool:
		return BOOLEAN
	default:
		return INVALID
	}
}

func (k DataType) Valid() bool {
	return k != INVALID
}

type Store struct {
	db        *sql.DB
	writeLock sync.Mutex
}

func NewDuckDBStore() (*Store, error) {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		return nil, fmt.Errorf("opening duckdb: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}
	return nil
}

func (s *Store) Query(ctx context.Context, stmt *QueryStatement) ([]map[string]any, error) {
	if err := stmt.Valid(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, stmt.Query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Error("closing rows: %w", closeErr)
		}
	}()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Query response Columns: %w", err)
	}
	// TODO: Find a way to estimate the size of the result set to reduce gc overhead.
	var out []map[string]any
	for rows.Next() {
		columns := make([]any, len(cols))
		// TODO: Reuse this pointer array to avoid individual allocation for synchronous process.
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err = rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}

		m := make(map[string]any)
		for i, colName := range cols {
			val, _ := columnPointers[i].(*any)
			m[colName] = *val
		}
		out = append(out, m)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("flushing rows: %w", err)
	}

	return out, nil
}

func (s *Store) Insert(ctx context.Context, stmt *InsertStatement) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	query, values, err := stmt.Query()
	if err != nil {
		return err
	}

	for {
		_, insertErr := s.db.ExecContext(ctx, query, values...)
		if insertErr == nil {
			break
		}
		handledErr := s.handleInsertError(ctx, stmt, insertErr)
		if handledErr != nil {
			return handledErr
		}
	}

	return nil
}

var missingTableRegex = regexp.MustCompile(
	`Catalog Error: Table with name [a-zA-Z_]+ does not exist!`,
)
var missingColumnRegex = regexp.MustCompile(
	`Binder Error: Table "[a-zA-Z_]+" does not have a column with name "([a-zA-Z_]+)"`,
)

// handleInsertError is the mechanism for syncing the given schema from the InsertStatement with the sql catalog.
func (s *Store) handleInsertError(ctx context.Context, stmt *InsertStatement, err error) error {
	if err == nil {
		return nil
	}
	if missingTableRegex.MatchString(err.Error()) {
		return s.CreateTable(ctx, stmt)
	}
	if missingColumnRegex.MatchString(err.Error()) {
		matches := missingColumnRegex.FindStringSubmatch(err.Error())
		return s.AddColumn(ctx, stmt, matches[1])
	}
	return fmt.Errorf("inserting values: %w", err)
}

func (s *Store) CreateTable(ctx context.Context, stmt *InsertStatement) error {
	query, err := stmt.CreateTableQueryString()
	if err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating Table: %w", err)
	}
	return nil
}

func (s *Store) AddColumn(ctx context.Context, stmt *InsertStatement, name string) error {
	query, err := stmt.AddColumnQueryString(name)
	if err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating Table: %w", err)
	}
	return nil
}

type InsertStatement struct {
	Table   string
	Columns map[string]any
}

func (s *InsertStatement) CreateTableQueryString() (string, error) {
	cols := make([]string, 0, len(s.Columns))
	for k, v := range s.Columns {
		kind := NewDataType(v)
		if !kind.Valid() {
			return "", fmt.Errorf("create Table: invalid data type for column (%s): %T", k, v)
		}
		cols = append(cols, fmt.Sprintf("%s %s", k, kind.DBType()))
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s(%s)",
		s.Table,
		strings.Join(cols, ", "),
	), nil
}

func (s *InsertStatement) AddColumnQueryString(name string) (string, error) {
	value, ok := s.Columns[name]
	if !ok {
		return "", fmt.Errorf("add column: column not present in InsertStatement: %s", name)
	}
	kind := NewDataType(value)
	if !kind.Valid() {
		return "", fmt.Errorf("add column: invalid data type for column (%s): %T", name, value)
	}

	return fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s %s",
		s.Table,
		name,
		kind.DBType(),
	), nil
}

func (s *InsertStatement) Validate() error {
	if s == nil {
		return errors.New("invalid InsertStatement: nil")
	}

	// TODO: validate Table name to have no spaces, etc.
	if s.Table == "" {
		return errors.New("invalid InsertStatement: missing Table name")
	}

	if len(s.Columns) == 0 {
		return errors.New("invalid InsertStatement: no Columns")
	}
	// TODO: Consider validating column names for sql acceptance.
	return nil
}

func (s *InsertStatement) Query() (string, []any, error) {
	keys := make([]string, 0, len(s.Columns))
	values := make([]any, 0, len(s.Columns))
	placeholders := make([]string, 0, len(s.Columns))
	for k, v := range s.Columns {
		keys = append(keys, k)
		values = append(values, v)
		placeholders = append(placeholders, "?")
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		s.Table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	), values, nil
}

type QueryStatement struct {
	Query string
}

func (s *QueryStatement) Valid() error {
	if s == nil {
		return errors.New("invalid QueryStatement: nil")
	}

	if s.Query == "" {
		return errors.New("invalid QueryStatement: Query empty")
	}
	return nil
}
