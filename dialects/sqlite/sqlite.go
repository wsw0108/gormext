package sqlite

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jinzhu/gorm"

	_ "modernc.org/sqlite"
)

type sqlite3 struct {
	db gorm.SQLCommon
}

// commonDialect from https://github.com/jinzhu/gorm/blob/master/dialect_common.go {{{

var keyNameRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

// SetDB implements gorm.Dialect.
func (s *sqlite3) SetDB(db gorm.SQLCommon) {
	s.db = db
}

// BindVar implements gorm.Dialect.
func (*sqlite3) BindVar(i int) string {
	return "$$$" // ?
}

// Quote implements gorm.Dialect.
func (*sqlite3) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (s *sqlite3) fieldCanAutoIncrement(field *gorm.StructField) bool {
	if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}

// RemoveIndex implements gorm.Dialect.
func (s *sqlite3) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	return err
}

// HasForeignKey implements gorm.Dialect.
func (*sqlite3) HasForeignKey(tableName string, foreignKeyName string) bool {
	return false
}

// ModifyColumn implements gorm.Dialect.
func (s *sqlite3) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v", tableName, columnName, typ))
	return err
}

// LimitAndOffsetSQL implements gorm.Dialect.
func (s *sqlite3) LimitAndOffsetSQL(limit interface{}, offset interface{}) (sql string, err error) {
	if limit != nil {
		if parsedLimit, err := s.parseInt(limit); err != nil {
			return "", err
		} else if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)
		}
	}
	if offset != nil {
		if parsedOffset, err := s.parseInt(offset); err != nil {
			return "", err
		} else if parsedOffset >= 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		}
	}
	return
}

// SelectFromDummyTable implements gorm.Dialect.
func (*sqlite3) SelectFromDummyTable() string {
	return ""
}

// LastInsertIDOutputInterstitial implements gorm.Dialect.
func (*sqlite3) LastInsertIDOutputInterstitial(tableName string, columnName string, columns []string) string {
	return ""
}

// LastInsertIDReturningSuffix implements gorm.Dialect.
func (*sqlite3) LastInsertIDReturningSuffix(tableName string, columnName string) string {
	return ""
}

// DefaultValueStr implements gorm.Dialect.
func (*sqlite3) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

// BuildKeyName implements gorm.Dialect.
func (*sqlite3) BuildKeyName(kind string, tableName string, fields ...string) string {
	keyName := fmt.Sprintf("%s_%s_%s", kind, tableName, strings.Join(fields, "_"))
	keyName = keyNameRegex.ReplaceAllString(keyName, "_")
	return keyName
}

// NormalizeIndexAndColumn implements gorm.Dialect.
func (*sqlite3) NormalizeIndexAndColumn(indexName string, columnName string) (string, string) {
	return indexName, columnName
}

func (*sqlite3) parseInt(value interface{}) (int64, error) {
	return strconv.ParseInt(fmt.Sprint(value), 0, 0)
}

// }}} commonDialect

// dialect_sqlite3 from https://github.com/jinzhu/gorm/blob/master/dialect_sqlite3.go {{{

// GetName implements gorm.Dialect.
func (*sqlite3) GetName() string {
	return "sqlite"
}

// DataTypeOf implements gorm.Dialect.
func (s *sqlite3) DataTypeOf(field *gorm.StructField) string {
	dataValue, sqlType, size, additionalType := gorm.ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "bool"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "integer primary key autoincrement"
			} else {
				sqlType = "integer"
			}
		case reflect.Int64, reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "integer primary key autoincrement"
			} else {
				sqlType = "bigint"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "real"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "text"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "datetime"
			}
		default:
			if gorm.IsByteArrayOrSlice(dataValue) {
				sqlType = "blob"
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for sqlite3", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

// HasIndex implements gorm.Dialect.
func (s *sqlite3) HasIndex(tableName string, indexName string) bool {
	var count int
	s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM sqlite_master WHERE tbl_name = ? AND sql LIKE '%%INDEX %v ON%%'", indexName), tableName).Scan(&count)
	return count > 0
}

// HasTable implements gorm.Dialect.
func (s *sqlite3) HasTable(tableName string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&count)
	return count > 0
}

// HasColumn implements gorm.Dialect.
func (s *sqlite3) HasColumn(tableName string, columnName string) bool {
	var count int
	s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM sqlite_master WHERE tbl_name = ? AND (sql LIKE '%%\"%v\" %%' OR sql LIKE '%%%v %%');\n", columnName, columnName), tableName).Scan(&count)
	return count > 0
}

// CurrentDatabase implements gorm.Dialect.
func (s *sqlite3) CurrentDatabase() (name string) {
	var (
		ifaces   = make([]interface{}, 3)
		pointers = make([]*string, 3)
		i        int
	)
	for i = 0; i < 3; i++ {
		ifaces[i] = &pointers[i]
	}
	if err := s.db.QueryRow("PRAGMA database_list").Scan(ifaces...); err != nil {
		return
	}
	if pointers[1] != nil {
		name = *pointers[1]
	}
	return
}

/// }}} dialect_sqlite3

func init() {
	gorm.RegisterDialect("sqlite", &sqlite3{})
}
