package plainkv

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// PlainKV is a key-value database that uses
// MySQL/MariaDB as its storage backend
type PlainKV struct {
	DSN       string // Data Source Name
	db        *sql.DB
	currBuckt string
}

const (
	mimeBuckt string = `--mime--`
)

// NewPlainKV creates a new PlainKV object
// This is the recommended method
func NewPlainKV(dsn string) *PlainKV {
	return &PlainKV{
		DSN:       dsn,
		currBuckt: `default`,
	}
}

// Get retrieves a record using a key
func (p *PlainKV) Get(key string) ([]byte, error) {

	var (
		err error
		val []byte
	)

	val = make([]byte, 0)

	if err = p.Open(); err != nil {
		return val, err
	}

	if p.currBuckt == "" {
		p.currBuckt = "default"
	}

	if err = p.db.QueryRow(`SELECT Value
							FROM KeyValueTBL
							WHERE Bucket=?
								AND KeyID=?;`,
		p.currBuckt,
		key).Scan(&val); err != nil {

		return val, err
	}

	return val, nil
}

// Get retrieves a record using a key
func (p *PlainKV) GetMime(key string) (string, error) {

	var (
		err error
		val []byte
	)

	val = make([]byte, 0)

	if err = p.Open(); err != nil {
		return "", err
	}

	if err = p.db.QueryRow(`SELECT Value
							FROM KeyValueTBL
							WHERE Bucket=?
								AND KeyID=?;`,
		mimeBuckt,
		key).Scan(&val); err != nil {

		return string(val), err
	}

	if len(val) == 0 {
		return "text/html", nil
	}

	return string(val), nil
}

// Set creates or updates the record by the value
func (p *PlainKV) Set(key string, value []byte) error {

	if p.currBuckt == "" {
		p.currBuckt = "default"
	}

	if err := p.set(p.currBuckt, key, value); err != nil {
		return err
	}

	return nil
}

// SetMime sets the mime of the value stored
func (p *PlainKV) SetMime(key string, mime string) error {

	if err := p.set(mimeBuckt, key, []byte(mime)); err != nil {
		return err
	}
	return nil
}

// SetBucket sets the current bucket.
// If set, all succeeding values will be retrieved and stored by the bucket name
func (p *PlainKV) SetBucket(bucket string) {
	p.currBuckt = bucket
}

// Del deletes a record with the provided key
func (p *PlainKV) Del(key string) error {

	var (
		err error
	)

	if err = p.Open(); err != nil {
		return err
	}

	if p.currBuckt == "" {
		p.currBuckt = "default"
	}

	if _, err = p.db.Exec(`DELETE FROM KeyValueTBL
							WHERE Bucket = ?
								AND KeyID = ?;`,
		p.currBuckt,
		key); err != nil {
		return err
	}

	if _, err = p.db.Exec(`DELETE FROM KeyValueTBL
							WHERE Bucket = ?
								AND KeyID = ?;`,
		mimeBuckt,
		key); err != nil {
		return err
	}

	return nil
}

// ListKeys lists all keys containing the current pattern
func (p *PlainKV) ListKeys(pattern string) ([]string, error) {
	var (
		err error
		val []string
		k   string
	)

	val = make([]string, 0)

	if err = p.Open(); err != nil {
		return val, err
	}

	if p.currBuckt == "" {
		p.currBuckt = "default"
	}

	sqr, err := p.db.Query(`SELECT KeyID
							FROM KeyValueTBL
							WHERE Bucket=?
								AND KeyID LIKE ?;`,
		p.currBuckt,
		pattern+"%")
	if err != nil {
		return val, err
	}
	defer sqr.Close()

	for sqr.Next() {
		if err = sqr.Scan(&k); err != nil {
			return val, err
		}

		val = append(val, k)
	}

	if err = sqr.Err(); err != nil {
		return val, err
	}

	return val, nil
}

// Open a connection to a MySQL database database
func (p *PlainKV) Open() error {

	if p.db == nil {
		var err error

		p.db, err = sql.Open("mysql", p.DSN)
		if err != nil {
			return err
		}

		// See "Important settings" section.
		p.db.SetConnMaxLifetime(time.Minute * 3)
		p.db.SetMaxOpenConns(10)
		p.db.SetMaxIdleConns(10)

		// Check if table exists and create it if not
		p.db.Exec(`CREATE TABLE IF NOT EXISTS KeyValueTBL (
						Bucket VARCHAR(50),
						KeyID VARCHAR(300),
						Value MEDIUMBLOB,
						PRIMARY KEY (Bucket, KeyID)
					);`)
	}

	return nil
}

// Close closes the database
func (p *PlainKV) Close() error {
	if p.db != nil {
		return p.db.Close()
	}

	return nil
}

// Set creates or updates the record by the value
func (p *PlainKV) set(bucket, key string, value []byte) error {

	var (
		err error
	)

	if err = p.Open(); err != nil {
		return err
	}

	if _, err = p.db.Exec(`INSERT INTO KeyValueTBL VALUES (?, ?, ?)
							ON DUPLICATE KEY
								UPDATE Value=?;`,
		bucket,
		key,
		value,
		value); err != nil {
		return err
	}

	return nil
}
