package plainkv

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type PlainKV struct {
	DSN       string // Data Source Name
	db        *sql.DB
	currBuckt string
}

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

// Set creates or updates the record by the value
func (p *PlainKV) Set(key string, value []byte) error {

	var (
		err error
	)

	if err = p.Open(); err != nil {
		return err
	}

	if p.currBuckt == "" {
		p.currBuckt = "default"
	}

	if _, err = p.db.Exec(`INSERT INTO KeyValueTBL VALUES (?, ?, ?)
							ON DUPLICATE KEY
								UPDATE Value=?;`,
		p.currBuckt,
		key,
		value,
		value); err != nil {
		return err
	}

	return nil
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

	return nil
}

// SetBucket sets the current bucket
func (p *PlainKV) SetBucket(bucket string) {
	p.currBuckt = bucket
}

// Open a connection to the database
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
						Value MEDIUMTEXT,
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
