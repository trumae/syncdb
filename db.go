package syncdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/satori/go.uuid"
)

//ISyncDB inteface to synchronized DB
type ISyncDB interface {
	Begin() error
	Commit() error
	Rollback() error
	Exec(sql string, params []interface{}) error
	Query(sql string, params []interface{}) ([][]interface{}, []string, error)
}

//SQLreg record the sql smds
type SQLreg struct {
	SQL    string
	Params []interface{}
}

//SyncDB implementation
type SyncDB struct {
	sqlite    *sql.DB
	mu        sync.Mutex
	tx        *sql.Tx
	idtx      string
	seq       int
	port      int
	queryOnly bool
	name      string
	Debug     bool
}

var (
	//ErrDBInQueryOnlyMode is an error for this condition
	ErrDBInQueryOnlyMode = errors.New("DB in Query only mode")
)

type contextKeyDB int

const (
	keyDB contextKeyDB = iota
)

//New create a new instance of SyncDB
func New(arq string) (*SyncDB, error) {
	db, err := sql.Open("sqlite3", arq)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("PRAGMA INTEGRITY_CHECK")
	if err != nil {
		return nil, err
	}

	//Create logs tables
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS __DBTX__ (ID TEXT NOT NULL PRIMARY KEY, DATETIME TEXT)")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("create index if not exists datetime_dbtx_idx on __DBTX__(DATETIME)")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS __DBLOG__ (ID TEXT NOT NULL PRIMARY KEY, 
		TXID TEXT NOT NULL,
		SQL TEXT NOT NULL, 
		SEQ INT NOT NULL, 
		DATETIME TEXT)`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("create index if not exists txid_dblog_idx on __DBLOG__(TXID)")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("create index if not exists datetime_dblog_idx on __DBLOG__(DATETIME)")
	if err != nil {
		return nil, err
	}

	//Create settings tables
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS SETTINGS (ID INTEGER PRIMARY KEY AUTOINCREMENT, 
		KEY TEXT NOT NULL, 
		VALUE TEXT NOT NULL)`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("create unique index if not exists key_idx_unique_settings on settings (KEY)")
	if err != nil {
		return nil, err
	}

	DB := &SyncDB{sqlite: db}
	DB.initSettings()

	go func() {
		serverMux := http.NewServeMux()
		serverMux.HandleFunc("/txs", handleGetAllUUIDs)
		serverMux.HandleFunc("/diffs", handleDiffs)
		contextedMux := func() http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := context.WithValue(r.Context(), keyDB, DB)
				serverMux.ServeHTTP(w, r.WithContext(ctx))
			})
		}()
		for {
			DB.mu.Lock()
			DB.port = rand.Int()%10000 + 10000
			DB.mu.Unlock()

			log.Println(http.ListenAndServe(":"+strconv.Itoa(DB.port), contextedMux))
			time.Sleep(1 * time.Second)
		}
	}()

	return DB, err
}

func strace() string {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(3, pc)
	f := runtime.FuncForPC(pc[0])
	file, line := f.FileLine(pc[0])
	return fmt.Sprintf("%s:%d %s\n", file, line, f.Name())
}

//Begin init transaction
func (db *SyncDB) Begin() error {
	if db.Debug {
		log.Println("BEGIN", strace())
	}
	idtx := uuid.NewV4().String()
	return db.beginWithIDAndDatetime(idtx, "")
}

//beginWithIDAndDatetime init transaction
func (db *SyncDB) beginWithIDAndDatetime(idtx, datetime string) error {
	var err error

	db.mu.Lock()

	db.tx, err = db.sqlite.Begin()
	if err != nil {
		log.Println(err)
		return err
	}

	db.idtx = idtx
	db.queryOnly = false
	db.seq = 1
	if len(datetime) == 0 {
		_, err = db.tx.Exec("INSERT INTO __DBTX__(id, datetime) VALUES (?, datetime('now'))", db.idtx)
	} else {
		_, err = db.tx.Exec("INSERT INTO __DBTX__(id, datetime) VALUES (?, ?)", db.idtx, datetime)
	}
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

//BeginForQuery init transaction
func (db *SyncDB) BeginForQuery() error {
	if db.Debug {
		log.Println("BEGINFORQUERY", strace())
	}

	var err error

	db.mu.Lock()

	db.tx, err = db.sqlite.Begin()
	if err != nil {
		log.Println(err)
		return err
	}

	db.idtx = ""
	db.queryOnly = true

	return nil
}

//gcLog remove __DBTX__ entry without __DBLOG__ entries
func (db *SyncDB) gcLog() error {
	uuid := db.idtx
	res, _, err := db.Query("SELECT id FROM __DBLOG__ WHERE TXID = ?", []interface{}{uuid})
	if err != nil {
		log.Println(err)
		return err
	}

	if len(res) == 0 {
		err = db.ExecWithoutLog("DELETE FROM __DBTX__ WHERE ID = ?", []interface{}{uuid})
		if err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

//Commit confirm the current transaction
func (db *SyncDB) Commit() error {
	if db.Debug {
		log.Println("COMMIT", strace())
	}
	defer db.mu.Unlock()
	err := db.gcLog()
	if err != nil {
		log.Println(err)
		return err
	}

	err = db.tx.Commit()
	if err != nil {
		log.Println(err)
		return err
	}
	db.tx = nil
	return nil
}

//Rollback cancel the current transaction
func (db *SyncDB) Rollback() error {
	if db.Debug {
		log.Println("ROLLBACK", strace())
	}
	defer db.mu.Unlock()

	err := db.tx.Rollback()
	if err != nil {
		log.Println(err)
		return err
	}
	db.tx = nil
	return nil
}

//Exec execute sql on db
func (db *SyncDB) Exec(sql string, params []interface{}) error {
	if db.queryOnly {
		return ErrDBInQueryOnlyMode
	}

	_, err := db.tx.Exec(sql, params...)
	if err != nil {
		return err
	}

	reg := SQLreg{
		SQL:    sql,
		Params: params}

	b, err := json.Marshal(reg)
	if err != nil {
		return err
	}

	idlog := uuid.NewV4().String()
	_, err = db.tx.Exec("INSERT INTO __DBLOG__(id, txid, sql, seq, datetime) VALUES (?, ?, ?, ?, datetime('now'))",
		idlog,
		db.idtx,
		string(b),
		db.seq)
	if err != nil {
		return err
	}

	db.seq++
	return nil
}

//ExecWithoutLog execute sql on db
func (db *SyncDB) ExecWithoutLog(sql string, params []interface{}) error {
	_, err := db.tx.Exec(sql, params...)
	if err != nil {
		return err
	}

	return nil
}

//Query make a query on db return interfaces
func (db *SyncDB) Query(sql string, params []interface{}) ([][]interface{}, []string, error) {
	rows, err := db.tx.Query(sql, params...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	ret := [][]interface{}{}
	for rows.Next() {
		ss := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			s := new(string)
			//s := ""
			ss[i] = s
		}
		err = rows.Scan(ss...)
		ret = append(ret, ss)
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	return ret, cols, nil
}
