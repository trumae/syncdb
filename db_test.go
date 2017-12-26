package syncdb

import (
	"testing"
)

func TestNewSyncDB(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	err = s.Begin()
	if err != nil {
		t.Error(err)
	}

	err = s.Commit()
	if err != nil {
		t.Error(err)
	}

	err = s.Begin()
	if err != nil {
		t.Error(err)
	}

	err = s.Rollback()
	if err != nil {
		t.Error(err)
	}

}

func TestSyncDBCreateTable(t *testing.T) {
	db, err := New(":memory:")
	///db, err := New("ttt.db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Begin()
	if err != nil {
		t.Error(err)
	}

	err = db.Exec("create table foo(id integer not null primary key, name text);", []interface{}{})
	if err != nil {
		t.Error(err)
	}

	err = db.Exec("insert into foo values (NULL, ?)", []interface{}{"teste"})
	if err != nil {
		t.Error(err)
	}

	err = db.Commit()
	if err != nil {
		t.Error(err)
	}

	err = db.Begin()
	if err != nil {
		t.Error(err)
	}

	rows, cols, err := db.Query("select * from foo", []interface{}{})
	if err != nil {
		t.Error(err)
	}

	if len(cols) != 2 {
		t.Error("Wrong number of cols")
	}

	if len(rows) != 1 {
		t.Error("Wrong number of rows")
	}

	err = db.Commit()
	if err != nil {
		t.Error(err)
	}

}

func TestQuery(t *testing.T) {
	db, err := New(":memory:")
	///db, err := New("ttt.db")
	if err != nil {
		t.Fatal(err)
	}

	db.Begin()
	defer db.Commit()

	db.Exec("create table foo(id integer not null primary key, name text)", []interface{}{})
	db.Exec("insert into foo values (NULL, ?)", []interface{}{"teste1"})
	db.Exec("insert into foo values (NULL, ?)", []interface{}{"teste2"})
	db.Exec("insert into foo values (NULL, ?)", []interface{}{"teste3"})
	db.Exec("insert into foo values (NULL, ?)", []interface{}{"teste4"})

	rows, _, err := db.Query("select name from foo", []interface{}{})
	if err != nil {
		t.Error(err)
	}

	if len(rows) != 4 {
		t.Error("Wrong number of rows")
	}

	if *rows[0][0].(*string) != "teste1" {
		t.Error("Expected 'teste1' - value", rows[0][0].(*string))
	}
	if *rows[1][0].(*string) != "teste2" {
		t.Error("Expected 'teste2' - value", rows[1][0].(*string))
	}
	if *rows[2][0].(*string) != "teste3" {
		t.Error("Expected 'teste3' - value", rows[2][0].(*string))
	}
	if *rows[3][0].(*string) != "teste4" {
		t.Error("Expected 'teste4' - value", rows[3][0].(*string))
	}
}
