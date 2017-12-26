package syncdb

import (
	"testing"
	"time"
)

const (
	idcompany = "company1"
	node1     = "node1"
	node2     = "node2"
)

func TestListTX(t *testing.T) {
	_, err := discoverNodes("192.168.0.101", idcompany, "12345", node1)
	if err != nil {
		t.Error(err)
	}
}

func TestSync(t *testing.T) {
	db1, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db1.name = "DB1"
	db1.Begin()
	db1.Set("company", "company1")
	db1.Set("id", "id1")

	db1.Exec("create table if not exists foo(id integer not null primary key, name text)", []interface{}{})
	db1.Exec("insert into foo values (NULL, ?)", []interface{}{"teste1"})
	db1.Exec("insert into foo values (NULL, ?)", []interface{}{"teste2"})
	db1.Exec("insert into foo values (NULL, ?)", []interface{}{"teste3"})
	db1.Exec("insert into foo values (NULL, ?)", []interface{}{"teste4"})

	db1.Commit()

	db2, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db2.name = "DB2"
	db2.Begin()
	db2.Set("company", "company1")
	db2.Set("id", "id2")
	db2.Commit()

	err = db1.Sync()
	if err != nil {
		t.Fatal(err)
	}

	err = db2.Sync()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Second)

	db2.BeginForQuery()
	rows, _, err := db2.Query("select * from foo", []interface{}{})
	if err != nil {
		t.Error(err)
	}

	if len(rows) != 4 {
		t.Error("Wrong number of rows")
	}
	db2.Commit()
}

func TestGetIPS(t *testing.T) {
	st, err := getMyIPs()
	if err != nil {
	}

	if len(st) == 0 {
		t.Error("No ip valid")
	}
}

func TestGetAllUUIDs(t *testing.T) {
	db1, err := New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db1.Begin()
	db1.Set("company", "company1")
	db1.Set("id", "id1")
	db1.Commit()

	uuids, err := db1.getAllUUIDSLocal()
	if err != nil {
		t.Error(err)
	}

	if len(uuids) != 0 {
		t.Error("Wrong number of uuids")
	}
}
