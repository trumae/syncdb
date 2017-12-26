package syncdb

import (
	"testing"
)

func TestSettingsSimplet(t *testing.T) {
	db, err := New(":memory:")
	//db, err := New("settings.db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Begin()
	if err != nil {
		t.Error(err)
	}

	_, err = db.Get("foo")
	if err == nil {
		t.Error(err)
	}

	err = db.Set("foo", "bar")
	if err != nil {
		t.Error(err)
	}

	val, err := db.Get("foo")
	if err != nil {
		t.Error(err)
	}

	if val != "bar" {
		t.Error("val value not expected")
	}

	err = db.Commit()
	if err != nil {
		t.Error(err)
	}
}
