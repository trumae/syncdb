package syncdb

import (
	"errors"

	"github.com/satori/go.uuid"
)

var (
	//ErrKeyNotFound error when the key is not found on setting
	ErrKeyNotFound = errors.New("Error getting key in settting")
)

//Get value of the key from setting
func (db *SyncDB) Get(key string) (string, error) {
	res, _, err := db.Query("SELECT value FROM SETTINGS WHERE KEY = ?", []interface{}{key})
	if err != nil {
		return "", err
	}

	if len(res) == 0 {
		return "", ErrKeyNotFound
	}

	return *res[0][0].(*string), nil
}

//Set value to key into settings
func (db *SyncDB) Set(key, value string) error {
	err := db.ExecWithoutLog("DELETE FROM SETTINGS WHERE KEY = ?", []interface{}{key})
	if err != nil {
		return err
	}

	err = db.ExecWithoutLog("INSERT INTO SETTINGS(ID, KEY, VALUE) VALUES(null, ?,?)", []interface{}{key, value})
	if err != nil {
		return err
	}

	return nil
}

//GSet value to key into settings for all p2p network
func (db *SyncDB) GSet(key, value string) error {
	err := db.Exec("DELETE FROM SETTINGS WHERE KEY = ?", []interface{}{key})
	if err != nil {
		return err
	}

	err = db.Exec("INSERT INTO SETTINGS(ID, KEY, VALUE) VALUES(null, ?,?)", []interface{}{key, value})
	if err != nil {
		return err
	}

	return nil
}

func (db *SyncDB) initSettings() error {
	db.Begin()
	defer db.Commit()

	_, err := db.Get("id")
	if err != nil {
		id, err := uuid.NewV4()
		if err != nil {
			return err
		}
		db.Set("id", id.String())
	}
	return nil
}
