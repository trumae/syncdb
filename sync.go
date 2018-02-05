package syncdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	rum "github.com/rumlang/rum/runtime"
)

var (
	//ErrIDNotFound error when the key is not found on setting
	ErrIDNotFound = errors.New("Error getting ID from txlogs tables")

	//URLDiscoverService is where the sync system get info about the nodes on companies
	URLDiscoverService = "https://piscine-monsieur-96181.herokuapp.com"

	RumContext *rum.Context
)

type logReg struct {
	ID  string
	Seq string
	SQL string
}

type txReg struct {
	ID         string
	TxDatetime string
	SQLs       []logReg
}

type msgDiff struct {
	IHas  []txReg
	IWant []string
}

//NodeInfo is info about node
type NodeInfo struct {
	IP   string
	Port string
	Rum  string
}

func handleGetAllUUIDs(w http.ResponseWriter, r *http.Request) {
	db := r.Context().Value(keyDB).(*SyncDB)

	uuids, err := db.getAllUUIDSLocal()
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	b, err := json.Marshal(uuids)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// A very simple health check.
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func handleDiffs(w http.ResponseWriter, r *http.Request) {
	db := r.Context().Value(keyDB).(*SyncDB)

	//Get Message
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "[]")
		return
	}
	msg := msgDiff{}

	err = json.Unmarshal(body, &msg)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "[]")
	}

	//process received txs
	db.syncRegister(msg.IHas)

	//Get requested content
	ihas, err := db.uuids2txRegs(msg.IWant)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "[]")
	}

	b, err := json.Marshal(ihas)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "[]")
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	w.Write(b)
}

func getMyIPs() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var ips []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
			}
		}
	}

	return ips, nil
}

func discoverNodes(ip []string, company, port, id string) (map[string]NodeInfo, error) {
	res, err := http.Get(URLDiscoverService + "/?i=" + strings.Join(ip, ",") +
		"&c=" + company + "&p=" + port + "&id=" + id)
	if err != nil {
		return nil, err
	}

	text, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	nodes := map[string]NodeInfo{}
	err = json.Unmarshal(text, &nodes)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

//Sync initialize sync procedure from db node
func (db *SyncDB) Sync() error {
	log.Println("Init Sync")
	ips, err := getMyIPs()
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("*** Getting sync info")
	//Get info
	err = db.Begin()
	if err != nil {
		log.Println(err)
		return err
	}
	company, err := db.Get("company")
	if err != nil {
		log.Println(err)
		db.Rollback()
		return err
	}
	id, err := db.Get("id")
	if err != nil {
		log.Println(err)
		db.Rollback()
		return err
	}
	err = db.Rollback()
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("*** info", company, id)

	//discover nodes
	log.Println("Discovering nodes")
	nodes, err := discoverNodes(ips, company, strconv.Itoa(db.port), id)
	if err != nil {
		log.Println(err)
		return err
	}

	for key, val := range nodes {
		if key != id {
			rips := strings.Split(val.IP, ",")
			for _, ip := range rips {
				if ip != "127.0.0.1" {
					log.Println("Sync with node", ip, val.Port)
					err = db.syncWithNode(ip, val.Port)
					if err != nil {
						log.Println(err)
					}
				}
			}
		} else {
			if RumContext != nil {
				//process scripts
				if len(val.Rum) > 0 {
					_, err := rumEval(val.Rum, RumContext)
					if err != nil {
						log.Println("RUM:", err)
					}
				}
			}
		}
	}

	return nil
}

func (db *SyncDB) uuid2txReg(uuid string) (txReg, error) {
	db.BeginForQuery()
	defer db.Commit()

	txReg := txReg{}
	res, _, err := db.Query("select id, datetime from __DBTX__ where id=? order by datetime", []interface{}{uuid})
	if err != nil {
		return txReg, err
	}
	if len(res) != 1 {
		return txReg, ErrIDNotFound
	}

	txReg.ID = *res[0][0].(*string)
	txReg.TxDatetime = *res[0][1].(*string)

	res, _, err = db.Query("select id, seq, sql from __DBLOG__ where txid=? order by seq", []interface{}{uuid})
	if err != nil {
		return txReg, err
	}

	for i := 0; i < len(res); i++ {
		entry := logReg{}

		entry.ID = *res[i][0].(*string)
		entry.Seq = *res[i][1].(*string)
		entry.SQL = *res[i][2].(*string)
		txReg.SQLs = append(txReg.SQLs, entry)
	}

	return txReg, nil
}

func (db *SyncDB) uuids2txRegs(uuids []string) ([]txReg, error) {
	ret := []txReg{}
	for _, val := range uuids {
		txreg, err := db.uuid2txReg(val)
		if err != nil {
			return nil, err
		}
		ret = append(ret, txreg)
	}
	return ret, nil
}

func (db *SyncDB) syncWithNode(ip, port string) error {
	//get remote uuids
	ruuids, err := getAllUUIDSFromNode(ip, port)
	if err != nil {
		return err
	}

	//get local uuids
	luuids, err := db.getAllUUIDSLocal()
	if err != nil {
		return err
	}

	onlyRemote := uuidsDiff(ruuids, luuids)
	onlyLocal := uuidsDiff(luuids, ruuids)

	ihas, err := db.uuids2txRegs(onlyLocal)
	if err != nil {
		return err
	}

	msg := msgDiff{
		IHas:  ihas,
		IWant: onlyRemote}

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	txs, err := sendReceiveTXS(ip, port, b)
	if err != nil {
		return err
	}

	//process received txs
	db.syncRegister(txs)

	return nil
}

func containsUUID(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func uuidsDiff(uuids1, uuids2 []string) (res []string) {
	for _, a := range uuids1 {
		if !containsUUID(uuids2, a) {
			res = append(res, a)
		}
	}
	return
}

func (db *SyncDB) getAllUUIDSLocal() ([]string, error) {
	db.BeginForQuery()
	defer db.Commit()

	res, _, err := db.Query("SELECT ID FROM __DBTX__ ORDER BY DATETIME ", []interface{}{})
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, val := range res {
		ret = append(ret, *val[0].(*string))
	}

	return ret, nil
}

func getAllUUIDSFromNode(ip, port string) ([]string, error) {
	res, err := http.Get("http://" + ip + ":" + port + "/txs")
	if err != nil {
		return nil, err
	}

	text, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	uuids := []string{}
	err = json.Unmarshal(text, &uuids)
	if err != nil {
		return nil, err
	}

	return uuids, nil
}

func sendReceiveTXS(ip, port string, txs []byte) ([]txReg, error) {
	r := bytes.NewReader(txs)
	res, err := http.Post("http://"+ip+":"+port+"/diffs", "application/json; charset=utf-8", r)
	if err != nil {
		return nil, err
	}

	text, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	regs := []txReg{}
	err = json.Unmarshal(text, &regs)
	if err != nil {
		return nil, err
	}

	return regs, nil

}

func (db *SyncDB) syncRegister(txs []txReg) error {
	for _, tx := range txs {
		func() {
			log.Println("---->", tx.ID, tx.TxDatetime)
			err := db.beginWithIDAndDatetime(tx.ID, tx.TxDatetime)
			if err != nil {
				log.Println("ERROR in syncregister BEGINTRANS", tx.ID,
					tx.TxDatetime, err)
			}
			defer db.Commit()

			for _, tsql := range tx.SQLs {
				sql := SQLreg{}

				err := json.Unmarshal([]byte(tsql.SQL), &sql)
				if err != nil {
					return
				}
				err = db.Exec(sql.SQL, sql.Params)
				if err != nil {
					log.Println("ERROR in syncregister", sql.SQL,
						sql.Params, tx.ID, tx.TxDatetime, err)
				}
			}
		}()
	}
	return nil
}
