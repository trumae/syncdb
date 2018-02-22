package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/dimiro1/banner"
	colorable "github.com/mattn/go-colorable"
	"github.com/trumae/syncdb"
)

const textBanner = `
 ____                   ____  ____  
/ ___| _   _ _ __   ___|  _ \| __ ) 
\___ \| | | | '_ \ / __| | | |  _ \ 
 ___) | |_| | | | | (__| |_| | |_) |
|____/ \__, |_| |_|\___|____/|____/ 
       |___/      

GoVersion: {{ .GoVersion }}
GOOS: {{ .GOOS }}
`

var (
	DB   *syncdb.SyncDB
	inTx bool
)

func main() {
	filedb := "store.db"
	flag.StringVar(&filedb, "db", "store.db", "database path")
	flag.Parse()

	isEnabled := true
	isColorEnabled := true
	banner.Init(colorable.NewColorableStdout(), isEnabled,
		isColorEnabled, bytes.NewBufferString(textBanner))

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 "> ",
		HistoryFile:            "/tmp/.syncdb-history",
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	DB, err = syncdb.New(filedb)
	if err != nil {
		log.Fatal(err)
	}

	var cmds []string
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cmds = append(cmds, line)
		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt("... ")
			continue
		}
		cmd := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt("> ")
		rl.SaveHistory(cmd)
		fmt.Println(cmd)
		fmt.Println(processCmd(cmd))
	}
}

func help() string {
	return `quit                           Exit this program
exit                           Exit this program
get <key>                      read a key an setting
set <key> <val>                write a key/value an settings
`
}

func processCmd(cmd string) string {
	fcmd := strings.TrimSpace(cmd[:len(cmd)-1])
	upcmd := strings.ToUpper(fcmd)
	switch {
	case strings.HasPrefix(upcmd, "QUIT") || strings.HasPrefix(upcmd, "EXIT"):
		os.Exit(0)

	case strings.HasPrefix(upcmd, "HELP"):
		return help()

	case strings.HasPrefix(upcmd, "SET"):
		params := strings.Split(fcmd, " ")
		if len(params) != 3 {
			return "usage: set <key> <val>;"
		}

		if !inTx {
			DB.Begin()
			defer DB.Commit()
		}

		key := params[1]
		val := params[2]
		err := DB.Set(key, val)
		if err != nil {
			return "Error write setting " + err.Error()
		}

		return key + " = " + val

	case strings.HasPrefix(upcmd, "GET"):
		params := strings.Split(fcmd, " ")
		log.Println(params, len(params))
		if len(params) != 2 {
			return "usage: get <key>;"
		}

		if !inTx {
			DB.Begin()
			defer DB.Commit()
		}

		key := params[1]
		val, err := DB.Get(key)
		if err != nil {
			return "Error read setting " + err.Error()
		}

		return key + " = " + val

	case strings.HasPrefix(upcmd, "SYNC"):
		err := DB.Sync()
		if err != nil {
			return "Error in sync " + err.Error()
		}
		return "Done"

	case strings.HasPrefix(upcmd, "BEGIN"):
		if !inTx {
			DB.Begin()
			inTx = true
		} else {
			return "Just in a transaction"
		}

		return "BEGIN"

	case strings.HasPrefix(upcmd, "COMMIT"):
		if inTx {
			DB.Commit()
			inTx = false
		} else {
			return "Not in a transaction"
		}
		return "COMMIT"

	case strings.HasPrefix(upcmd, "ROLLBACK"):
		if inTx {
			DB.Rollback()
			inTx = false
		} else {
			return "Not in a transaction"
		}
		return "ROLLBACK"

	case strings.HasPrefix(upcmd, "SELECT"):
		if !inTx {
			DB.Begin()
			defer DB.Commit()
		}
		rows, cols, err := DB.Query(cmd, []interface{}{})
		if err != nil {
			return "Error in query " + err.Error()
		}

		ret := strings.Join(cols, "| ")
		for _, row := range rows {
			ret += "\n|"
			for _, cell := range row {
				ret += *cell.(*string) + " | "
			}
		}
		return ret

	default:
		return "Command not found"
	}
	return ""
}
