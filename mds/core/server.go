package mds

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	client "ddb/client/core"
	filelog "ddb/lib/common/filelog"
	log "ddb/lib/common/log"
	"ddb/lib/common/lsm"
	"ddb/lib/common/sequence"
)

type KeyValueStorage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	Close()
}

type MdsParameters struct {
	ApiAddress   string
	DebugAddress string
	LogFile      string
	PidFile      string
	StoragePath  string
}

type Stats struct {
	getKey    *sequence.Sequence
	setKey    *sequence.Sequence
	deleteKey *sequence.Sequence
}

type Mds struct {
	apiServer     *http.Server
	debugServer   *http.Server
	signalChannel chan os.Signal
	errorChannel  chan error
	log           *log.Log
	kvs           KeyValueStorage
	stats         Stats
}

var globalMds Mds

func GetMds() *Mds {
	return &globalMds
}

func decodeJson(w http.ResponseWriter, r *http.Request, v interface{}) error {
	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		GetMds().log.Pf(0, "json parse error %v")
		return err
	}
	return nil
}

func errorToHttpStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}

	switch err {
	case ErrBadRequest:
		return http.StatusBadRequest
	case ErrNotFound, lsm.ErrNotFound:
		return http.StatusNotFound
	case ErrAlreadyExists:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func completeRequest(w http.ResponseWriter, requestId string, err error, v interface{}) {
	GetMds().log.Pf(0, "request %s complete error %v", requestId, err)

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(errorToHttpStatus(err))
		err = json.NewEncoder(w).Encode(&client.BaseResponse{RequestId: requestId, Error: err.Error()})
		if err != nil {
			panic(fmt.Sprintf("encode error failed, error %v", err))
		}
	} else {
		w.WriteHeader(http.StatusOK)
		switch tv := v.(type) {
		case *client.GetKeyResponse:
			resp := v.(*client.GetKeyResponse)
			resp.Error = ""
			resp.RequestId = requestId
		case *client.BaseResponse:
			resp := v.(*client.BaseResponse)
			resp.Error = ""
			resp.RequestId = requestId
		default:
			panic(fmt.Sprintf("unknown type %v", tv))
		}

		err = json.NewEncoder(w).Encode(v)
		if err != nil {
			panic(fmt.Sprintf("encode error failed, error %v", err))
		}
	}
}

func setKey(w http.ResponseWriter, r *http.Request) {
	timeStart := time.Now()

	var err error

	req := &client.SetKeyRequest{}
	resp := &client.BaseResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
		GetMds().stats.setKey.Append(time.Since(timeStart).Seconds())
	}()

	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		err = ErrBadRequest
		return
	}

	GetMds().log.Pf(0, "request %s", req.RequestId)

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	if key == "" || req.Value == "" {
		err = ErrBadRequest
		return
	}

	err = GetMds().kvs.Set(key, req.Value)
	if err != nil {
		return
	}

	return
}

func deleteKey(w http.ResponseWriter, r *http.Request) {
	timeStart := time.Now()

	var err error

	req := &client.BaseRequest{}
	resp := &client.BaseResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
		GetMds().stats.deleteKey.Append(time.Since(timeStart).Seconds())
	}()

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	GetMds().log.Pf(0, "request %s", req.RequestId)

	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		err = ErrBadRequest
		return
	}

	if key == "" {
		err = ErrBadRequest
		return
	}

	err = GetMds().kvs.Delete(key)
	return
}

func getKey(w http.ResponseWriter, r *http.Request) {
	timeStart := time.Now()
	var err error

	req := &client.BaseRequest{}
	resp := &client.GetKeyResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
		GetMds().stats.getKey.Append(time.Since(timeStart).Seconds())
	}()

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	GetMds().log.Pf(0, "request %s", req.RequestId)

	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		err = ErrBadRequest
		return
	}

	if key == "" {
		err = ErrBadRequest
		return
	}

	resp.Value, err = GetMds().kvs.Get(key)
	if err != nil {
		return
	}

	return
}

func getStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	stats := &GetMds().stats

	fmt.Fprintf(w, "setKey count %d avg %f 50p %f 95p %f 99p %f\n",
		stats.setKey.Count(), stats.setKey.GetAverage(), stats.setKey.Get50P(), stats.setKey.Get95P(), stats.setKey.Get99P())
	fmt.Fprintf(w, "getKey count %d avg %f 50p %f 95p %f 99p %f\n",
		stats.getKey.Count(), stats.getKey.GetAverage(), stats.getKey.Get50P(), stats.getKey.Get95P(), stats.getKey.Get99P())
	fmt.Fprintf(w, "deleteKey count %d avg %f 50p %f 95p %f 99p %f\n",
		stats.getKey.Count(), stats.deleteKey.GetAverage(), stats.deleteKey.Get50P(), stats.deleteKey.Get95P(), stats.deleteKey.Get99P())
}

func (mds *Mds) shutdown() {
	mds.log.Pf(0, "shutdowning")
	mds.apiServer.Shutdown(context.Background())
	mds.debugServer.Shutdown(context.Background())
	mds.kvs.Close()
	mds.log.Pf(0, "shutdown")
	mds.log.Shutdown()
}

func (mds *Mds) apiLoop() {
	mds.log.Pf(0, "running api server")
	err := mds.apiServer.ListenAndServe()
	if err != nil {
		mds.log.Pf(0, "run api server error %v", err)
		mds.errorChannel <- err
	}
}

func (mds *Mds) debugLoop() {
	mds.log.Pf(0, "running debug server")
	err := mds.debugServer.ListenAndServe()
	if err != nil {
		mds.log.Pf(0, "run debug server error %v", err)
		mds.errorChannel <- err
	}
}

func (mds *Mds) eventLoop() error {
	mds.log.Pf(0, "running event loop")
	for {
		select {
		case <-mds.signalChannel:
			mds.log.Pf(0, "received signal")
			mds.shutdown()
			return nil
		case <-mds.errorChannel:
			mds.log.Pf(0, "received error")
			mds.shutdown()
			return nil
		}
	}
}

func (mds *Mds) Run(params *MdsParameters) error {
	filelog, err := filelog.NewFileLog(params.LogFile)
	if err != nil {
		return err
	}

	mds.log = log.NewLog(filelog)
	kvs, err := lsm.OpenLsm(mds.log, params.StoragePath)
	if err != nil {
		kvs, err = lsm.NewLsm(mds.log, params.StoragePath)
		if err != nil {
			mds.log.Shutdown()
			return err
		}
	}
	mds.kvs = kvs

	mds.stats.setKey = sequence.NewSequence()
	mds.stats.getKey = sequence.NewSequence()
	mds.stats.deleteKey = sequence.NewSequence()

	if params.PidFile != "" {
		f, err := os.OpenFile(params.PidFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			mds.kvs.Close()
			mds.log.Shutdown()
			return err
		}
		defer f.Close()

		_, err = f.WriteString(strconv.Itoa(os.Getpid()))
		if err != nil {
			mds.kvs.Close()
			mds.log.Shutdown()
			return err
		}
	}

	dr := mux.NewRouter()
	dr.HandleFunc("/debug/pprof/", pprof.Index)
	dr.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	dr.HandleFunc("/debug/pprof/profile", pprof.Profile)
	dr.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	dr.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	dr.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	dr.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	dr.Handle("/debug/pprof/block", pprof.Handler("block"))

	r := mux.NewRouter()
	r.HandleFunc("/set/{key}", setKey).Methods("POST").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/get/{key}", getKey).Methods("GET").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/delete/{key}", deleteKey).Methods("POST").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/stats", getStats).Methods("GET")

	mds.debugServer = &http.Server{
		Handler:      dr,
		Addr:         params.DebugAddress,
		WriteTimeout: 120 * time.Second,
		ReadTimeout:  120 * time.Second,
	}

	mds.apiServer = &http.Server{
		Handler:      r,
		Addr:         params.ApiAddress,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	mds.signalChannel = make(chan os.Signal, 1)
	mds.errorChannel = make(chan error, 1)
	signal.Notify(mds.signalChannel, syscall.SIGINT, syscall.SIGTERM)

	go mds.apiLoop()
	go mds.debugLoop()
	return mds.eventLoop()
}
