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

	filelog "ddb/lib/common/filelog"
	log "ddb/lib/common/log"
)

type MdsParameters struct {
	ApiAddress   string
	DebugAddress string
	LogFile      string
	PidFile      string
}

type Mds struct {
	apiServer     *http.Server
	debugServer   *http.Server
	signalChannel chan os.Signal
	errorChannel  chan error
	log           *log.Log
}

var globalMds Mds

func GetMds() *Mds {
	return &globalMds
}

type BaseRequest struct {
	RequestId string `json:"requestId"`
}

type CreateKeyRequest struct {
	BaseRequest
}

type SetKeyRequest struct {
	BaseRequest
	Value string `json:"value"`
}

type BaseResponse struct {
	RequestId string `json:"requestId"`
	Error     string `json:"error"`
}

type CreateKeyResponse struct {
	BaseResponse
	Id string `json:"id"`
}

type GetKeyResponse struct {
	BaseResponse
	Value string `json:"value"`
}

type StatKeyResponse struct {
	BaseResponse
	State string `json:"state"`
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
	case ErrNotFound:
		return http.StatusNotFound
	case ErrAlreadyExists:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func completeRequest(w http.ResponseWriter, requestId string, err error, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(errorToHttpStatus(err))
		err = json.NewEncoder(w).Encode(&BaseResponse{RequestId: requestId, Error: err.Error()})
		if err != nil {
			panic(fmt.Sprintf("encode error failed, error %v", err))
		}
	} else {
		w.WriteHeader(http.StatusOK)
		switch tv := v.(type) {
		case *StatKeyResponse:
			resp := v.(*StatKeyResponse)
			resp.Error = ""
			resp.RequestId = requestId
		case *GetKeyResponse:
			resp := v.(*GetKeyResponse)
			resp.Error = ""
			resp.RequestId = requestId
		case *CreateKeyResponse:
			resp := v.(*CreateKeyResponse)
			resp.Error = ""
			resp.RequestId = requestId
		case *BaseResponse:
			resp := v.(*BaseResponse)
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

func createKey(w http.ResponseWriter, r *http.Request) {
	var err error
	var key *Key
	req := &CreateKeyRequest{}
	resp := &CreateKeyResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
	}()

	err = decodeJson(w, r, req)

	key, err = GetMds().createKey()
	if err != nil {
		return
	}

	resp.Id = key.getId()
	return
}

func setKey(w http.ResponseWriter, r *http.Request) {
	var err error

	req := &SetKeyRequest{}
	resp := &BaseResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
	}()

	vars := mux.Vars(r)
	id, ok := vars["id"]
	if !ok {
		err = ErrBadRequest
		return
	}

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	err = GetMds().setKey(id, req.Value)
	if err != nil {
		return
	}

	return
}

func deleteKey(w http.ResponseWriter, r *http.Request) {
	var err error

	req := &BaseRequest{}
	resp := &BaseResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
	}()

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	vars := mux.Vars(r)
	id, ok := vars["id"]
	if !ok {
		err = ErrBadRequest
		return
	}

	err = GetMds().deleteKey(id)
	return
}

func statKey(w http.ResponseWriter, r *http.Request) {
	var err error
	var key *Key

	req := &BaseRequest{}
	resp := &StatKeyResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
	}()

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	vars := mux.Vars(r)
	id, ok := vars["id"]
	if !ok {
		err = ErrBadRequest
		return
	}

	key, err = GetMds().lookupKey(id)
	if err != nil {
		return
	}

	resp.State = key.getState()
	return
}

func getKey(w http.ResponseWriter, r *http.Request) {
	var err error
	var key *Key

	req := &BaseRequest{}
	resp := &StatKeyResponse{}
	defer func() {
		completeRequest(w, req.RequestId, err, resp)
	}()

	err = decodeJson(w, r, req)
	if err != nil {
		return
	}

	vars := mux.Vars(r)
	id, ok := vars["id"]
	if !ok {
		err = ErrBadRequest
		return
	}

	key, err = GetMds().lookupKey(id)
	if err != nil {
		return
	}

	resp.State = key.getValue()
	return
}

func (mds *Mds) shutdown() {
	mds.log.Pf(0, "shutdowning")
	mds.apiServer.Shutdown(context.Background())
	mds.debugServer.Shutdown(context.Background())
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

	if params.PidFile != "" {
		f, err := os.OpenFile(params.PidFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = f.WriteString(strconv.Itoa(os.Getpid()))
		if err != nil {
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
	r.HandleFunc("/key/create", createKey).Methods("POST").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/key/{id}/set", setKey).Methods("POST").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/key/{id}/get", getKey).Methods("GET").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/key/{id}/delete", deleteKey).Methods("POST").HeadersRegexp("Content-Type", "application/json")
	r.HandleFunc("/key/{id}/state", statKey).Methods("GET").HeadersRegexp("Content-Type", "application/json")

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
