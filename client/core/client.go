package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	uuid "github.com/pborman/uuid"
)

var (
	ErrNotFound   = fmt.Errorf("Not found")
	ErrConflict   = fmt.Errorf("Conflict")
	ErrBadRequest = fmt.Errorf("Bad request")
	ErrInternal   = fmt.Errorf("Internal error")
	ErrUnknown    = fmt.Errorf("Unknown error")
	ErrEmptyKey   = fmt.Errorf("Empty key")
	ErrEmptyValue = fmt.Errorf("Empty value")
)

type BaseRequest struct {
	RequestId string `json:"requestId"`
}

type SetKeyRequest struct {
	BaseRequest
	Value string `json:"value"`
}

type BaseResponse struct {
	RequestId string `json:"requestId"`
	Error     string `json:"error"`
}

type GetKeyResponse struct {
	BaseResponse
	Value string `json:"value"`
}

type Client struct {
	endpoint   string
	httpClient *http.Client
}

func httpStatusToError(status int) error {
	switch status {
	case http.StatusInternalServerError:
		return ErrInternal
	case http.StatusBadRequest:
		return ErrBadRequest
	case http.StatusConflict:
		return ErrConflict
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusOK:
		return nil
	default:
		return ErrUnknown
	}
}

func NewClient(endpoint string) *Client {
	c := &Client{endpoint: endpoint,
		httpClient: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
			MaxIdleConnsPerHost: 10,
			DisableKeepAlives:   true,
		}}}

	return c
}

func (c *Client) newRequestId() string {
	return uuid.New()
}

func (c *Client) GetKey(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	var req BaseRequest
	req.RequestId = c.newRequestId()

	reqBody, err := json.Marshal(&req)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequest("GET", c.endpoint+"/get/"+key, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	err = httpStatusToError(httpResp.StatusCode)
	if err != nil {
		return "", err
	}

	var resp GetKeyResponse
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

func (c *Client) SetKey(key string, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	if value == "" {
		return ErrEmptyValue
	}

	var req SetKeyRequest
	req.RequestId = c.newRequestId()
	req.Value = value

	reqBody, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	httpResp, err := c.httpClient.Post(c.endpoint+"/set/"+key, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	err = httpStatusToError(httpResp.StatusCode)
	if err != nil {
		return err
	}

	var resp BaseResponse
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) DeleteKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	var req BaseRequest
	req.RequestId = c.newRequestId()

	reqBody, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	httpResp, err := c.httpClient.Post(c.endpoint+"/delete/"+key, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	err = httpStatusToError(httpResp.StatusCode)
	if err != nil {
		return err
	}

	var resp BaseResponse
	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	if err != nil {
		return err
	}

	return nil
}
