package main

import (
	"cloud.google.com/go/datastore"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	//project := os.Getenv("PROJECT_ID")

	url := "https://api.github.com/users/makocchan0509/events?per_page=5"
	header := map[string]string{
		"Accept":        "application/vnd.github+json",
		"Authorization": token,
	}
	httpClient, err := newHttpClient(url, "GET")
	if err != nil {
		log.Printf("http client error: %v\n", err)
		return
	}
	defer httpClient.Close()

	httpClient.setHeader(header)
	httpClient.SendRequest()

	var event []EventResp
	b, err := ioutil.ReadAll(httpClient.resp.Body)
	if err != nil {
		log.Printf("response parse error :%v\n", err)
		return
	}
	if err := json.Unmarshal(b, &event); err != nil {
		log.Printf("response json parse:%v\n", err)
		return
	}

}

type HttpClient struct {
	req    *http.Request
	client *http.Client
	method string
	url    string
	resp   *http.Response
}

func newHttpClient(url string, method string) (*HttpClient, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return &HttpClient{}, err
	}
	client := new(http.Client)
	return &HttpClient{
		req:    req,
		client: client,
		method: method,
		url:    url,
	}, nil
}

func (h *HttpClient) setHeader(header map[string]string) {
	for k, v := range header {
		h.req.Header.Set(k, v)
	}
}

func (h *HttpClient) SendRequest() error {
	r, err := h.client.Do(h.req)
	if err != nil {
		return err
	}
	h.resp = r
	return nil
}

func (h *HttpClient) Close() {
	h.resp.Body.Close()
}

func (h *HttpClient) RespToString() (string, error) {
	b, err := ioutil.ReadAll(h.resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type EventResp struct {
	Id    string `json:"id"`
	Type  string `json:"type"`
	Actor struct {
		Id           int    `json:"id"`
		Login        string `json:"login"`
		DisplayLogin string `json:"display_login"`
		GravatarId   string `json:"gravatar_id"`
		Url          string `json:"url"`
		AvatarUrl    string `json:"avatar_url"`
	} `json:"actor"`
	Repo struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
		Url  string `json:"url"`
	} `json:"repo"`
	Payload struct {
		PushId       int64    `json:"push_id"`
		Size         int      `json:"size"`
		DistinctSize int      `json:"distinct_size"`
		Ref          string   `json:"ref"`
		Head         string   `json:"head"`
		Before       string   `json:"before"`
		Commits      []Commit `json:"commits"`
	} `json:"payload"`
	Public    bool      `json:"public"`
	CreatedAt time.Time `json:"created_at"`
}

type Commit struct {
	Sha    string `json:"sha"`
	Author struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	} `json:"author"`
	Message  string `json:"message"`
	Distinct bool   `json:"distinct"`
	Url      string `json:"url"`
}

type ExtractedEvent struct {
	Id           string    `json:"id"`
	Type         string    `json:"type"`
	DisplayLogin string    `json:"display_login"`
	RepoName     string    `json:"repo_name"`
	Commits      []Commit  `json:"commits"`
	Public       bool      `json:"public"`
	CreatedAt    time.Time `json:"created_at"`
}

type dataStoreClient struct {
	client  *datastore.Client
	taskKey *datastore.Key
}

func newDataStoreClient(ctx context.Context, project string) (dataStoreClient, error) {
	cli, err := datastore.NewClient(ctx, project)
	if err != nil {
		log.Printf("Failed to create datastore client: %v", err)
		return dataStoreClient{}, err
	}
	return dataStoreClient{
		client: cli,
	}, nil
}

func (dc *dataStoreClient) generateIDKey(kind string, id int64) {
	dc.taskKey = datastore.IDKey(kind, id, nil)
}

func (dc *dataStoreClient) put(ctx context.Context, entity ExtractedEvent) error {
	if _, err := dc.client.Put(ctx, dc.taskKey, &entity); err != nil {
		log.Printf("Failed to save entity: %v\n", err)
		return err
	}
	return nil
}

func (dc *dataStoreClient) close() {
	dc.client.Close()
}
