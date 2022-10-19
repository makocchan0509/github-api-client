package main

import (
	"cloud.google.com/go/datastore"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	project := os.Getenv("PROJECT_ID")

	url := "https://api.github.com/users/makocchan0509/events/public?per_page=80"
	header := map[string]string{
		"Accept":        "application/vnd.github+json",
		"Authorization": token,
	}
	httpClient, err := newHttpClient(url, "GET")
	if err != nil {
		log.Printf("http client error: %v\n", err)
		return
	}

	httpClient.setHeader(header)
	httpClient.SendRequest()

	var events []EventResp
	b, err := ioutil.ReadAll(httpClient.resp.Body)
	if err != nil {
		log.Printf("response parse error :%v\n", err)
		return
	}
	httpClient.Close()
	if err := json.Unmarshal(b, &events); err != nil {
		log.Printf("response json parse:%v\n", err)
		return
	}

	//log.Println(events)

	ctx := context.Background()
	dataStoreClient, err := newDataStoreClient(ctx, project)
	if err != nil {
		log.Printf("datastore client error:%v\n", err)
		return
	}
	defer dataStoreClient.close()

	for _, event := range events {

		record := ExtractedEvent{}
		ID, err := strconv.ParseInt(event.Id, 10, 64)
		if err != nil {
			log.Printf("string to int64 parse error :%v\n", err)
			return
		}
		dataStoreClient.generateIDKey("github-event", ID)

		err = dataStoreClient.get(ctx, record)
		if errors.Is(err, datastore.ErrNoSuchEntity) {

			record = ExtractedEvent{
				Type:         event.Type,
				DisplayLogin: event.Actor.DisplayLogin,
				RepoName:     event.Repo.Name,
				Commits:      event.Payload.Commits,
				Public:       event.Public,
				CreatedAt:    event.CreatedAt,
			}
			if err := dataStoreClient.put(ctx, record); err != nil {
				log.Printf("put record error datastore:%v\n", err)
				return
			}
		} else if err != nil {
			log.Printf("get record error from datastore:%v\n", err)
			return
		} else {
			log.Printf("getted record exists at datastore:%v\n", dataStoreClient.taskKey.ID)
		}

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
	fmt.Println("data store put", entity)
	if _, err := dc.client.Put(ctx, dc.taskKey, &entity); err != nil {
		log.Printf("Failed to save entity: %v\n", err)
		return err
	}
	return nil
}

func (dc *dataStoreClient) get(ctx context.Context, entity ExtractedEvent) error {
	fmt.Println("get key: ", dc.taskKey.ID)
	if err := dc.client.Get(ctx, dc.taskKey, &entity); err != nil {
		return err
	}
	fmt.Println("data store get result", entity)
	return nil
}

func (dc *dataStoreClient) close() {
	dc.client.Close()
}
