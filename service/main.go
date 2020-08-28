package main

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"gopkg.in/olivere/elastic.v6"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// in go, only variable starts with upper case will be exposed to outside
// cannot do below
//	type Location struct {
//		lat float64
//		lon float64
//	}
// so need `json:"lat"` to translate go type to normal json type (lower case)
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
	Type     string   `json:"type"`
	Face     bool     `json:"face"`
}

const (
	INDEX            = "around"
	TYPE             = "post"
	DISTANCE         = "200km"
	EsUrl            = "http://35.193.149.251:9200"
	BucketName       = "post-images-286400"
	ProjectId        = "orbital-heaven-286400"
	BigtableInstance = "around-post"
	API_PREFIX       = "/api/v1"
)

var (
	mediaType = map[string]string{
		".jpeg": "image",
		".jpg":  "image",
		".gif":  "image",
		".png":  "image",
		".mov":  "video",
		".mp4":  "video",
		".avi":  "video",
		".flv":  "video",
		".wmv":  "video",
	}
)

var mySigningKey = []byte("secret") // secret key for JWT, please use more complicated key in production

func main() {
	// wait for network connection
	waitForConnection()

	// create client
	client, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// use the IndexExists service to check if a specified index exists
	// if index doesn't exists, create an index with mapping
	exists, err := client.IndexExists(INDEX).Do(context.Background())

	if err != nil {
		panic(err)
	}
	if !exists {
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type": "geo_point"
						}
					}
				}
			}
		}`
		// index (database) is around; type (table) is post
		_, err := client.CreateIndex(INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("started service")

	r := mux.NewRouter()

	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	// register the url pattern and http handler function
	r.Handle(API_PREFIX+"/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle(API_PREFIX+"/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle(API_PREFIX+"/search-face", jwtMiddleware.Handler(http.HandlerFunc(handlerSearchFace))).Methods("GET")
	r.Handle(API_PREFIX+"/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle(API_PREFIX+"/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	//backend endpoint
	http.Handle(API_PREFIX+"/", r)

	//frontend endpoint
	http.Handle("/", http.FileServer(http.Dir("build")))

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func waitForConnection() {
	fmt.Println("Waiting for ES connection")
	if _, err := http.Get(EsUrl); err != nil {
		trial := 0
		for err != nil {
			if trial >= 5 {
				panic("Failed connecting to ES server, please double check")
			}
			time.Sleep(5 * time.Second)
			fmt.Println("Waiting for ES connection")
			_, err = http.Get(EsUrl)
			trial++
		}
	}
}

// need to user multipart form to handle attachment (image)
func handlerPost(w http.ResponseWriter, r *http.Request) {
	// set header
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	// set form max size to 32M
	_ = r.ParseMultipartForm(32 << 20)

	fmt.Printf("Received one post request %s\n", r.FormValue("message"))

	// get current user info
	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	// get info from request form
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	// handle the upload image
	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}
	defer file.Close()

	// figure out the file type
	fileExt := filepath.Ext(header.Filename)

	if t, ok := mediaType[fileExt]; ok {
		p.Type = t
	} else {
		p.Type = "unknown"
	}

	// save to GCS
	ctx := context.Background()
	_, attrs, err := saveToGCS(ctx, file, BucketName, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}

	p.Url = attrs.MediaLink

	// GCP Vision API
	if faceDetected, err := detectFacesURI(attrs.MediaLink); err != nil {
		http.Error(w, "Failed to annotate the image", http.StatusInternalServerError)
		fmt.Printf("Failed to annotate the image %v\n", err)
		panic(err)
	} else {
		p.Face = faceDetected
	}

	// save to ES
	saveToES(p, id)

	// save to BigTable
	saveToBigTable(ctx, p, id)
}

// save to BigTable
func saveToBigTable(ctx context.Context, p *Post, id string) {
	btClient, err := bigtable.NewClient(ctx, ProjectId, BigtableInstance)
	if err != nil {
		panic(err)
		return
	}

	tbl := btClient.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()

	mut.Set("post", "user", t, []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	if err = tbl.Apply(ctx, id, mut); err != nil {
		panic(err)
		return
	}

	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)

}

// save to Google cloud storage
func saveToGCS(ctx context.Context, file multipart.File, name string, id string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// create connection to GCS
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	// link to bucket
	bucket := client.Bucket(name)
	// check if bucket exists- check if the metadata exists
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(id)
	wc := obj.NewWriter(ctx)

	if _, err = io.Copy(wc, file); err != nil {
		return nil, nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, nil, err
	}
	// set uploaded file access right
	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	// get uploaded file's url and pass to user
	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)

	return obj, attrs, err

}

// save to elasticsreach
func saveToES(p *Post, id string) {
	esClient, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	_, err = esClient.Index().Index(INDEX).Type(TYPE).Id(id).BodyJson(p).Refresh("true").Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	fmt.Println("Received one request for search")
	// get parameter from URL
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Printf("Search received: %f, %f %s\n", lat, lon, ran)

	// built connection to elastic service
	client, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// query name- can choose any name, here we use location
	// query all results in the range
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().Index(INDEX).Query(q).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts\n", searchResult.TotalHits())

	// convert ES query results to our defined structure- Post
	var ps []Post
	// reflect to tell elastic the type of data- here is 'Post'
	for _, item := range searchResult.Each(reflect.TypeOf(Post{})) {
		p := item.(Post) // golang type assertions- assert item to Post (our structure)
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	// use json package convert slices of Post to json format
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	// write results
	_, _ = w.Write(js)
}

func handlerSearchFace(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	fmt.Println("Received one request for search face")
	// get parameter from URL
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Printf("Search received: %f, %f %s\n", lat, lon, ran)

	// built connection to elastic service
	client, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// query name- can choose any name, here we use location
	// query all results in the range
	q := elastic.NewMatchQuery("face", "true")

	searchResult, err := client.Search().Index(INDEX).Query(q).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts\n", searchResult.TotalHits())

	// convert ES query results to our defined structure- Post
	var ps []Post
	// reflect to tell elastic the type of data- here is 'Post'
	for _, item := range searchResult.Each(reflect.TypeOf(Post{})) {
		p := item.(Post) // golang type assertions- assert item to Post (our structure)
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	// use json package convert slices of Post to json format
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	// write results
	_, _ = w.Write(js)

}

func containsFilteredWords(s *string) bool {

	FilteredWords := []string{
		"fuck",
		"shit",
	}

	for _, word := range FilteredWords {
		if strings.Contains(*s, word) {
			return true
		}
	}
	return false
}
