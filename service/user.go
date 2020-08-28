package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"gopkg.in/olivere/elastic.v6"
	"net/http"
	"reflect"
	"regexp"
	"time"
)

const TypeUser = "user"

var usernamePattern = regexp.MustCompile(`^[a-z0-9_]+$`).MatchString

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Age      int    `json:"age"`
	Gender   string `json:"gender"`
}

func checkUser(username string, password string) bool {
	esClient, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false
	}

	// search with a term query
	termQuery := elastic.NewTermQuery("username", username)
	queryResult, err := esClient.Search().Index(INDEX).Query(termQuery).Pretty(true).Do(context.Background())
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}

	for _, item := range queryResult.Each(reflect.TypeOf(User{})) {
		u := item.(User)
		return u.Password == password && u.Username == username
	}
	return false
}

func addUser(user User) bool {
	esClient, err := elastic.NewClient(elastic.SetURL(EsUrl), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false
	}

	// check if user already exist
	termQuery := elastic.NewTermQuery("username", user.Username)
	queryResult, err := esClient.Search().Index(INDEX).Query(termQuery).Pretty(true).Do(context.Background())
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}
	if queryResult.TotalHits() != 0 {
		fmt.Printf("Username %s already exists", user.Username)
		return false
	}

	_, err = esClient.Index().Index(INDEX).Type(TypeUser).Id(user.Username).BodyJson(user).Refresh("true").Do(context.Background())
	if err != nil {
		fmt.Printf("ES save user failed %v\n", err)
		return false
	}
	return true
}

func signupHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received an signup request")

	decoder := json.NewDecoder(r.Body)

	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}

	if u.Username != "" && u.Password != "" && usernamePattern(u.Username) {
		if addUser(u) {
			fmt.Println("User add successfully")
			_, _ = w.Write([]byte("User added successfully"))
		} else {
			fmt.Println("Failed to add user")
			http.Error(w, "Failed to add user", http.StatusInternalServerError)
		}
	} else {
		fmt.Println("Empty password or username")
		http.Error(w, "User added successfully", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one login request")

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
		return
	}

	if checkUser(u.Username, u.Password) {
		token := jwt.New(jwt.SigningMethodHS256)
		claims := token.Claims.(jwt.MapClaims)

		claims["username"] = u.Username
		claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

		tokenString, _ := token.SignedString(mySigningKey)

		_, _ = w.Write([]byte(tokenString))
	} else {
		fmt.Println("Invalid password or username")
		http.Error(w, "Invalid password or username", http.StatusForbidden)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}
