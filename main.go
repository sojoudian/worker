package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strconv"
)

type VoteData struct {
	VoterID string `json:"voter_id"`
	Vote    string `json:"vote"`
}

func main() {
	redisHost := getEnv("REDIS", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB := getEnv("REDIS_DB", "0")
	postgresHost := getEnv("POSTGRES", "localhost")
	postgresPort := getEnv("POSTGRES_PORT", "5432")
	postgresUser := getEnv("POSTGRES_USER", "postgres")
	postgresPassword := getEnv("POSTGRES_PASSWORD", "postgres")
	postgresDB := getEnv("POSTGRES_DB", "postgres")

	//ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPassword,
		DB:       getRedisDB(redisDB),
	})
	defer rdb.Close()

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}
	defer db.Close()

	log.Println("Worker started")

	for {
		vote, err := rdb.BLPop(0, "votes").Result()
		if err != nil {
			log.Printf("Error getting vote: %v", err)
			continue
		}

		log.Printf("Processing vote: %v", vote)

		var voteData VoteData
		err = json.Unmarshal([]byte(vote[1]), &voteData)
		if err != nil {
			log.Printf("Error unmarshalling vote: %v", err)
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			log.Printf("Error beginning transaction: %v", err)
			continue
		}

		_, err = tx.Exec("INSERT INTO votes (vote_id, voter_id, vote) VALUES (nextval('votes_sequence'), $1, $2)", voteData.VoterID, voteData.Vote)
		if err != nil {
			_ = tx.Rollback()
			log.Printf("Error inserting vote: %v", err)
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Printf("Error committing transaction: %v", err)
			continue
		}
	}
}

func getEnv(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getRedisDB(db string) int {
	dbInt, err := strconv.Atoi(db)
	if err != nil {
		dbInt = 0
	}
	return dbInt
}
