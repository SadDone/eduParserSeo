package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

type SitemapItem struct {
	ID  int64  `json:"id"`
	Url string `json:"url"`
}

func main() {
	start := time.Now()

	connStr := "postgresql://edu:edu@localhost:54320/edu?sslmode=disable"
	conn, err := pgxpool.New(context.Background(), connStr)

	if err != nil {
		log.Fatal("cannot connect to db:", err)
	}

	urls, err := getAllUrls(conn)

	if err != nil {
		log.Fatal(err)
	}

	parseUrls(context.Background(), conn, urls)

	fmt.Println(time.Since(start))
}

func getAllUrls(conn *pgxpool.Pool) ([]SitemapItem, error) {

	rows, err := conn.Query(
		context.Background(),
		"select * from sitemap where url like '%vuz%'",
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []SitemapItem
	for rows.Next() {
		var i SitemapItem
		if err := rows.Scan(
			&i.ID,
			&i.Url,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}

	if err != nil {
		return nil, err
	}

	return items, nil
}
