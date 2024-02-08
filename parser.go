package main

import (
	"context"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type CreateSeoTextParams struct {
	Url       string   `json:"url"`
	Published bool     `json:"published"`
	Title     []string `json:"title"`
	Text      string   `json:"text"`
}

const createSeoText = `INSERT INTO seo_text_pages (url, titles, text) VALUES (@url, @titles, @text)`

func parseUrls(ctx context.Context, conn *pgxpool.Pool, urls []SitemapItem) {
	// todo поменять на 1000 или 5000
	chunkSize := 200

	fmt.Println(len(urls))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var items []CreateSeoTextParams

	// todo убрать потом
	//urls = urls[0:5000]

	wg.Add(len(urls))
	for i := 0; i < len(urls); i += chunkSize {

		fmt.Println(i)
		end := i + chunkSize

		if end > len(urls) {
			end = len(urls)
		}

		divided := urls[i:end]

		for _, value := range divided {
			go func(url SitemapItem) {
				defer wg.Done()
				response, err := http.Get(url.Url)

				if err != nil {
					fmt.Println("Error fetching URL:", err)
					return
				}

				doc, err := goquery.NewDocumentFromReader(response.Body)

				if err != nil {
					fmt.Println("Error parsing old HTML:", err)
					return
				}

				defer response.Body.Close()

				var params CreateSeoTextParams

				params.Url = url.Url
				params.Title = append(params.Title, strings.TrimSpace(doc.Find("section#bottom-text > h2").First().Text()))
				params.Text = strings.TrimSpace(doc.Find("section#bottom-text > p").First().Text())

				mu.Lock()
				items = append(items, params)
				mu.Unlock()
			}(value)
		}

		time.Sleep(time.Second * 10)
	}

	wg.Wait()

	fmt.Println(len(items))

	batch := &pgx.Batch{}

	for _, param := range items {
		args := pgx.NamedArgs{
			"url":    param.Url,
			"titles": param.Title,
			"text":   param.Text,
		}
		batch.Queue(createSeoText, args)
	}

	results := conn.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < batch.Len(); i++ {
		_, err := results.Exec()
		if err != nil {
			log.Fatal("unable to insert row: %w", err)
		}
	}

}

//func bulkInsertSeo(ctx context.Context, conn *pgxpool.Pool, paramsChan chan CreateSeoTextParams) error {
//	batch := &pgx.Batch{}
//
//	select {
//	case param := <-paramsChan:
//		args := pgx.NamedArgs{
//			"url":  param.Url,
//			"text": param.Text,
//		}
//		batch.Queue(query, args)
//	case <-quit:
//		results := conn.SendBatch(ctx, batch)
//		defer results.Close()
//
//		for i := 0; i < lenChan; i++ {
//			_, err := results.Exec()
//			if err != nil {
//				return fmt.Errorf("unable to insert row: %w", err)
//			}
//		}
//
//		return results.Close()
//	}
//}
