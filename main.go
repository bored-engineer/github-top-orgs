package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	ghauth "github.com/bored-engineer/github-auth-http-transport"
	"github.com/shurcooL/githubv4"
	"github.com/spf13/pflag"
	"go.uber.org/ratelimit"
)

// csvDateTime formats a githubv4.DateTime as a string for CSV output.
func csvDateTime(dt githubv4.DateTime) string {
	if dt.IsZero() {
		return ""
	}
	return dt.Time.Format(time.RFC3339)
}

// Organization is a struct that represents a GitHub organization.
type Organization struct {
	ArchivedAt  githubv4.DateTime
	CreatedAt   githubv4.DateTime
	DatabaseId  int64
	Description string
	Email       string
	Id          string
	IsVerified  bool
	Location    string
	Login       string
	Name        string
	UpdatedAt   githubv4.DateTime
	WebsiteUrl  string
}

// Search runs a GitHub search query using to retrieve a list of matching orgs.
func Search(
	ctx context.Context,
	client *githubv4.Client,
	query string,
	rl ratelimit.Limiter,
) (repos []Organization, _ error) {
	// Loop but with overlapping offsets to ensure we don't miss any results
	uniq := make(map[int64]struct{})
	for offset := 0; offset < 1000; offset += 91 {
	Retry:
		var cursor *githubv4.String
		if offset > 0 {
			cursor = githubv4.NewString(githubv4.String(
				base64.StdEncoding.EncodeToString(
					[]byte(fmt.Sprintf("cursor:%d", offset)),
				),
			))
		}
		var results struct {
			Search struct {
				Nodes []struct {
					Organization Organization `graphql:"... on Organization"`
				}
				PageInfo struct {
					HasNextPage bool
				}
			} `graphql:"search(query: $query, type: USER, first: 100, after: $cursor)"`
		}
		rl.Take()
		if err := client.Query(ctx, &results, map[string]any{
			"query":  githubv4.String(query),
			"cursor": cursor,
		}); err != nil {
			// We hit secondary rate limit errors sometimes, just wait a bit
			// We've also seen "something went wrong" before, retry those
			if strings.Contains(err.Error(), "You have exceeded a secondary rate limit") || strings.Contains(err.Error(), "Something went wrong while executing your query") || strings.Contains(err.Error(), "504 Gateway Timeout") {
				log.Printf("sleeping: %s", err.Error())
				time.Sleep(10 * time.Second)
				goto Retry
			}
			return nil, err
		}
		log.Printf("searching(%d): %s", offset, query)
		for _, node := range results.Search.Nodes {
			if _, ok := uniq[node.Organization.DatabaseId]; ok {
				continue // Skip duplicate entries
			}
			uniq[node.Organization.DatabaseId] = struct{}{}
			repos = append(repos, node.Organization)
		}
		if !results.Search.PageInfo.HasNextPage {
			break // No more pages, exit the loop early
		}
	}
	return repos, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	query := pflag.StringP("query", "q", "", "GitHub search query")
	start := pflag.StringP("start", "s", "", "Start date for filtering repositories (RFC3339 format)")
	end := pflag.StringP("end", "e", "", "End date for filtering repositories (RFC3339 format)")
	rate := pflag.IntP("rate", "r", 4900, "Rate limit for making requests per hour")
	pflag.Parse()
	if *query == "" || *start == "" || *end == "" {
		pflag.Usage()
		os.Exit(1)
	}

	startTime, err := time.ParseInLocation("2006-01-02", *start, time.UTC)
	if err != nil {
		log.Fatalf("time.ParseInLocation failed: %v", err)
	}
	endTime, err := time.ParseInLocation("2006-01-02", *end, time.UTC)
	if err != nil {
		log.Fatalf("time.ParseInLocation failed: %v", err)
	}
	if startTime.After(endTime) {
		log.Fatalf("start date %s is after end date %s", startTime, endTime)
	}

	transport, err := ghauth.Transport(ctx, nil)
	if err != nil {
		log.Fatalf("ghauth.Transport failed: %v", err)
	}
	client := githubv4.NewClient(&http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	})

	rl := ratelimit.New(*rate, ratelimit.Per(time.Hour))

	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()
	for day := startTime; !day.After(endTime); day = day.AddDate(0, 0, 1) {
		for hour := 0; hour < 24; hour++ {
			query := fmt.Sprintf("type:org %s created:%sT%02d:00:00Z..%sT%02d:59:59Z", *query, day.Format("2006-01-02"), hour, day.Format("2006-01-02"), hour)
			orgs, err := Search(ctx, client, query, rl)
			if err != nil {
				log.Fatalf("Search failed: %v", err)
			}
			for _, org := range orgs {
				if err := writer.Write([]string{
					org.Login,
					org.Name,
					strconv.FormatInt(org.DatabaseId, 10),
					org.Id,
					org.Description,
					org.Email,
					strconv.FormatBool(org.IsVerified),
					org.Location,
					org.WebsiteUrl,
					csvDateTime(org.CreatedAt),
					csvDateTime(org.UpdatedAt),
					csvDateTime(org.ArchivedAt),
				}); err != nil {
					log.Fatalf("(*csv.Writer).Write failed: %v", err)
				}
			}
		}
	}
	if err := writer.Error(); err != nil {
		log.Fatalf("(*csv.Writer).Flush failed: %v", err)
	}
}
