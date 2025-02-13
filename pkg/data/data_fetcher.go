// pkg/data/data_fetcher.go
package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	pb "github.com/yourusername/nbastream/pkg/proto"
	"github.com/yourusername/nbastream/pkg/subscription"
)

// APIResponse represents the structure of the balldontlie API response.
type APIResponse struct {
	Data []Game `json:"data"`
}

// Game represents a game from the API.
type Game struct {
	ID          int    `json:"id"`
	HomeTeam    Team   `json:"home_team"`
	VisitorTeam Team   `json:"visitor_team"`
	Status      string `json:"status"`
}

// Team holds team details.
type Team struct {
	FullName string `json:"full_name"`
}

// FetchAndDispatch fetches NBA data and dispatches updates to subscriptions.
func FetchAndDispatch(manager *subscription.Manager) error {
	// Use the current date (format: YYYY-MM-DD)
	currentDate := time.Now().Format("2006-01-02")
	url := fmt.Sprintf("https://www.balldontlie.io/api/v1/games?dates[]=%s", currentDate)
	log.Printf("Fetching data from: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		notifyError(manager, "Error fetching data from API: "+err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		notifyError(manager, fmt.Sprintf("API returned non-OK status: %s", resp.Status))
		return fmt.Errorf("non-OK API status: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		notifyError(manager, "Error reading API response: "+err.Error())
		return err
	}

	var apiResp APIResponse
	err = json.Unmarshal(body, &apiResp)
	if err != nil {
		notifyError(manager, "Error parsing API response: "+err.Error())
		return err
	}

	// Create an update for each game.
	for _, game := range apiResp.Data {
		updateMessage := fmt.Sprintf("Game %d: %s vs %s | Status: %s",
			game.ID, game.HomeTeam.FullName, game.VisitorTeam.FullName, game.Status)
		update := &pb.Update{
			Type:    "score",
			Message: updateMessage,
		}

		// Broadcast to subscriptions matching the team (case‑insensitive).
		manager.BroadcastUpdate(update, func(sub *subscription.Subscription) bool {
			if sub.Request.Team != "" {
				teamName := sub.Request.Team
				if containsIgnoreCase(game.HomeTeam.FullName, teamName) ||
					containsIgnoreCase(game.VisitorTeam.FullName, teamName) {
					return true
				}
			}
			// (Player subscriptions can be implemented similarly if the API supported them.)
			return false
		})
	}

	return nil
}

// notifyError sends an error update to all subscribers.
func notifyError(manager *subscription.Manager, errMsg string) {
	errorUpdate := &pb.Update{
		Type:    "error",
		Message: errMsg,
	}
	manager.BroadcastUpdate(errorUpdate, func(sub *subscription.Subscription) bool {
		return true
	})
}

// containsIgnoreCase checks if a string contains a substring, case‑insensitively.
func containsIgnoreCase(str, substr string) bool {
	return strings.Contains(strings.ToLower(str), strings.ToLower(substr))
}
