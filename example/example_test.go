package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/theplant/testenv"
	"github.com/tnclong/go-que"
)

const (
	SubjectIdentityCreated = "ciam.identity.created"
	SubjectIdentityUpdated = "ciam.identity.updated"
)

// User represents user data structure
type User struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Email           string `json:"email"`
	SubscriptionOpt bool   `json:"subscriptionOpt"`
}

// UserUpdate represents user information update data structure with from/to states
type UserUpdate struct {
	From User `json:"from"`
	To   User `json:"to"`
}

// TestCIAMBusinessMarketingIntegration simulates communication between CIAM, Business, and Marketing
// systems through go-bus based on the sequence diagram.
func TestCIAMBusinessMarketingIntegration(t *testing.T) {
	env, err := testenv.New().DBEnable(true).SetUp()
	require.NoError(t, err, "Failed to create test environment")
	defer func() { _ = env.TearDown() }()

	db, err := env.DB.DB()
	require.NoError(t, err, "Failed to get database connection")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	businessEvents := make(chan User, 10)
	marketingEvents := make(chan UserUpdate, 10)
	marketingBQSyncs := make(chan User, 10)

	businessReady := make(chan struct{})
	marketingReady := make(chan struct{})

	newUser := User{
		ID:              "user123",
		Name:            "John Doe",
		Email:           "john@example.com",
		SubscriptionOpt: false,
	}

	updatedUser := User{
		ID:              "user123",
		Name:            "John Doe",
		Email:           "john.doe@company.com", // Email changed
		SubscriptionOpt: true,                   // Subscription opt-in changed
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go startBusiness(ctx, t, db, &wg, businessEvents, businessReady)
	go startMarketing(ctx, t, db, &wg, marketingEvents, marketingBQSyncs, marketingReady)
	go startCIAM(ctx, t, db, &wg, newUser, updatedUser, businessReady, marketingReady)

	// Wait for the business service to process the event
	select {
	case receivedUser := <-businessEvents:
		assert.Equal(t, newUser.ID, receivedUser.ID, "Business service received incorrect user ID")
		assert.Equal(t, newUser.Name, receivedUser.Name, "Business service received incorrect user name")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for business service to process user creation")
	}

	// Wait for the marketing service to process the update event
	select {
	case receivedUpdate := <-marketingEvents:
		assert.Equal(t, newUser.ID, receivedUpdate.From.ID, "Marketing service received incorrect original user ID")
		assert.Equal(t, updatedUser.ID, receivedUpdate.To.ID, "Marketing service received incorrect updated user ID")
		assert.Equal(t, newUser.Email, receivedUpdate.From.Email, "Marketing service received incorrect original email")
		assert.Equal(t, updatedUser.Email, receivedUpdate.To.Email, "Marketing service received incorrect updated email")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for marketing service to process user update")
	}

	// Verify that the marketing service detected changes and synced to BQ
	select {
	case syncedUser := <-marketingBQSyncs:
		assert.Equal(t, updatedUser.ID, syncedUser.ID, "BQ sync received incorrect user ID")
		assert.Equal(t, updatedUser.Email, syncedUser.Email, "BQ sync received incorrect email")
		assert.Equal(t, updatedUser.SubscriptionOpt, syncedUser.SubscriptionOpt, "BQ sync received incorrect subscription status")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for marketing service to sync to BQ")
	}

	// Signal services to shutdown
	cancel()

	// Wait for all service goroutines to complete
	wg.Wait()

	// Close channels after test completion
	close(businessEvents)
	close(marketingEvents)
	close(marketingBQSyncs)
}

// startCIAM simulates CIAM system publishing user creation and update events
func startCIAM(ctx context.Context, t *testing.T, db *sql.DB, wg *sync.WaitGroup,
	newUser User, updatedUser User,
	businessReady <-chan struct{}, marketingReady <-chan struct{},
) {
	defer wg.Done()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create CIAM bus instance")

	// Wait for both services to be ready, this is for testing purpose
	t.Log("CIAM waiting for other services to be ready")
	select {
	case <-businessReady:
		t.Log("Business service is ready")
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for services to be ready")
		return
	}
	select {
	case <-marketingReady:
		t.Log("Marketing service is ready")
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for services to be ready")
		return
	}
	t.Log("CIAM other services are ready")

	// User Creation
	userBytes, err := json.Marshal(newUser)
	require.NoError(t, err, "Failed to marshal user data")

	t.Log("CIAM creating new user in database")
	err = b.Publish(ctx, SubjectIdentityCreated, userBytes, bus.WithUniqueID(newUser.ID))
	require.NoError(t, err, "Failed to publish user creation event")

	// User Information Update
	update := UserUpdate{
		From: newUser,
		To:   updatedUser,
	}

	updateBytes, err := json.Marshal(update)
	require.NoError(t, err, "Failed to marshal user update data")

	t.Log("CIAM updating user information in database")
	err = b.Publish(ctx, SubjectIdentityUpdated, updateBytes, bus.WithUniqueID(updatedUser.ID))
	require.NoError(t, err, "Failed to publish user update event")

	<-ctx.Done()
}

// startBusiness simulates Business system listening for user creation events
func startBusiness(ctx context.Context, t *testing.T, db *sql.DB, wg *sync.WaitGroup,
	events chan<- User, ready chan<- struct{},
) {
	defer wg.Done()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Business bus instance")

	businessQueue := b.Queue("business_service")

	consumer, err := businessQueue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		var user User
		if err := json.Unmarshal(msg.Payload, &user); err != nil {
			return err
		}

		t.Logf("Business service received new user: %s", user.Name)
		events <- user

		t.Logf("Business service issued coupon for user: %s", user.ID)

		return msg.Done(ctx)
	})
	require.NoError(t, err, "Failed to start business service consumer")
	defer func() { _ = consumer.Stop() }()
	t.Logf("Business service consumer started")

	_, err = businessQueue.Subscribe(ctx, SubjectIdentityCreated, bus.WithPlanConfig(bus.PlanConfig{
		RetryPolicy:     bus.DefaultRetryPolicy,
		RunAtDelta:      0,
		UniqueLifecycle: que.Always,
	}))
	require.NoError(t, err, "Failed to subscribe business service to identity created events")
	t.Logf("Business service subscribed to identity created events")

	close(ready)
	t.Log("Business service signaled ready")

	<-ctx.Done()
}

// startMarketing simulates Marketing system detecting email or subscription changes
func startMarketing(ctx context.Context, t *testing.T, db *sql.DB, wg *sync.WaitGroup,
	events chan<- UserUpdate, bqSyncs chan<- User, ready chan<- struct{},
) {
	defer wg.Done()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Marketing bus instance")

	marketingQueue := b.Queue("marketing_service")

	consumer, err := marketingQueue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		var update UserUpdate
		if err := json.Unmarshal(msg.Payload, &update); err != nil {
			return err
		}

		t.Logf("Marketing service received user update: %s", update.To.Name)
		events <- update

		// Check if email or subscription status changed
		emailChanged := update.From.Email != update.To.Email
		subscriptionChanged := update.From.SubscriptionOpt != update.To.SubscriptionOpt

		if emailChanged || subscriptionChanged {
			t.Logf("Marketing detected changes in email or subscription for user: %s", update.To.ID)
			t.Logf("Marketing syncing data to BQ for user: %s", update.To.ID)
			bqSyncs <- update.To
		}

		return msg.Destroy(ctx)
	})
	require.NoError(t, err, "Failed to start marketing service consumer")
	defer func() { _ = consumer.Stop() }()
	t.Logf("Marketing service consumer started")

	_, err = marketingQueue.Subscribe(ctx, SubjectIdentityUpdated)
	require.NoError(t, err, "Failed to subscribe marketing service to identity updated events")
	t.Logf("Marketing service subscribed to identity updated events")

	close(ready)
	t.Log("Marketing service signaled ready")

	<-ctx.Done()
}
