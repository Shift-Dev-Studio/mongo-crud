package mongocrud

import (
	// Standard
	"context"
	"fmt"
	"time"

	// External
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

type DatabaseConfiguration struct {
	DatabaseUser          string
	DatabasePassword      string
	DatabaseConnectionUrl string
	DatabaseName          string
}

type DatabaseClient struct {
	Instance *mongo.Client

	Database    *mongo.Database
	Collections []*DatabaseCollection

	logger *zap.Logger
}

// NewStorage creates a Mongo client for communicating with Mongo DB's
func NewStorage(c *DatabaseConfiguration, l *zap.Logger) (*DatabaseClient, error) {
	resp := &DatabaseClient{}
	// Set package variables
	resp.logger = l.With(zap.String("package", "mongocrud"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		err error
	)

	// MongoDB Init
	var uri string = fmt.Sprintf("mongodb+srv://%s:%s@%s/%s?retryWrites=true&w=majority",
		c.DatabaseUser,
		c.DatabasePassword,
		c.DatabaseConnectionUrl,
		c.DatabaseName,
	)
	resp.Instance, err = mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		resp.logger.Error("new client failed",
			zap.String("func", "GetInstance"),
			zap.Error(err),
		)
		return resp, err
	} else {
		resp.logger.Info("new client created")
	}

	// MongoDB Connect
	err = resp.Instance.Connect(ctx)
	if err != nil {
		resp.logger.Error("client connection failed",
			zap.String("func", "GetInstance"),
			zap.Error(err),
		)
		return resp, err
	} else {
		resp.logger.Info("client connection established")
	}

	// MongoDB Database init
	resp.Database = resp.Instance.Database(c.DatabaseName)

	return resp, nil
}

// Ping sends a ping to the Mongo client to determine if the connection is still alive
func (s DatabaseClient) Ping() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.Instance.Ping(ctx, readpref.Primary())
	if err != nil {
		s.logger.Error("ping failed",
			zap.String("func", "Ping"),
			zap.Error(err),
		)
	} else {
		s.logger.Info("client ping success")
	}
}

// AddCollections appends to the current database collections (allows for mock collections to be added)
func (c *DatabaseClient) AddCollections(ctx context.Context, cols []*DatabaseCollection) {
	for i := range cols {
		c.Collections = append(c.Collections, cols[i])
	}
}

// MongoCollectionsToDatabaseCollections converts the Mongo DB collections present in the database to the local
// database collection for use in program
func (c *DatabaseClient) MongoCollectionsToDatabaseCollections(ctx context.Context) (resp []*DatabaseCollection) {
	collectionStrings := c.ListCollections(ctx)

	for _, collection := range collectionStrings {
		temp := c.Database.Collection(collection)

		resp = append(resp, &DatabaseCollection{
			name:       collection,
			collection: temp,
		})
	}

	return resp
}

// ListCollections returns a slice of collections of the configured database
func (c DatabaseClient) ListCollections(ctx context.Context) []string {
	collections, err := c.Database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		c.logger.Warn("get collections failed",
			zap.String("func", "ListCollections"),
			zap.Error(err),
		)
	}

	return collections
}

func (c *DatabaseClient) GetCollection(collectionName string) *DatabaseCollection {
	for i := range c.Collections {
		if c.Collections[i].name == collectionName {
			return c.Collections[i]
		}
	}

	return nil
}
