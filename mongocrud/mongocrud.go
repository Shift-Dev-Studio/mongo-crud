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

	// MongoDB Collections init
	collectionStrings, err := resp.Database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		resp.logger.Warn("unable to get collection names")
	}

	for _, collection := range collectionStrings {
		temp := resp.Database.Collection(collection)

		resp.Collections = append(resp.Collections, &DatabaseCollection{
			name:       collection,
			collection: temp,
		})
	}

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

// ListCollections returns a slice of collections of the configured database
func (s DatabaseClient) ListCollections() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collections, err := s.Database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		s.logger.Warn("get collections failed",
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
