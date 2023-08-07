package mongocrud

import (
	// Standard
	"context"
	"errors"
	"reflect"
	"time"

	// External
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrorAlreadyExists = errors.New("item already exists")
	ErrorInsertFailed  = errors.New("failed to insert")
	ErrorGetFailed     = errors.New("failed to get")
	ErrorDeleteFailed  = errors.New("failed to delete")
	ErrorUpdateFailed  = errors.New("failed to update")

	ErrorIdBlank = errors.New("id cannot be blank")

	ErrorValueNotPointer = errors.New("failed to accept argument, must be a pointer")
	ErrorValueNotStruct  = errors.New("failed to accept argument, must be a struct")
)

type DatabaseCollection struct {
	name       string
	collection mongoCollection
}

type mongoCollection interface {
	InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	FindOne(context.Context, interface{}, ...*options.FindOneOptions) *mongo.SingleResult
	ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
}

func (c *DatabaseCollection) NewItem(ctx context.Context, i interface{}) (*mongo.SingleResult, error) {
	rv := reflect.ValueOf(i)

	if rv.Kind() != reflect.Ptr {
		return nil, ErrorValueNotPointer
	}

	tgt := rv.Elem()
	if tgt.Kind() != reflect.Struct {
		return nil, ErrorValueNotStruct
	}

	if tgt.FieldByName("ID").Interface().(primitive.ObjectID) == primitive.NilObjectID {
		return nil, ErrorIdBlank
	}

	_, err := c.collection.InsertOne(ctx, i)
	if err != nil {
		return nil, ErrorInsertFailed
	}

	return c.GetItem(ctx, "id", tgt.FieldByName("ID").Interface().(primitive.ObjectID).Hex())
}

func (c *DatabaseCollection) ItemExists(ctx context.Context, by, value string) bool {
	var filter primitive.D

	switch by {
	case "_id", "id":
		objID, _ := primitive.ObjectIDFromHex(value)
		filter = bson.D{{Key: "_id", Value: objID}}
	default:
		filter = bson.D{primitive.E{Key: by, Value: value}}
	}

	result := c.collection.FindOne(ctx, filter)
	return result.Err() == nil
}

func (c *DatabaseCollection) GetItem(ctx context.Context, by, value string) (*mongo.SingleResult, error) {
	var filter primitive.D

	switch by {
	case "_id", "id":
		objID, _ := primitive.ObjectIDFromHex(value)
		filter = bson.D{{Key: "_id", Value: objID}}
	default:
		filter = bson.D{primitive.E{Key: by, Value: value}}
	}

	item := c.collection.FindOne(ctx, filter)
	if item.Err() != nil {
		return nil, ErrorGetFailed
	}

	return item, nil
}

func (c *DatabaseCollection) UpdateItem(ctx context.Context, i interface{}) (*mongo.SingleResult, error) {
	rv := reflect.ValueOf(i)

	if rv.Kind() != reflect.Ptr {
		return nil, ErrorValueNotPointer
	}

	tgt := rv.Elem()
	if tgt.Kind() != reflect.Struct {
		return nil, ErrorValueNotStruct
	}

	if tgt.FieldByName("ID").Interface().(primitive.ObjectID) == primitive.NilObjectID {
		return nil, ErrorIdBlank
	}

	id := tgt.FieldByName("ID").Interface().(primitive.ObjectID)

	filter := bson.D{{Key: "_id", Value: id}}

	_, err := c.collection.ReplaceOne(ctx, filter, i)
	if err != nil {
		return nil, ErrorUpdateFailed
	}

	return c.GetItem(ctx, "id", tgt.FieldByName("ID").Interface().(primitive.ObjectID).Hex())
}

func (c *DatabaseCollection) DeleteItem(id primitive.ObjectID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	filter := bson.D{{Key: "_id", Value: id}}

	_, err := c.collection.DeleteOne(ctx, filter)
	if err != nil {
		return ErrorDeleteFailed
	}

	return nil
}

func (c *DatabaseCollection) MongoCollectionType() *mongoCollection {
	return &c.collection
}
