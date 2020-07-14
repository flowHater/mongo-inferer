package discover

import (
	"context"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestLinkify(t *testing.T) {
	type args struct {
		m           primitive.M
		currentPath string
	}
	tests := []struct {
		name    string
		args    args
		want    []Link
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Linkify(tt.args.m, tt.args.currentPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("Linkify() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Linkify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiscover_MatchLink(t *testing.T) {
	type fields struct {
		repo Repo
	}
	type args struct {
		ctx    context.Context
		client *mongo.Client
		ls     []Link
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Link
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Discover{
				repo: tt.fields.repo,
			}
			got, err := d.MatchLink(tt.args.ctx, tt.args.client, tt.args.ls)
			if (err != nil) != tt.wantErr {
				t.Errorf("Discover.MatchLink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Discover.MatchLink() = %v, want %v", got, tt.want)
			}
		})
	}
}
