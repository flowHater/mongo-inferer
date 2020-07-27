package discover

import (
	"context"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestLinkify(t *testing.T) {
	type args struct {
		m           primitive.M
		currentPath string
	}

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()

	tests := []struct {
		name string
		args args
		want []Link
	}{
		{
			name: "nominal case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeeeId": oid1}},
			want: []Link{{Path: "eeeeeId", Value: oid1.Hex()}},
		}, {
			name: "nested case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee": primitive.M{"testId": oid1}}},
			want: []Link{{Path: "eeeee.testId", Value: oid1.Hex()}},
		}, {
			name: "multiple case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1, "aaaaaaaa2": oid2}},
			want: []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}},
		}, {
			name: "multiple nested case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1, "aaaaaaaa2": oid2, "nested": primitive.M{"field1": oid1, "field2": oid2}}},
			want: []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}, {Path: "nested.field1", Value: oid1.Hex()}, {Path: "nested.field2", Value: oid2.Hex()}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Linkify(tt.args.m, tt.args.currentPath)
			if err != nil {
				t.Errorf("Linkify() error = %v", err)
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
		ctx context.Context
		ls  []Link
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Link
		wantErr bool
	}{
		// {name: "nominal case", fields: fields{repo:}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Discover{
				repo: tt.fields.repo,
			}
			got, err := d.matchLink(tt.args.ctx, tt.args.ls)
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
