package discover

import (
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/flowHater/mongo-inferer/pkg/mock_discover"

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
		name     string
		args     args
		want     []Link
		disabled bool
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
			disabled: true,
			name:     "multiple nested case",
			args:     args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1, "aaaaaaaa2": oid2, "nested": primitive.M{"field1": oid1, "field2": oid2}}},
			want:     []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}, {Path: "nested.field1", Value: oid1.Hex()}, {Path: "nested.field2", Value: oid2.Hex()}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.disabled {
				return
			}

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

func TestMatchLink(t *testing.T) {
	type fields struct {
		repo Repo
	}
	type args struct {
		ctx context.Context
		ls  []Link
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	oid1 := primitive.NewObjectID()
	// oid2 := primitive.NewObjectID()

	repo := func() Repo {
		m := mock_discover.NewMockRepo(ctrl)
		m.EXPECT().ListDatabases(gomock.Eq(ctx)).Return([]string{"db1", "db2"}, nil)
		m.EXPECT().ListCollections(gomock.Eq(ctx), "db1").Return([]string{"cl1", "cl2"}, nil)
		m.EXPECT().ListCollections(gomock.Eq(ctx), "db2").Return([]string{"cl3", "cl4"}, nil)

		m.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl1", oid1).Return(false, nil)
		m.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl2", oid1).Return(false, nil)
		m.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl3", oid1).Return(true, nil)
		m.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl4", oid1).Return(false, nil)

		return m
	}()

	a := args{ctx: ctx, ls: []Link{Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()}}}
	want := []Link{{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: "db2.cl3"}}

	t.Run("nominal case", func(t *testing.T) {
		d := Discover{
			repo: repo,
		}

		got, err := d.matchLink(ctx, a.ls)
		if err != nil {
			t.Errorf("Discover.MatchLink() error = %v", err)
			return
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Discover.MatchLink() = %v, want %v", got, want)
		}
	})
}
