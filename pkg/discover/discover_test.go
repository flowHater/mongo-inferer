package discover

import (
	"context"
	"reflect"
	"sort"
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
			name: "multiple nested case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1, "aaaaaaaa2": oid2, "nested": primitive.M{"field1": oid1, "field2": oid2}}},
			want: []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}, {Path: "nested.field1", Value: oid1.Hex()}, {Path: "nested.field2", Value: oid2.Hex()}},
		}, {
			name: "string representing ObjectID",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1.Hex(), "aaaaaaaa2": oid2, "nested": primitive.M{"field1": oid1, "field2": oid2}}},
			want: []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}, {Path: "nested.field1", Value: oid1.Hex()}, {Path: "nested.field2", Value: oid2.Hex()}},
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

			if !reflect.DeepEqual(sortLinkInPlace(got), sortLinkInPlace(tt.want)) {
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

	type test struct {
		fields fields
		args   args
		want   []Link
		name   string
		ctrl   *gomock.Controller
	}

	tests := []test{
		func() test {
			ctx := context.Background()
			ctrl := gomock.NewController(t)

			oid1 := primitive.NewObjectID()

			repo := mock_discover.NewMockRepo(ctrl)
			repo.EXPECT().ListDatabases(gomock.Eq(ctx)).Return([]string{"db1", "db2"}, nil)
			repo.EXPECT().ListCollections(gomock.Eq(ctx), "db1").Return([]string{"cl1", "cl2"}, nil)
			repo.EXPECT().ListCollections(gomock.Eq(ctx), "db2").Return([]string{"cl3", "cl4"}, nil)

			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl1", oid1).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl2", oid1).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl3", oid1).Return(true, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl4", oid1).Return(false, nil)

			a := args{ctx: ctx, ls: []Link{Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()}}}
			want := []Link{{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: "db2.cl3"}}

			return test{name: "nominal case - 2 db, 2 collections for each", fields: fields{repo: repo}, args: a, want: want, ctrl: ctrl}
		}(),
		func() test {
			ctx := context.Background()
			ctrl := gomock.NewController(t)

			oid1 := primitive.NewObjectID()
			oid2 := primitive.NewObjectID()

			repo := mock_discover.NewMockRepo(ctrl)
			repo.EXPECT().ListDatabases(gomock.Eq(ctx)).Return([]string{"db1", "db2"}, nil)
			repo.EXPECT().ListCollections(gomock.Eq(ctx), "db1").Return([]string{"cl1", "cl2"}, nil)
			repo.EXPECT().ListCollections(gomock.Eq(ctx), "db2").Return([]string{"cl3", "cl4"}, nil)

			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl1", oid1).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl2", oid1).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl3", oid1).Return(true, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl4", oid1).Return(false, nil)

			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl1", oid2).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db1", "cl2", oid2).Return(true, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl3", oid2).Return(false, nil)
			repo.EXPECT().ExistsByID(gomock.Eq(ctx), "db2", "cl4", oid2).Return(false, nil)

			a := args{ctx: ctx, ls: []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex()},
			}}
			want := []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: "db2.cl3"},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex(), With: "db1.cl2"},
			}

			return test{name: "nominal case - MORE COMPLEX", fields: fields{repo: repo}, args: a, want: want, ctrl: ctrl}
		}(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.ctrl.Finish()
			d := Discover{
				repo: tt.fields.repo,
			}

			got, err := d.matchLink(tt.args.ctx, tt.args.ls)
			if err != nil {
				t.Errorf("Discover.MatchLink() error = %v", err)
				return
			}

			if !reflect.DeepEqual(sortLinkInPlace(got), sortLinkInPlace(tt.want)) {
				t.Errorf("Discover.MatchLink() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_reduceLinks(t *testing.T) {
	type args struct {
		lss [][]Link
	}

	tests := []struct {
		name    string
		args    args
		want    CollectionLinks
		wantErr bool
	}{
		{
			name: "Nominal case - with all links present at 100%",
			args: args{
				lss: [][]Link{
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: "db2.cl3"},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: "db1.cl2"},
					},
				},
			}, want: CollectionLinks{
				"eeeeeeeeee.aaaaaaaaaaaaa.ccccccc":     Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: "db2.cl3", Percent: 1},
				"ttttttttttt3.ppppppppppp.dda.ccccccc": Link{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: "db1.cl2", Percent: 1},
			},
		}, {
			name: "Nominal case - with links present at random%",
			args: args{
				lss: [][]Link{
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: "db2.cl3"},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: "db1.cl2"},
					},
					{
						{Path: "eeeeeeeeee.455aaaaaaaa.ccccccc", With: "db8.cl40"},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: "db1.cl2"},
					},
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: "db2.cl3"},
						{Path: "ttttttttttt3.ooop.dda.ccccccc", With: "db4.cl4"},
					},
				},
			}, want: CollectionLinks{
				"eeeeeeeeee.455aaaaaaaa.ccccccc":       {Path: "eeeeeeeeee.455aaaaaaaa.ccccccc", With: "db8.cl40", Percent: 0.33333334},
				"eeeeeeeeee.aaaaaaaaaaaaa.ccccccc":     {Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: "db2.cl3", Percent: 0.6666667},
				"ttttttttttt3.ooop.dda.ccccccc":        {Path: "ttttttttttt3.ooop.dda.ccccccc", With: "db4.cl4", Percent: 0.33333334},
				"ttttttttttt3.ppppppppppp.dda.ccccccc": {Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: "db1.cl2", Percent: 0.6666667},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reduceLinks(tt.args.lss)
			if (err != nil) != tt.wantErr {
				t.Errorf("reduceLinks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("reduceLinks() = %+v, want %v", got, tt.want)
			}
		})
	}
}

func sortLinkInPlace(ls []Link) []Link {
	sort.Slice(ls, func(i, j int) bool {
		return ls[i].Path < ls[j].Path
	})

	return ls
}
