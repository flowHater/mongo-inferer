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

var withCancelCtx = reflect.TypeOf((*context.Context)(nil)).Elem()

func TestLinkify(t *testing.T) {
	type args struct {
		m           primitive.M
		currentPath string
	}

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()
	oid4 := primitive.NewObjectID()

	tests := []struct {
		name     string
		args     args
		want     []Link
		disabled bool
	}{
		{
			name: "nominal case",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeeeId": oid1, "_id": oid1}},
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
		}, {
			name: "array with nested oid",
			args: args{currentPath: "", m: primitive.M{"keyField": "valueField", "eeeee1": oid1.Hex(), "aaaaaaaa2": oid2, "nested": primitive.M{"array1": primitive.A{primitive.M{"field1": oid1, "field3": oid2}}, "field2": oid2}}},
			want: []Link{{Path: "eeeee1", Value: oid1.Hex()}, {Path: "aaaaaaaa2", Value: oid2.Hex()}, {Path: "nested.array1.$.field1", Value: oid1.Hex()}, {Path: "nested.array1.$.field3", Value: oid2.Hex()}, {Path: "nested.field2", Value: oid2.Hex()}},
		}, {
			name: "multiple nested arrays with nested oid - complex/20",
			args: args{currentPath: "", m: primitive.M{
				"basic": oid1.Hex(),
				"nested": primitive.M{
					"array1": primitive.A{
						primitive.M{"array2": primitive.A{primitive.M{"doubleNestedField": oid1, "useless": "field"}}, "field3": oid2},
						primitive.M{"array2": primitive.A{primitive.M{"doubleNestedField": oid4, "useless": "field"}}, "field3": oid3},
						primitive.M{
							"array4": primitive.A{primitive.M{
								"doubleNestedField": oid2,
								"useless":           "field",
								"array5":            primitive.A{primitive.M{"superNestedField": oid3}},
							}},
							"field3": oid4,
						},
					},
					"field2": oid2,
				},
			},
			},
			want: []Link{
				{Path: "basic", Value: oid1.Hex()},
				{Path: "nested.array1.$.array2.$.doubleNestedField", Value: oid1.Hex()},
				{Path: "nested.array1.$.field3", Value: oid2.Hex()},
				{Path: "nested.array1.$.array2.$.doubleNestedField", Value: oid4.Hex()},
				{Path: "nested.array1.$.field3", Value: oid3.Hex()},
				{Path: "nested.array1.$.array4.$.doubleNestedField", Value: oid2.Hex()},
				{Path: "nested.array1.$.array4.$.array5.$.superNestedField", Value: oid3.Hex()},
				{Path: "nested.array1.$.field3", Value: oid4.Hex()},
				{Path: "nested.field2", Value: oid2.Hex()}},
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
		fetcher Fetcher
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

			fetcher := mock_discover.NewMockFetcher(ctrl)
			fetcher.EXPECT().ListDatabases(gomock.AssignableToTypeOf(withCancelCtx)).Return([]string{"db1", "db2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db1").Return([]string{"cl1", "cl2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db2").Return([]string{"cl3", "cl4"}, nil)

			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "cl3", oid1).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			a := args{ctx: ctx, ls: []Link{Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()}}}
			want := []Link{{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: []string{"db2.cl3"}}}

			return test{name: "nominal case - 2 db, 2 collections for each", fields: fields{fetcher: fetcher}, args: a, want: want, ctrl: ctrl}
		}(),
		func() test {
			ctx := context.Background()
			ctrl := gomock.NewController(t)

			oid1 := primitive.NewObjectID()
			oid2 := primitive.NewObjectID()

			fetcher := mock_discover.NewMockFetcher(ctrl)
			fetcher.EXPECT().ListDatabases(gomock.AssignableToTypeOf(withCancelCtx)).Return([]string{"db1", "db2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db1").Return([]string{"cl1", "cl2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db2").Return([]string{"cl3", "cl4"}, nil)

			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "cl3", oid1).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "cl2", oid2).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			a := args{ctx: ctx, ls: []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex()},
			}}
			want := []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: []string{"db2.cl3"}},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex(), With: []string{"db1.cl2"}},
			}

			return test{name: "nominal case - MORE COMPLEX", fields: fields{fetcher: fetcher}, args: a, want: want, ctrl: ctrl}
		}(),

		func() test {
			ctx := context.Background()
			ctrl := gomock.NewController(t)

			oid1 := primitive.NewObjectID()
			oid2 := primitive.NewObjectID()
			oid3 := primitive.NewObjectID()

			fetcher := mock_discover.NewMockFetcher(ctrl)
			fetcher.EXPECT().ListDatabases(gomock.AssignableToTypeOf(withCancelCtx)).Return([]string{"db1", "db2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db1").Return([]string{"cl1", "cl2"}, nil)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db2").Return([]string{"cl3", "cl4"}, nil)

			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "cl3", oid1).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "cl2", oid2).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "cl4", oid3).Return(true, nil)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			a := args{ctx: ctx, ls: []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex()},
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid3.Hex()},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex()},
			}}
			want := []Link{
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid1.Hex(), With: []string{"db2.cl3"}},
				{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", Value: oid3.Hex(), With: []string{"db2.cl4"}},
				{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", Value: oid2.Hex(), With: []string{"db1.cl2"}},
			}

			return test{name: "nominal case - MORE COMPLEX", fields: fields{fetcher: fetcher}, args: a, want: want, ctrl: ctrl}
		}(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.ctrl.Finish()
			d := New(tt.fields.fetcher)

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
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}},
					},
				},
			}, want: CollectionLinks{
				"eeeeeeeeee.aaaaaaaaaaaaa.ccccccc":     Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}, Avg: 1},
				"ttttttttttt3.ppppppppppp.dda.ccccccc": Link{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}, Avg: 1},
			},
		}, {
			name: "Nominal case - with links present at random%",
			args: args{
				lss: [][]Link{
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}},
					},
					{
						{Path: "eeeeeeeeee.455aaaaaaaa.ccccccc", With: []string{"db8.cl40"}},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}},
					},
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}},
						{Path: "ttttttttttt3.ooop.dda.ccccccc", With: []string{"db4.cl4"}},
					},
				},
			}, want: CollectionLinks{
				"eeeeeeeeee.455aaaaaaaa.ccccccc":       {Path: "eeeeeeeeee.455aaaaaaaa.ccccccc", With: []string{"db8.cl40"}, Avg: 0.33333334},
				"eeeeeeeeee.aaaaaaaaaaaaa.ccccccc":     {Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}, Avg: 0.6666667},
				"ttttttttttt3.ooop.dda.ccccccc":        {Path: "ttttttttttt3.ooop.dda.ccccccc", With: []string{"db4.cl4"}, Avg: 0.33333334},
				"ttttttttttt3.ppppppppppp.dda.ccccccc": {Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}, Avg: 0.6666667},
			},
		}, {
			name: "With same path matching multiple collections - polymorphism",
			args: args{
				lss: [][]Link{
					{
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl4"}},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}},
					}, {
						{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl3"}},
						{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}},
					},
				},
			},
			want: CollectionLinks{
				"eeeeeeeeee.aaaaaaaaaaaaa.ccccccc":     Link{Path: "eeeeeeeeee.aaaaaaaaaaaaa.ccccccc", With: []string{"db2.cl4", "db2.cl3"}, Avg: 1},
				"ttttttttttt3.ppppppppppp.dda.ccccccc": Link{Path: "ttttttttttt3.ppppppppppp.dda.ccccccc", With: []string{"db1.cl2"}, Avg: 1},
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

func TestDiscover_Collection(t *testing.T) {
	type fields struct {
		fetcher Fetcher
	}
	type args struct {
		ctx        context.Context
		db         string
		collection string
	}
	type test struct {
		name    string
		fields  fields
		args    args
		want    CollectionLinks
		wantErr bool
		ctrl    *gomock.Controller
	}
	tests := []test{
		func() test {
			ctx := context.Background()
			ctrl := gomock.NewController(t)

			oid1 := primitive.NewObjectID()
			oid2 := primitive.NewObjectID()
			oid3 := primitive.NewObjectID()
			oid4 := primitive.NewObjectID()
			oid5 := primitive.NewObjectID()
			oid6 := primitive.NewObjectID()
			oid7 := primitive.NewObjectID()
			oid8 := primitive.NewObjectID()
			oid9 := primitive.NewObjectID()
			oid10 := primitive.NewObjectID()
			oid11 := primitive.NewObjectID()
			oid12 := primitive.NewObjectID()
			oid13 := primitive.NewObjectID()
			oid14 := primitive.NewObjectID()
			oid15 := primitive.NewObjectID()

			a := args{ctx: ctx, collection: "cl00020", db: "db902"}

			fetcher := mock_discover.NewMockFetcher(ctrl)
			fetcher.EXPECT().SampleCollection(gomock.AssignableToTypeOf(withCancelCtx), a.db, a.collection, sampleSize).Return([]primitive.M{
				{"keyField": "valueField", "eeeeeId": oid4, "otherField": oid7, "otherFieldStr": oid10, "_id": oid1, "nested": primitive.M{"field": oid14}},
				{"keyField": "valueField", "eeeeeId": oid5, "otherField": oid8, "otherFieldStr": oid11, "randomField": oid13, "_id": oid2},
				{"keyField": "valueField", "eeeeeId": oid6, "otherField": oid9, "otherFieldStr": oid12, "_id": oid3, "nested": primitive.M{"field": oid15}},
			}, nil)
			fetcher.EXPECT().ListDatabases(gomock.AssignableToTypeOf(withCancelCtx)).Return([]string{"db1", "db2"}, nil).Times(3)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db1").Return([]string{"otherFields", "randomFields", "nestedDocs"}, nil).Times(3)
			fetcher.EXPECT().ListCollections(gomock.AssignableToTypeOf(withCancelCtx), "db2").Return([]string{"otherFieldStrs", "uselessDocs", "eeeees"}, nil).Times(3)

			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "otherFields", oid7).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "otherFields", oid8).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "otherFields", oid9).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "nestedDocs", oid14).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "nestedDocs", oid15).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db1", "randomFields", oid13).Return(true, nil).Times(1)

			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "otherFieldStrs", oid10).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "otherFieldStrs", oid11).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "otherFieldStrs", oid12).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "eeeees", oid4).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "eeeees", oid5).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), "db2", "eeeees", oid6).Return(true, nil).Times(1)
			fetcher.EXPECT().ExistsByID(gomock.AssignableToTypeOf(withCancelCtx), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			want := CollectionLinks{
				"eeeeeId":       {Path: "eeeeeId", With: []string{"db2.eeeees"}, Avg: 1},
				"otherField":    {Path: "otherField", With: []string{"db1.otherFields"}, Avg: 1},
				"otherFieldStr": {Path: "otherFieldStr", With: []string{"db2.otherFieldStrs"}, Avg: 1},
				"randomField":   {Path: "randomField", With: []string{"db1.randomFields"}, Avg: 0.33333334},
				"nested.field":  {Path: "nested.field", With: []string{"db1.nestedDocs"}, Avg: 0.6666667},
			}

			return test{name: "nominal case - should output all links inside the source collection", fields: fields{fetcher: fetcher}, args: a, want: want, ctrl: ctrl}
		}(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := New(tt.fields.fetcher)
			got, err := d.Collection(tt.args.ctx, tt.args.db, tt.args.collection)
			if (err != nil) != tt.wantErr {
				t.Errorf("Discover.Collection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Discover.Collection() = %+v, \nwant %+v", got, tt.want)
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
