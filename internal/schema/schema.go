package schema

import (
	"fmt"
	"go-story/internal/data"
	"strconv"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/mitchellh/mapstructure"
)

// Build constructs the GraphQL schema using provided repo.
func Build(repo *data.Repo) (graphql.Schema, error) {
	jsonScalar := newJSONScalar()
	dateTimeScalar := newDateTimeScalar()

	// Input types
	stringFilterFields := graphql.InputObjectConfigFieldMap{}
	stringFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "StringFilter",
		Fields: stringFilterFields,
	})
	stringFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.String}
	stringFilterFields["in"] = &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.String)}
	stringFilterFields["not"] = &graphql.InputObjectFieldConfig{Type: stringFilterInput}

	booleanFilterFields := graphql.InputObjectConfigFieldMap{}
	booleanFilterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "BooleanFilter",
		Fields: booleanFilterFields,
	})
	booleanFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: graphql.Boolean}

	dateTimeNullableFilterFields := graphql.InputObjectConfigFieldMap{}
	dateTimeNullableFilter := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "DateTimeNullableFilter",
		Fields: dateTimeNullableFilterFields,
	})
	dateTimeNullableFilterFields["equals"] = &graphql.InputObjectFieldConfig{Type: dateTimeScalar}
	dateTimeNullableFilterFields["not"] = &graphql.InputObjectFieldConfig{Type: dateTimeNullableFilter}

	sectionWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":  &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})
	sectionManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SectionManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: sectionWhereInputType},
		},
	})

	categoryWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CategoryWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":         &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state":        &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isMemberOnly": &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
		},
	})
	categoryManyRelationFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CategoryManyRelationFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"some": &graphql.InputObjectFieldConfig{Type: categoryWhereInputType},
		},
	})

	partnerWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PartnerWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	postWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":       &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"sections":   &graphql.InputObjectFieldConfig{Type: sectionManyRelationFilterType},
			"categories": &graphql.InputObjectFieldConfig{Type: categoryManyRelationFilterType},
			"state":      &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isAdult":    &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"isMember":   &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"isFeatured": &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"topics": &graphql.InputObjectFieldConfig{Type: graphql.NewInputObject(graphql.InputObjectConfig{
				Name: "PostTopicsWhereInput",
				Fields: graphql.InputObjectConfigFieldMap{
					"id": &graphql.InputObjectFieldConfig{Type: graphql.NewInputObject(graphql.InputObjectConfig{
						Name: "IDFilter",
						Fields: graphql.InputObjectConfigFieldMap{
							"equals": &graphql.InputObjectFieldConfig{Type: graphql.ID},
						},
					})},
				},
			})},
		},
	})

	postWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id":   &graphql.InputObjectFieldConfig{Type: graphql.ID},
			"slug": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	externalWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ExternalWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":          &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state":         &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"partner":       &graphql.InputObjectFieldConfig{Type: partnerWhereInputType},
			"publishedDate": &graphql.InputObjectFieldConfig{Type: dateTimeNullableFilter},
		},
	})

	orderDirectionEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"asc":  &graphql.EnumValueConfig{Value: "asc"},
			"desc": &graphql.EnumValueConfig{Value: "desc"},
		},
	})

	postOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PostOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"title":         &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	externalOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ExternalOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"publishedDate": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt":     &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	topicWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug":       &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"name":       &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"state":      &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"isFeatured": &graphql.InputObjectFieldConfig{Type: booleanFilterInput},
			"type":       &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"style":      &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	topicWhereUniqueInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicWhereUniqueInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"id":   &graphql.InputObjectFieldConfig{Type: graphql.ID},
			"name": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"slug": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	topicOrderByInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TopicOrderByInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"sortOrder": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"createdAt": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"updatedAt": &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"name":      &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
			"slug":      &graphql.InputObjectFieldConfig{Type: orderDirectionEnum},
		},
	})

	groupWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "GroupWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"_placeholder": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})

	tagWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TagWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"slug": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
			"name": &graphql.InputObjectFieldConfig{Type: stringFilterInput},
		},
	})

	photoWhereInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PhotoWhereInput",
		Fields: graphql.InputObjectConfigFieldMap{
			// PhotoWhereInput 目前沒有實作具體的過濾邏輯
			// 添加一個 placeholder 欄位以滿足 GraphQL 的要求（InputObject 必須至少有一個欄位）
			"_placeholder": &graphql.InputObjectFieldConfig{
				Type:        graphql.Boolean,
				Description: "Placeholder field (PhotoWhereInput filtering not implemented yet)",
			},
		},
	})

	// Object types
	imageFileType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ImageFile",
		Fields: graphql.Fields{
			"width":  &graphql.Field{Type: graphql.Int},
			"height": &graphql.Field{Type: graphql.Int},
		},
	})

	resizedType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Resized",
		Fields: graphql.Fields{
			"original": &graphql.Field{Type: graphql.String},
			"w480":     &graphql.Field{Type: graphql.String},
			"w800":     &graphql.Field{Type: graphql.String},
			"w1200":    &graphql.Field{Type: graphql.String},
			"w1600":    &graphql.Field{Type: graphql.String},
			"w2400":    &graphql.Field{Type: graphql.String},
		},
	})

	sectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Section",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.ID},
			"name":  &graphql.Field{Type: graphql.String},
			"slug":  &graphql.Field{Type: graphql.String},
			"state": &graphql.Field{Type: graphql.String},
		},
	})

	categoryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Category",
		Fields: graphql.Fields{
			"id":           &graphql.Field{Type: graphql.ID},
			"name":         &graphql.Field{Type: graphql.String},
			"slug":         &graphql.Field{Type: graphql.String},
			"state":        &graphql.Field{Type: graphql.String},
			"isMemberOnly": &graphql.Field{Type: graphql.Boolean},
			"sections": &graphql.Field{
				Type: graphql.NewList(sectionType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					c, ok := p.Source.(data.Category)
					if !ok {
						return nil, nil
					}
					where, err := decodeSectionWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterSections(c.Sections, where), nil
				},
			},
		},
	})

	contactType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Contact",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
		},
	})

	groupType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Group",
		Fields: graphql.Fields{
			"id":      &graphql.Field{Type: graphql.ID},
			"keyword": &graphql.Field{Type: graphql.String},
		},
	})

	userType := graphql.NewObject(graphql.ObjectConfig{
		Name: "User",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.ID},
			"name":  &graphql.Field{Type: graphql.String},
			"email": &graphql.Field{Type: graphql.String},
		},
	})

	tagType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Tag",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{Type: graphql.String},
			"slug": &graphql.Field{Type: graphql.String},
		},
	})

	photoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Photo",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.ID},
			"name": &graphql.Field{
				Type: graphql.String,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					photo, ok := p.Source.(*data.Photo)
					if !ok || photo == nil {
						return nil, nil
					}
					if name, ok := photo.Metadata["name"].(string); ok {
						return name, nil
					}
					return photo.Name, nil
				},
			},
			"topicKeywords": &graphql.Field{
				Type: graphql.String,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					photo, ok := p.Source.(*data.Photo)
					if !ok || photo == nil {
						return nil, nil
					}
					if keywords, ok := photo.Metadata["topicKeywords"].(string); ok {
						return keywords, nil
					}
					return photo.TopicKeywords, nil
				},
			},
			"imageFile":   &graphql.Field{Type: imageFileType},
			"resized":     &graphql.Field{Type: resizedType},
			"resizedWebp": &graphql.Field{Type: resizedType},
		},
	})

	videoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Video",
		Fields: graphql.Fields{
			"id":       &graphql.Field{Type: graphql.ID},
			"videoSrc": &graphql.Field{Type: graphql.String},
			"heroImage": &graphql.Field{
				Type: photoType,
			},
		},
	})

	partnerType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Partner",
		Fields: graphql.Fields{
			"id":          &graphql.Field{Type: graphql.ID},
			"slug":        &graphql.Field{Type: graphql.String},
			"name":        &graphql.Field{Type: graphql.String},
			"showOnIndex": &graphql.Field{Type: graphql.Boolean},
			"showThumb":   &graphql.Field{Type: graphql.Boolean},
			"showBrief":   &graphql.Field{Type: graphql.Boolean},
		},
	})

	var postType *graphql.Object
	var topicType *graphql.Object
	topicType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Topic",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":        &graphql.Field{Type: graphql.ID},
				"name":      &graphql.Field{Type: graphql.String},
				"slug":      &graphql.Field{Type: graphql.String},
				"sortOrder": &graphql.Field{Type: graphql.Int},
				"state":     &graphql.Field{Type: graphql.String},
				"brief":     &graphql.Field{Type: jsonScalar},
				"heroImage": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizeTopic(p.Source).HeroImage, nil
					},
				},
				"heroUrl":        &graphql.Field{Type: graphql.String},
				"leading":        &graphql.Field{Type: graphql.String},
				"og_title":       &graphql.Field{Type: graphql.String},
				"og_description": &graphql.Field{Type: graphql.String},
				"og_image": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizeTopic(p.Source).OgImage, nil
					},
				},
				"isFeatured":  &graphql.Field{Type: graphql.Boolean},
				"title_style": &graphql.Field{Type: graphql.String},
				"type":        &graphql.Field{Type: graphql.String},
				"style":       &graphql.Field{Type: graphql.String},
				"javascript":  &graphql.Field{Type: graphql.String},
				"dfp":         &graphql.Field{Type: graphql.String},
				"mobile_dfp":  &graphql.Field{Type: graphql.String},
				"createdAt":   &graphql.Field{Type: dateTimeScalar},
				"updatedAt":   &graphql.Field{Type: dateTimeScalar},
				"tags": &graphql.Field{
					Type: graphql.NewList(tagType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodeTagWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterTags(current.Tags, where), nil
					},
				},
				"tagsCount": &graphql.Field{
					Type: graphql.Int,
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodeTagWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return len(filterTags(current.Tags, where)), nil
					},
				},
				"slideshow_images": &graphql.Field{
					Type: graphql.NewList(photoType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: photoWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodePhotoWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterPhotos(current.SlideshowImages, where), nil
					},
				},
				"slideshow_imagesInInputOrder": &graphql.Field{
					Type: graphql.NewList(photoType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizeTopic(p.Source).SlideshowImagesInOrder, nil
					},
				},
				"manualOrderOfSlideshowImages": &graphql.Field{Type: jsonScalar},
				"posts": &graphql.Field{
					Type: graphql.NewList(postType),
					Args: graphql.FieldConfigArgument{
						"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
						"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
						"take":    &graphql.ArgumentConfig{Type: graphql.Int},
						"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodePostWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						orders := parseOrderRules(p.Args["orderBy"])
						take, skip := parsePagination(p.Args)
						return filterAndPaginatePosts(current.Posts, where, orders, take, skip), nil
					},
				},
				"postsCount": &graphql.Field{
					Type: graphql.Int,
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: postWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodePostWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						// 如果 topic 已經有 posts，直接過濾計算
						if len(current.Posts) > 0 {
							return len(filterPosts(current.Posts, where)), nil
						}
						// 否則需要從資料庫查詢
						topicID, _ := strconv.Atoi(current.ID)
						if topicID == 0 {
							return 0, nil
						}
						postWhere := &data.PostWhereInput{
							Topics: &data.PostTopicsWhereInput{
								ID: &data.IDFilter{Equals: &current.ID},
							},
						}
						if where != nil {
							// 合併 where 條件
							postWhere.State = where.State
							postWhere.IsFeatured = where.IsFeatured
							postWhere.IsMember = where.IsMember
							postWhere.IsAdult = where.IsAdult
						}
						return repo.QueryPostsCount(p.Context, postWhere)
					},
				},
				"featuredPostsCount": &graphql.Field{
					Type: graphql.Int,
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: postWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizeTopic(p.Source)
						where, err := data.DecodePostWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						// 創建一個包含 isFeatured 的 where
						featuredWhere := &data.PostWhereInput{
							IsFeatured: &data.BooleanFilter{Equals: boolPtr(true)},
						}
						if where != nil {
							featuredWhere.State = where.State
							featuredWhere.IsMember = where.IsMember
							featuredWhere.IsAdult = where.IsAdult
						}
						topicID, _ := strconv.Atoi(current.ID)
						if topicID == 0 {
							return 0, nil
						}
						featuredWhere.Topics = &data.PostTopicsWhereInput{
							ID: &data.IDFilter{Equals: &current.ID},
						}
						return repo.QueryPostsCount(p.Context, featuredWhere)
					},
				},
			}
		}),
	})

	postType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Post",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":            &graphql.Field{Type: graphql.ID},
				"slug":          &graphql.Field{Type: graphql.String},
				"title":         &graphql.Field{Type: graphql.String},
				"subtitle":      &graphql.Field{Type: graphql.String},
				"state":         &graphql.Field{Type: graphql.String},
				"style":         &graphql.Field{Type: graphql.String},
				"publishedDate": &graphql.Field{Type: dateTimeScalar},
				"updatedAt":     &graphql.Field{Type: dateTimeScalar},
				"isMember":      &graphql.Field{Type: graphql.Boolean},
				"isAdult":       &graphql.Field{Type: graphql.Boolean},
				"sections": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.Sections, where), nil
					},
				},
				"sectionsInInputOrder": &graphql.Field{
					Type: graphql.NewList(sectionType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeSectionWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterSections(current.SectionsInInputOrder, where), nil
					},
				},
				"categories": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.Categories, where), nil
					},
				},
				"categoriesInInputOrder": &graphql.Field{
					Type: graphql.NewList(categoryType),
					Args: graphql.FieldConfigArgument{
						"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						current := normalizePost(p.Source)
						where, err := decodeCategoryWhere(p.Args["where"])
						if err != nil {
							return nil, err
						}
						return filterCategories(current.CategoriesInInputOrder, where), nil
					},
				},
				"writers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Writers, nil
					},
				},
				"writersInInputOrder": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).WritersInInputOrder, nil
					},
				},
				"photographers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Photographers, nil
					},
				},
				"camera_man": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).CameraMan, nil
					},
				},
				"designers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Designers, nil
					},
				},
				"engineers": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Engineers, nil
					},
				},
				"vocals": &graphql.Field{
					Type: graphql.NewList(contactType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Vocals, nil
					},
				},
				"extend_byline": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).ExtendByline, nil
					},
				},
				"tags": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Tags, nil
					},
				},
				"tags_algo": &graphql.Field{
					Type: graphql.NewList(tagType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TagsAlgo, nil
					},
				},
				"heroVideo": &graphql.Field{
					Type: videoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroVideo, nil
					},
				},
				"heroImage": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HeroImage, nil
					},
				},
				"heroCaption": &graphql.Field{Type: graphql.String},
				"brief":       &graphql.Field{Type: jsonScalar},
				"trimmedContent": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).TrimmedContent, nil
					},
				},
				"content": &graphql.Field{
					Type: jsonScalar,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Content, nil
					},
				},
				"relateds": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Relateds, nil
					},
				},
				"relatedsInInputOrder": &graphql.Field{
					Type: graphql.NewList(postType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsInInputOrder, nil
					},
				},
				"relatedsOne": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsOne, nil
					},
				},
				"relatedsTwo": &graphql.Field{
					Type: postType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).RelatedsTwo, nil
					},
				},
				"redirect": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Redirect, nil
					},
				},
				"og_title": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgTitle, nil
					},
				},
				"og_image": &graphql.Field{
					Type: photoType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgImage, nil
					},
				},
				"og_description": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).OgDescription, nil
					},
				},
				"hiddenAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).HiddenAdvertised, nil
					},
				},
				"isAdvertised": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsAdvertised, nil
					},
				},
				"isFeatured": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).IsFeatured, nil
					},
				},
				"topics": &graphql.Field{
					Type: topicType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return normalizePost(p.Source).Topics, nil
					},
				},
			}
		}),
	})

	externalType := graphql.NewObject(graphql.ObjectConfig{
		Name: "External",
		Fields: graphql.Fields{
			"id":                  &graphql.Field{Type: graphql.ID},
			"slug":                &graphql.Field{Type: graphql.String},
			"title":               &graphql.Field{Type: graphql.String},
			"state":               &graphql.Field{Type: graphql.String},
			"publishedDate":       &graphql.Field{Type: dateTimeScalar},
			"publishedDateString": &graphql.Field{Type: graphql.String},
			"extend_byline":       &graphql.Field{Type: graphql.String},
			"thumb":               &graphql.Field{Type: graphql.String},
			"thumbCaption":        &graphql.Field{Type: graphql.String},
			"brief":               &graphql.Field{Type: graphql.String},
			"content":             &graphql.Field{Type: graphql.String},
			"source":              &graphql.Field{Type: graphql.String},
			"partner":             &graphql.Field{Type: partnerType},
			"sections": &graphql.Field{
				Type: graphql.NewList(sectionType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := decodeSectionWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterSections(current.Sections, where), nil
				},
			},
			"sectionsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: sectionWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := decodeSectionWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return len(filterSections(current.Sections, where)), nil
				},
			},
			"categories": &graphql.Field{
				Type: graphql.NewList(categoryType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := decodeCategoryWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterCategories(current.Categories, where), nil
				},
			},
			"categoriesCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: categoryWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := decodeCategoryWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return len(filterCategories(current.Categories, where)), nil
				},
			},
			"tags": &graphql.Field{
				Type: graphql.NewList(tagType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodeTagWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterTags(current.Tags, where), nil
				},
			},
			"tagsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodeTagWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return len(filterTags(current.Tags, where)), nil
				},
			},
			"tags_algo": &graphql.Field{
				Type: graphql.NewList(tagType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodeTagWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return filterTags(current.TagsAlgo, where), nil
				},
			},
			"tags_algoCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: tagWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodeTagWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return len(filterTags(current.TagsAlgo, where)), nil
				},
			},
			"relateds": &graphql.Field{
				Type: graphql.NewList(postType),
				Args: graphql.FieldConfigArgument{
					"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return filterAndPaginatePosts(current.Relateds, where, orders, take, skip), nil
				},
			},
			"relatedsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return len(filterPosts(current.Relateds, where)), nil
				},
			},
			"groups": &graphql.Field{
				Type: graphql.NewList(groupType),
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: groupWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					return current.Groups, nil
				},
			},
			"groupsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: groupWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					current := normalizeExternal(p.Source)
					return len(current.Groups), nil
				},
			},
			"createdAt": &graphql.Field{Type: dateTimeScalar},
			"updatedAt": &graphql.Field{Type: dateTimeScalar},
			"createdBy": &graphql.Field{Type: userType},
			"updatedBy": &graphql.Field{Type: userType},
		},
	})

	rootQuery := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"posts": &graphql.Field{
				Type: graphql.NewList(postType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(postOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return repo.QueryPosts(p.Context, where, orders, take, skip)
				},
			},
			"postsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryPostsCount(p.Context, where)
				},
			},
			"post": &graphql.Field{
				Type: postType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: postWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodePostWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryPostByUnique(p.Context, where)
				},
			},
			"topics": &graphql.Field{
				Type: graphql.NewList(topicType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(topicOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: topicWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return repo.QueryTopics(p.Context, where, orders, take, skip)
				},
			},
			"topicsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: topicWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryTopicsCount(p.Context, where)
				},
			},
			"topic": &graphql.Field{
				Type: topicType,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: topicWhereUniqueInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeTopicWhereUnique(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryTopicByUnique(p.Context, where)
				},
			},
			"externals": &graphql.Field{
				Type: graphql.NewList(externalType),
				Args: graphql.FieldConfigArgument{
					"take":    &graphql.ArgumentConfig{Type: graphql.Int},
					"skip":    &graphql.ArgumentConfig{Type: graphql.Int},
					"orderBy": &graphql.ArgumentConfig{Type: graphql.NewList(externalOrderByInput)},
					"where":   &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					orders := parseOrderRules(p.Args["orderBy"])
					take, skip := parsePagination(p.Args)
					return repo.QueryExternals(p.Context, where, orders, take, skip)
				},
			},
			"externalsCount": &graphql.Field{
				Type: graphql.Int,
				Args: graphql.FieldConfigArgument{
					"where": &graphql.ArgumentConfig{Type: externalWhereInputType},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					where, err := data.DecodeExternalWhere(p.Args["where"])
					if err != nil {
						return nil, err
					}
					return repo.QueryExternalsCount(p.Context, where)
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{
		Query: rootQuery,
	})
}

// Scalars
func newJSONScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "Arbitrary JSON value",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			return parseASTValue(valueAST)
		},
	})
}

func newDateTimeScalar() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name: "DateTime",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				return v.Value
			default:
				return nil
			}
		},
	})
}

// Helpers
func parseOrderRules(input interface{}) []data.OrderRule {
	rules := []data.OrderRule{}
	list, ok := input.([]interface{})
	if !ok {
		return rules
	}
	for _, item := range list {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for field, dir := range entry {
			rules = append(rules, data.OrderRule{
				Field:     field,
				Direction: fmt.Sprintf("%v", dir),
			})
		}
	}
	return rules
}

func parsePagination(args map[string]interface{}) (take int, skip int) {
	if raw, ok := args["take"]; ok {
		take = asInt(raw)
	}
	if raw, ok := args["skip"]; ok {
		skip = asInt(raw)
	}
	if skip < 0 {
		skip = 0
	}
	return
}

func asInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func parseASTValue(value ast.Value) interface{} {
	switch v := value.(type) {
	case *ast.StringValue:
		return v.Value
	case *ast.IntValue:
		return v.Value
	case *ast.FloatValue:
		return v.Value
	case *ast.BooleanValue:
		return v.Value
	case *ast.ObjectValue:
		result := map[string]interface{}{}
		for _, field := range v.Fields {
			result[field.Name.Value] = parseASTValue(field.Value)
		}
		return result
	case *ast.ListValue:
		values := make([]interface{}, 0, len(v.Values))
		for _, item := range v.Values {
			values = append(values, parseASTValue(item))
		}
		return values
	default:
		return nil
	}
}

// Filter helpers for nested fields
func decodeSectionWhere(input interface{}) (*data.SectionWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where data.SectionWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeCategoryWhere(input interface{}) (*data.CategoryWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where data.CategoryWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, err
	}
	return &where, nil
}

func decodeInto(input interface{}, target interface{}) error {
	cfg := &mapstructure.DecoderConfig{
		TagName: "mapstructure",
		Result:  target,
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}

func filterSections(items []data.Section, where *data.SectionWhereInput) []data.Section {
	if where == nil {
		return items
	}
	result := make([]data.Section, 0, len(items))
	for _, s := range items {
		if matchesSectionWhere(&s, where) {
			result = append(result, s)
		}
	}
	return result
}

func filterCategories(items []data.Category, where *data.CategoryWhereInput) []data.Category {
	if where == nil {
		return items
	}
	result := make([]data.Category, 0, len(items))
	for _, c := range items {
		if matchesCategoryWhere(&c, where) {
			result = append(result, c)
		}
	}
	return result
}

func filterTags(items []data.Tag, where *data.TagWhereInput) []data.Tag {
	if where == nil {
		return items
	}
	result := []data.Tag{}
	for _, item := range items {
		if !matchesStringFilter(item.Slug, where.Slug) {
			continue
		}
		if !matchesStringFilter(item.Name, where.Name) {
			continue
		}
		result = append(result, item)
	}
	return result
}

func filterPhotos(items []data.Photo, where *data.PhotoWhereInput) []data.Photo {
	if where == nil {
		return items
	}
	// PhotoWhereInput 目前沒有實作過濾邏輯，直接返回
	return items
}

func filterPosts(items []data.Post, where *data.PostWhereInput) []data.Post {
	if where == nil {
		return items
	}
	result := []data.Post{}
	for _, item := range items {
		if !matchesStringFilter(item.State, where.State) {
			continue
		}
		if !matchesBooleanFilter(item.IsFeatured, where.IsFeatured) {
			continue
		}
		if !matchesBooleanFilter(item.IsMember, where.IsMember) {
			continue
		}
		if !matchesBooleanFilter(item.IsAdult, where.IsAdult) {
			continue
		}
		result = append(result, item)
	}
	return result
}

func filterAndPaginatePosts(items []data.Post, where *data.PostWhereInput, orders []data.OrderRule, take, skip int) []data.Post {
	filtered := filterPosts(items, where)
	// TODO: 實作排序和分頁
	if skip > 0 && skip < len(filtered) {
		filtered = filtered[skip:]
	}
	if take > 0 && take < len(filtered) {
		filtered = filtered[:take]
	}
	return filtered
}

func matchesSectionWhere(s *data.Section, where *data.SectionWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(s.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(s.State, where.State) {
		return false
	}
	return true
}

func matchesCategoryWhere(c *data.Category, where *data.CategoryWhereInput) bool {
	if where == nil {
		return true
	}
	if !matchesStringFilter(c.Slug, where.Slug) {
		return false
	}
	if !matchesStringFilter(c.State, where.State) {
		return false
	}
	if !matchesBooleanFilter(c.IsMemberOnly, where.IsMemberOnly) {
		return false
	}
	return true
}

func matchesStringFilter(value string, filter *data.StringFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	if len(filter.In) > 0 {
		found := false
		for _, item := range filter.In {
			if value == item {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if filter.Not != nil && matchesStringFilter(value, filter.Not) {
		return false
	}
	return true
}

func matchesBooleanFilter(value bool, filter *data.BooleanFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Equals != nil && value != *filter.Equals {
		return false
	}
	return true
}

func normalizePost(src interface{}) data.Post {
	switch v := src.(type) {
	case data.Post:
		return v
	case *data.Post:
		if v == nil {
			return data.Post{}
		}
		return *v
	default:
		return data.Post{}
	}
}

func normalizeTopic(src interface{}) data.Topic {
	switch v := src.(type) {
	case data.Topic:
		return v
	case *data.Topic:
		if v == nil {
			return data.Topic{}
		}
		return *v
	default:
		return data.Topic{}
	}
}

func normalizeExternal(src interface{}) data.External {
	switch v := src.(type) {
	case data.External:
		return v
	case *data.External:
		if v == nil {
			return data.External{}
		}
		return *v
	default:
		return data.External{}
	}
}

func boolPtr(b bool) *bool {
	return &b
}
