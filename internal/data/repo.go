package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mitchellh/mapstructure"
)

// Domain models
type ImageFile struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type Resized struct {
	Original string `json:"original"`
	W480     string `json:"w480"`
	W800     string `json:"w800"`
	W1200    string `json:"w1200"`
	W1600    string `json:"w1600"`
	W2400    string `json:"w2400"`
}

type Photo struct {
	ID          string         `json:"id"`
	ImageFile   ImageFile      `json:"imageFile"`
	Resized     Resized        `json:"resized"`
	ResizedWebp Resized        `json:"resizedWebp"`
	Metadata    map[string]any `json:"-"`
}

type Section struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Slug  string `json:"slug"`
	State string `json:"state"`
}

type Category struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Slug         string    `json:"slug"`
	State        string    `json:"state"`
	IsMemberOnly bool      `json:"isMemberOnly"`
	Sections     []Section `json:"sections"`
}

type Contact struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Tag struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type TagWhereInput struct {
	Slug *StringFilter `mapstructure:"slug"`
	Name *StringFilter `mapstructure:"name"`
}

type PhotoWhereInput struct {
	// 目前不需要實作具體的過濾邏輯
}

type Video struct {
	ID        string `json:"id"`
	VideoSrc  string `json:"videoSrc"`
	HeroImage *Photo `json:"heroImage"`
}

type Partner struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	ShowOnIndex bool   `json:"showOnIndex"`
	ShowThumb   bool   `json:"showThumb"`
	ShowBrief   bool   `json:"showBrief"`
}

type Topic struct {
	ID                           string         `json:"id"`
	Name                         string         `json:"name"`
	Slug                         string         `json:"slug"`
	SortOrder                    *int           `json:"sortOrder"`
	State                        string         `json:"state"`
	Brief                        map[string]any `json:"brief"`
	HeroImage                    *Photo         `json:"heroImage"`
	HeroURL                      string         `json:"heroUrl"`
	Leading                      string         `json:"leading"`
	OgTitle                      string         `json:"og_title"`
	OgDescription                string         `json:"og_description"`
	OgImage                      *Photo         `json:"og_image"`
	IsFeatured                   bool           `json:"isFeatured"`
	TitleStyle                   string         `json:"title_style"`
	Type                         string         `json:"type"`
	Style                        string         `json:"style"`
	Tags                         []Tag          `json:"tags"`
	SlideshowImages              []Photo        `json:"slideshow_images"`
	SlideshowImagesInOrder       []Photo        `json:"slideshow_imagesInInputOrder"`
	ManualOrderOfSlideshowImages map[string]any `json:"manualOrderOfSlideshowImages"`
	Posts                        []Post         `json:"posts"`
	Javascript                   string         `json:"javascript"`
	Dfp                          string         `json:"dfp"`
	MobileDfp                    string         `json:"mobile_dfp"`
	CreatedAt                    string         `json:"createdAt"`
	UpdatedAt                    string         `json:"updatedAt"`
	Metadata                     map[string]any `json:"-"`
}

type Post struct {
	ID                     string         `json:"id"`
	Slug                   string         `json:"slug"`
	Title                  string         `json:"title"`
	Subtitle               string         `json:"subtitle"`
	State                  string         `json:"state"`
	Style                  string         `json:"style"`
	PublishedDate          string         `json:"publishedDate"`
	UpdatedAt              string         `json:"updatedAt"`
	IsMember               bool           `json:"isMember"`
	IsAdult                bool           `json:"isAdult"`
	Sections               []Section      `json:"sections"`
	SectionsInInputOrder   []Section      `json:"sectionsInInputOrder"`
	Categories             []Category     `json:"categories"`
	CategoriesInInputOrder []Category     `json:"categoriesInInputOrder"`
	Writers                []Contact      `json:"writers"`
	WritersInInputOrder    []Contact      `json:"writersInInputOrder"`
	Photographers          []Contact      `json:"photographers"`
	CameraMan              []Contact      `json:"camera_man"`
	Designers              []Contact      `json:"designers"`
	Engineers              []Contact      `json:"engineers"`
	Vocals                 []Contact      `json:"vocals"`
	ExtendByline           string         `json:"extend_byline"`
	Tags                   []Tag          `json:"tags"`
	TagsAlgo               []Tag          `json:"tags_algo"`
	HeroVideo              *Video         `json:"heroVideo"`
	HeroImage              *Photo         `json:"heroImage"`
	HeroCaption            string         `json:"heroCaption"`
	Brief                  map[string]any `json:"brief"`
	TrimmedContent         map[string]any `json:"trimmedContent"`
	Content                map[string]any `json:"content"`
	Relateds               []Post         `json:"relateds"`
	RelatedsInInputOrder   []Post         `json:"relatedsInInputOrder"`
	RelatedsOne            *Post          `json:"relatedsOne"`
	RelatedsTwo            *Post          `json:"relatedsTwo"`
	Redirect               string         `json:"redirect"`
	OgTitle                string         `json:"og_title"`
	OgImage                *Photo         `json:"og_image"`
	OgDescription          string         `json:"og_description"`
	HiddenAdvertised       bool           `json:"hiddenAdvertised"`
	IsAdvertised           bool           `json:"isAdvertised"`
	IsFeatured             bool           `json:"isFeatured"`
	Topics                 *Topic         `json:"topics"`
	Metadata               map[string]any `json:"-"`
}

type External struct {
	ID            string         `json:"id"`
	Slug          string         `json:"slug"`
	Partner       *Partner       `json:"partner"`
	Title         string         `json:"title"`
	State         string         `json:"state"`
	PublishedDate string         `json:"publishedDate"`
	ExtendByline  string         `json:"extend_byline"`
	Thumb         string         `json:"thumb"`
	ThumbCaption  string         `json:"thumbCaption"`
	Brief         string         `json:"brief"`
	Content       string         `json:"content"`
	UpdatedAt     string         `json:"updatedAt"`
	Tags          []Tag          `json:"tags"`
	Relateds      []Post         `json:"relateds"`
	Metadata      map[string]any `json:"metadata"`
}

// Filters
type StringFilter struct {
	Equals *string       `mapstructure:"equals"`
	In     []string      `mapstructure:"in"`
	Not    *StringFilter `mapstructure:"not"`
}

type BooleanFilter struct {
	Equals *bool `mapstructure:"equals"`
}

type SectionWhereInput struct {
	Slug  *StringFilter `mapstructure:"slug"`
	State *StringFilter `mapstructure:"state"`
}

type SectionManyRelationFilter struct {
	Some *SectionWhereInput `mapstructure:"some"`
}

type CategoryWhereInput struct {
	Slug         *StringFilter  `mapstructure:"slug"`
	State        *StringFilter  `mapstructure:"state"`
	IsMemberOnly *BooleanFilter `mapstructure:"isMemberOnly"`
}

type CategoryManyRelationFilter struct {
	Some *CategoryWhereInput `mapstructure:"some"`
}

type PartnerWhereInput struct {
	Slug *StringFilter `mapstructure:"slug"`
}

type DateTimeNullableFilter struct {
	Equals *string                 `mapstructure:"equals"`
	Not    *DateTimeNullableFilter `mapstructure:"not"`
}

type IDFilter struct {
	Equals *string `mapstructure:"equals"`
}

type PostTopicsWhereInput struct {
	ID *IDFilter `mapstructure:"id"`
}

type PostWhereInput struct {
	Slug       *StringFilter               `mapstructure:"slug"`
	Sections   *SectionManyRelationFilter  `mapstructure:"sections"`
	Categories *CategoryManyRelationFilter `mapstructure:"categories"`
	State      *StringFilter               `mapstructure:"state"`
	IsAdult    *BooleanFilter              `mapstructure:"isAdult"`
	IsMember   *BooleanFilter              `mapstructure:"isMember"`
	IsFeatured *BooleanFilter              `mapstructure:"isFeatured"`
	Topics     *PostTopicsWhereInput       `mapstructure:"topics"`
}

type PostWhereUniqueInput struct {
	ID   *string `mapstructure:"id"`
	Slug *string `mapstructure:"slug"`
}

type ExternalWhereInput struct {
	Slug          *StringFilter           `mapstructure:"slug"`
	State         *StringFilter           `mapstructure:"state"`
	Partner       *PartnerWhereInput      `mapstructure:"partner"`
	PublishedDate *DateTimeNullableFilter `mapstructure:"publishedDate"`
}

type TopicWhereInput struct {
	Slug       *StringFilter  `mapstructure:"slug"`
	Name       *StringFilter  `mapstructure:"name"`
	State      *StringFilter  `mapstructure:"state"`
	IsFeatured *BooleanFilter `mapstructure:"isFeatured"`
	Type       *StringFilter  `mapstructure:"type"`
	Style      *StringFilter  `mapstructure:"style"`
}

type TopicWhereUniqueInput struct {
	ID   *string `mapstructure:"id"`
	Name *string `mapstructure:"name"`
	Slug *string `mapstructure:"slug"`
}

type OrderRule struct {
	Field     string
	Direction string
}

// Repo wraps DB access.
type Repo struct {
	db          *sql.DB
	staticsHost string
	cache       *Cache
}

const timeLayoutMilli = "2006-01-02T15:04:05.000Z07:00"

func NewDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn := stdlib.OpenDB(*cfg)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return conn, nil
}

func NewRepo(db *sql.DB, staticsHost string, cache *Cache) *Repo {
	return &Repo{db: db, staticsHost: staticsHost, cache: cache}
}

// Decode helpers
func DecodePostWhere(input interface{}) (*PostWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where PostWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post where: %w", err)
	}
	return &where, nil
}

func DecodePostWhereUnique(input interface{}) (*PostWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	var where PostWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("post unique where: %w", err)
	}
	return &where, nil
}

func DecodeExternalWhere(input interface{}) (*ExternalWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where ExternalWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("external where: %w", err)
	}
	return &where, nil
}

func DecodeTopicWhere(input interface{}) (*TopicWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where TopicWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("topic where: %w", err)
	}
	return &where, nil
}

func DecodeTopicWhereUnique(input interface{}) (*TopicWhereUniqueInput, error) {
	if input == nil {
		return nil, nil
	}
	var where TopicWhereUniqueInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("topic unique where: %w", err)
	}
	return &where, nil
}

func DecodeTagWhere(input interface{}) (*TagWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where TagWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("tag where: %w", err)
	}
	return &where, nil
}

func DecodePhotoWhere(input interface{}) (*PhotoWhereInput, error) {
	if input == nil {
		return nil, nil
	}
	var where PhotoWhereInput
	if err := decodeInto(input, &where); err != nil {
		return nil, fmt.Errorf("photo where: %w", err)
	}
	return &where, nil
}

// Public queries
func (r *Repo) QueryPosts(ctx context.Context, where *PostWhereInput, orders []OrderRule, take, skip int) ([]Post, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("posts", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		var cachedPosts []Post
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedPosts); found {
			return cachedPosts, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
		if len(f.In) > 0 {
			conds = append(conds, fmt.Sprintf(`%s = ANY($%d)`, field, argIdx))
			args = append(args, f.In)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("state", where.State)
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
				sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
				args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildOrderClause(orders[0]))
	} else {
		sb.WriteString(` ORDER BY "publishedDate" DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	posts := []Post{}
	for rows.Next() {
		var (
			p             Post
			dbID          int
			publishedAt   sql.NullTime
			updatedAt     sql.NullTime
			heroImageID   sql.NullInt64
			heroVideoID   sql.NullInt64
			ogImageID     sql.NullInt64
			topicsID      sql.NullInt64
			relatedsOneID sql.NullInt64
			relatedsTwoID sql.NullInt64
			briefRaw      []byte
			contentRaw    []byte
		)
		if err := rows.Scan(
			&dbID,
			&p.Slug,
			&p.Title,
			&p.Subtitle,
			&p.State,
			&p.Style,
			&p.IsMember,
			&p.IsAdult,
			&publishedAt,
			&updatedAt,
			&p.HeroCaption,
			&p.ExtendByline,
			&heroImageID,
			&heroVideoID,
			&briefRaw,
			&contentRaw,
			&p.Redirect,
			&p.OgTitle,
			&p.OgDescription,
			&p.HiddenAdvertised,
			&p.IsAdvertised,
			&p.IsFeatured,
			&topicsID,
			&ogImageID,
			&relatedsOneID,
			&relatedsTwoID,
		); err != nil {
			return nil, err
		}
		p.ID = strconv.Itoa(dbID)
		if publishedAt.Valid {
			p.PublishedDate = publishedAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updatedAt.Valid {
			p.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
		}
		p.Brief = decodeJSONBytes(briefRaw)
		p.Content = decodeJSONBytes(contentRaw)
		p.TrimmedContent = p.Content
		p.Metadata = map[string]any{
			"heroImageID":   nullableInt(heroImageID),
			"ogImageID":     nullableInt(ogImageID),
			"heroVideoID":   nullableInt(heroVideoID),
			"topicsID":      nullableInt(topicsID),
			"relatedsOneID": nullableInt(relatedsOneID),
			"relatedsTwoID": nullableInt(relatedsTwoID),
		}
		posts = append(posts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(posts) == 0 {
		return posts, nil
	}
	if err := r.enrichPosts(ctx, posts); err != nil {
		return nil, err
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("posts", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		_ = r.cache.Set(ctx, cacheKey, posts)
	}

	return posts, nil
}

func (r *Repo) QueryPostsCount(ctx context.Context, where *PostWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	where = ensurePostPublished(where)

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Post" p`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("state", where.State)
		if where.IsAdult != nil && where.IsAdult.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isAdult" = $%d`, argIdx))
			args = append(args, *where.IsAdult.Equals)
			argIdx++
		}
		if where.IsMember != nil && where.IsMember.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isMember" = $%d`, argIdx))
			args = append(args, *where.IsMember.Equals)
			argIdx++
		}
		if where.Sections != nil && where.Sections.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Post_sections\" ps JOIN \"Section\" s ON s.id = ps.\"B\" WHERE ps.\"A\" = p.id"
			if where.Sections.Some.Slug != nil && where.Sections.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND s.slug = $%d", argIdx)
				args = append(args, *where.Sections.Some.Slug.Equals)
				argIdx++
			}
			if where.Sections.Some.State != nil && where.Sections.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND s.state = $%d", argIdx)
				args = append(args, *where.Sections.Some.State.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
		if where.Categories != nil && where.Categories.Some != nil {
			sub := "EXISTS (SELECT 1 FROM \"_Category_posts\" cp JOIN \"Category\" c ON c.id = cp.\"A\" WHERE cp.\"B\" = p.id"
			if where.Categories.Some.Slug != nil && where.Categories.Some.Slug.Equals != nil {
				sub += fmt.Sprintf(" AND c.slug = $%d", argIdx)
				args = append(args, *where.Categories.Some.Slug.Equals)
				argIdx++
			}
			if where.Categories.Some.State != nil && where.Categories.Some.State.Equals != nil {
				sub += fmt.Sprintf(" AND c.state = $%d", argIdx)
				args = append(args, *where.Categories.Some.State.Equals)
				argIdx++
			}
			if where.Categories.Some.IsMemberOnly != nil && where.Categories.Some.IsMemberOnly.Equals != nil {
				sub += fmt.Sprintf(" AND c.\"isMemberOnly\" = $%d", argIdx)
				args = append(args, *where.Categories.Some.IsMemberOnly.Equals)
				argIdx++
			}
			sub += ")"
			conds = append(conds, sub)
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	if err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Repo) QueryPostByUnique(ctx context.Context, where *PostWhereUniqueInput) (*Post, error) {
	if where == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("post:unique", where)
		var cachedPost *Post
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedPost); found {
			return cachedPost, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo" FROM "Post" p WHERE `)
	args := []interface{}{}
	argIdx := 1
	if where.ID != nil {
		sb.WriteString(fmt.Sprintf("id = $%d", argIdx))
		args = append(args, *where.ID)
		argIdx++
	} else if where.Slug != nil {
		sb.WriteString(fmt.Sprintf("slug = $%d", argIdx))
		args = append(args, *where.Slug)
		argIdx++
	} else {
		return nil, nil
	}
	sb.WriteString(" LIMIT 1")

	var (
		p             Post
		dbID          int
		publishedAt   sql.NullTime
		updatedAt     sql.NullTime
		heroImageID   sql.NullInt64
		heroVideoID   sql.NullInt64
		ogImageID     sql.NullInt64
		topicsID      sql.NullInt64
		relatedsOneID sql.NullInt64
		relatedsTwoID sql.NullInt64
		briefRaw      []byte
		contentRaw    []byte
	)

	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(
		&dbID,
		&p.Slug,
		&p.Title,
		&p.Subtitle,
		&p.State,
		&p.Style,
		&p.IsMember,
		&p.IsAdult,
		&publishedAt,
		&updatedAt,
		&p.HeroCaption,
		&p.ExtendByline,
		&heroImageID,
		&heroVideoID,
		&briefRaw,
		&contentRaw,
		&p.Redirect,
		&p.OgTitle,
		&p.OgDescription,
		&p.HiddenAdvertised,
		&p.IsAdvertised,
		&p.IsFeatured,
		&topicsID,
		&ogImageID,
		&relatedsOneID,
		&relatedsTwoID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.ID = strconv.Itoa(dbID)
	if publishedAt.Valid {
		p.PublishedDate = publishedAt.Time.UTC().Format(timeLayoutMilli)
	}
	if updatedAt.Valid {
		p.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
	}
	p.Brief = decodeJSONBytes(briefRaw)
	p.Content = decodeJSONBytes(contentRaw)
	p.TrimmedContent = p.Content
	p.Metadata = map[string]any{
		"heroImageID":   nullableInt(heroImageID),
		"ogImageID":     nullableInt(ogImageID),
		"heroVideoID":   nullableInt(heroVideoID),
		"topicsID":      nullableInt(topicsID),
		"relatedsOneID": nullableInt(relatedsOneID),
		"relatedsTwoID": nullableInt(relatedsTwoID),
	}
	posts := []Post{p}
	if err := r.enrichPosts(ctx, posts); err != nil {
		return nil, err
	}
	p = posts[0]

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("post:unique", where)
		_ = r.cache.Set(ctx, cacheKey, &p)
	}

	return &p, nil
}

func (r *Repo) QueryExternals(ctx context.Context, where *ExternalWhereInput, orders []OrderRule, take, skip int) ([]External, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	where = ensureExternalPublished(where)

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("externals", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		var cachedExternals []External
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedExternals); found {
			return cachedExternals, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT e.id, e.slug, e.title, e.state, e."publishedDate", e."extend_byline", e.thumb, e."thumbCaption", e.brief, e.content, e.partner, e."updatedAt" FROM "External" e`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	orderUsesPublished := len(orders) == 0 || (len(orders) > 0 && orders[0].Field == "publishedDate")
	if orderUsesPublished {
		conds = append(conds, `e."publishedDate" IS NOT NULL`)
	}

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("e.slug", where.Slug)
		buildStringFilter("e.state", where.State)
		if where.PublishedDate != nil {
			if where.PublishedDate.Equals != nil {
				conds = append(conds, fmt.Sprintf(`e."publishedDate" = $%d`, argIdx))
				args = append(args, *where.PublishedDate.Equals)
				argIdx++
			}
			if where.PublishedDate.Not != nil {
				if where.PublishedDate.Not.Equals == nil {
					conds = append(conds, `e."publishedDate" IS NOT NULL`)
				} else {
					conds = append(conds, fmt.Sprintf(`e."publishedDate" <> $%d`, argIdx))
					args = append(args, *where.PublishedDate.Not.Equals)
					argIdx++
				}
			}
		}
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildExternalOrder(orders[0]))
	} else {
		sb.WriteString(` ORDER BY e."publishedDate" DESC`)
	}
	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []External{}
	partnerIDs := []int{}
	externalIDs := []int{}
	for rows.Next() {
		var ext External
		var partnerID sql.NullInt64
		var dbID int
		var pubAt, updAt sql.NullTime
		if err := rows.Scan(&dbID, &ext.Slug, &ext.Title, &ext.State, &pubAt, &ext.ExtendByline, &ext.Thumb, &ext.ThumbCaption, &ext.Brief, &ext.Content, &partnerID, &updAt); err != nil {
			return nil, err
		}
		ext.ID = strconv.Itoa(dbID)
		if pubAt.Valid {
			ext.PublishedDate = pubAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updAt.Valid {
			ext.UpdatedAt = updAt.Time.UTC().Format(timeLayoutMilli)
		}
		externalIDs = append(externalIDs, dbID)
		if partnerID.Valid {
			ext.Metadata = map[string]any{"partnerID": int(partnerID.Int64)}
			partnerIDs = append(partnerIDs, int(partnerID.Int64))
		}
		result = append(result, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	partners, _ := r.fetchPartners(ctx, partnerIDs)
	tagsMap, _ := r.fetchExternalTags(ctx, "_External_tags", externalIDs)
	for i := range result {
		if pid := getMetaInt(result[i].Metadata, "partnerID"); pid > 0 {
			result[i].Partner = partners[pid]
		}
		idInt, _ := strconv.Atoi(result[i].ID)
		result[i].Tags = tagsMap[idInt]
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("externals", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		_ = r.cache.Set(ctx, cacheKey, result)
	}

	return result, nil
}

func (r *Repo) QueryExternalsCount(ctx context.Context, where *ExternalWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	where = ensureExternalPublished(where)
	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "External" e`)
	conds := []string{}
	args := []interface{}{}
	argIdx := 1
	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}
	if where != nil {
		buildStringFilter("e.slug", where.Slug)
		buildStringFilter("e.state", where.State)
		if where.Partner != nil && where.Partner.Slug != nil && where.Partner.Slug.Equals != nil {
			sb.WriteString(` JOIN "Partner" p ON p.id = e.partner`)
			conds = append(conds, fmt.Sprintf(`p.slug = $%d`, argIdx))
			args = append(args, *where.Partner.Slug.Equals)
			argIdx++
		}
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	var count int
	if err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Repo) QueryTopics(ctx context.Context, where *TopicWhereInput, orders []OrderRule, take, skip int) ([]Topic, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topics", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		var cachedTopics []Topic
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedTopics); found {
			return cachedTopics, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, name, slug, "sortOrder", state, brief, "heroImage", "heroUrl", "leading", "og_title", "og_description", "og_image", "isFeatured", "title_style", type, style, javascript, dfp, "mobile_dfp", "createdAt", "updatedAt" FROM "Topic" t`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
		if len(f.In) > 0 {
			conds = append(conds, fmt.Sprintf(`%s = ANY($%d)`, field, argIdx))
			args = append(args, f.In)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("name", where.Name)
		buildStringFilter("state", where.State)
		buildStringFilter("type", where.Type)
		buildStringFilter("style", where.Style)
		if where.IsFeatured != nil && where.IsFeatured.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isFeatured" = $%d`, argIdx))
			args = append(args, *where.IsFeatured.Equals)
			argIdx++
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	if len(orders) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(buildTopicOrderClause(orders[0]))
	} else {
		sb.WriteString(` ORDER BY "sortOrder" ASC NULLS LAST, "createdAt" DESC`)
	}

	if take > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	topics := []Topic{}
	for rows.Next() {
		var (
			t           Topic
			dbID        int
			sortOrder   sql.NullInt64
			heroImageID sql.NullInt64
			ogImageID   sql.NullInt64
			briefRaw    []byte
			createdAt   sql.NullTime
			updatedAt   sql.NullTime
		)
		if err := rows.Scan(
			&dbID,
			&t.Name,
			&t.Slug,
			&sortOrder,
			&t.State,
			&briefRaw,
			&heroImageID,
			&t.HeroURL,
			&t.Leading,
			&t.OgTitle,
			&t.OgDescription,
			&ogImageID,
			&t.IsFeatured,
			&t.TitleStyle,
			&t.Type,
			&t.Style,
			&t.Javascript,
			&t.Dfp,
			&t.MobileDfp,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		t.ID = strconv.Itoa(dbID)
		if sortOrder.Valid {
			val := int(sortOrder.Int64)
			t.SortOrder = &val
		}
		if createdAt.Valid {
			t.CreatedAt = createdAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updatedAt.Valid {
			t.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
		}
		t.Brief = decodeJSONBytes(briefRaw)
		t.Metadata = map[string]any{
			"heroImageID": nullableInt(heroImageID),
			"ogImageID":   nullableInt(ogImageID),
		}
		topics = append(topics, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(topics) == 0 {
		return topics, nil
	}
	if err := r.enrichTopics(ctx, topics); err != nil {
		return nil, err
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topics", map[string]interface{}{
			"where":  where,
			"orders": orders,
			"take":   take,
			"skip":   skip,
		})
		_ = r.cache.Set(ctx, cacheKey, topics)
	}

	return topics, nil
}

func (r *Repo) QueryTopicsCount(ctx context.Context, where *TopicWhereInput) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topicsCount", where)
		var cachedCount int
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedCount); found {
			return cachedCount, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT COUNT(*) FROM "Topic" t`)

	conds := []string{}
	args := []interface{}{}
	argIdx := 1

	buildStringFilter := func(field string, f *StringFilter) {
		if f == nil {
			return
		}
		if f.Equals != nil {
			conds = append(conds, fmt.Sprintf(`%s = $%d`, field, argIdx))
			args = append(args, *f.Equals)
			argIdx++
		}
	}

	if where != nil {
		buildStringFilter("slug", where.Slug)
		buildStringFilter("name", where.Name)
		buildStringFilter("state", where.State)
		buildStringFilter("type", where.Type)
		buildStringFilter("style", where.Style)
		if where.IsFeatured != nil && where.IsFeatured.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"isFeatured" = $%d`, argIdx))
			args = append(args, *where.IsFeatured.Equals)
			argIdx++
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	var count int
	if err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(&count); err != nil {
		return 0, err
	}

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topicsCount", where)
		_ = r.cache.Set(ctx, cacheKey, count)
	}

	return count, nil
}

func (r *Repo) QueryTopicByUnique(ctx context.Context, where *TopicWhereUniqueInput) (*Topic, error) {
	if where == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 嘗試從 cache 讀取
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topic:unique", where)
		var cachedTopic *Topic
		if found, _ := r.cache.Get(ctx, cacheKey, &cachedTopic); found {
			return cachedTopic, nil
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT id, name, slug, "sortOrder", state, brief, "heroImage", "heroUrl", "leading", "og_title", "og_description", "og_image", "isFeatured", "title_style", type, style, javascript, dfp, "mobile_dfp", "createdAt", "updatedAt" FROM "Topic" t WHERE `)
	args := []interface{}{}
	argIdx := 1
	if where.ID != nil {
		sb.WriteString(fmt.Sprintf("id = $%d", argIdx))
		args = append(args, *where.ID)
		argIdx++
	} else if where.Slug != nil {
		sb.WriteString(fmt.Sprintf("slug = $%d", argIdx))
		args = append(args, *where.Slug)
		argIdx++
	} else if where.Name != nil {
		sb.WriteString(fmt.Sprintf("name = $%d", argIdx))
		args = append(args, *where.Name)
		argIdx++
	} else {
		return nil, nil
	}
	sb.WriteString(" LIMIT 1")

	var (
		t           Topic
		dbID        int
		sortOrder   sql.NullInt64
		heroImageID sql.NullInt64
		ogImageID   sql.NullInt64
		briefRaw    []byte
		createdAt   sql.NullTime
		updatedAt   sql.NullTime
	)

	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(
		&dbID,
		&t.Name,
		&t.Slug,
		&sortOrder,
		&t.State,
		&briefRaw,
		&heroImageID,
		&t.HeroURL,
		&t.Leading,
		&t.OgTitle,
		&t.OgDescription,
		&ogImageID,
		&t.IsFeatured,
		&t.TitleStyle,
		&t.Type,
		&t.Style,
		&t.Javascript,
		&t.Dfp,
		&t.MobileDfp,
		&createdAt,
		&updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.ID = strconv.Itoa(dbID)
	if sortOrder.Valid {
		val := int(sortOrder.Int64)
		t.SortOrder = &val
	}
	if createdAt.Valid {
		t.CreatedAt = createdAt.Time.UTC().Format(timeLayoutMilli)
	}
	if updatedAt.Valid {
		t.UpdatedAt = updatedAt.Time.UTC().Format(timeLayoutMilli)
	}
	t.Brief = decodeJSONBytes(briefRaw)
	t.Metadata = map[string]any{
		"heroImageID": nullableInt(heroImageID),
		"ogImageID":   nullableInt(ogImageID),
	}

	topics := []Topic{t}
	if err := r.enrichTopics(ctx, topics); err != nil {
		return nil, err
	}
	t = topics[0]

	// 寫入 cache
	if r.cache != nil && r.cache.Enabled() {
		cacheKey := GenerateCacheKey("topic:unique", where)
		_ = r.cache.Set(ctx, cacheKey, &t)
	}

	return &t, nil
}

// Internal helpers
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

func ensurePostPublished(where *PostWhereInput) *PostWhereInput {
	if where == nil {
		where = &PostWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: ptrString("published")}
	}
	return where
}

func ensureExternalPublished(where *ExternalWhereInput) *ExternalWhereInput {
	if where == nil {
		where = &ExternalWhereInput{}
	}
	if where.State == nil {
		where.State = &StringFilter{Equals: ptrString("published")}
	}
	return where
}

func ptrString(s string) *string { return &s }

func decodeJSONBytes(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func nullableInt(v sql.NullInt64) int {
	if v.Valid {
		return int(v.Int64)
	}
	return 0
}

func getMetaInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		}
	}
	return 0
}

func buildOrderClause(rule OrderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`"publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`"updatedAt" %s`, dir)
	case "title":
		return fmt.Sprintf(`"title" %s`, dir)
	default:
		return `"publishedDate" DESC`
	}
}

func buildExternalOrder(rule OrderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}
	switch rule.Field {
	case "publishedDate":
		return fmt.Sprintf(`e."publishedDate" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`e."updatedAt" %s`, dir)
	default:
		return `e."publishedDate" DESC`
	}
}

func buildTopicOrderClause(rule OrderRule) string {
	dir := strings.ToUpper(rule.Direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "ASC"
	}
	switch rule.Field {
	case "sortOrder":
		return fmt.Sprintf(`"sortOrder" %s NULLS LAST`, dir)
	case "createdAt":
		return fmt.Sprintf(`"createdAt" %s`, dir)
	case "updatedAt":
		return fmt.Sprintf(`"updatedAt" %s`, dir)
	case "name":
		return fmt.Sprintf(`name %s`, dir)
	case "slug":
		return fmt.Sprintf(`slug %s`, dir)
	default:
		return `"sortOrder" ASC NULLS LAST, "createdAt" DESC`
	}
}

func (r *Repo) enrichPosts(ctx context.Context, posts []Post) error {
	if len(posts) == 0 {
		return nil
	}
	postIDs := make([]int, 0, len(posts))
	for _, p := range posts {
		id, _ := strconv.Atoi(p.ID)
		if id == 0 {
			continue
		}
		postIDs = append(postIDs, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	sectionsMap, err := r.fetchSections(ctx, postIDs)
	if err != nil {
		return err
	}
	categoriesMap, err := r.fetchCategories(ctx, postIDs)
	if err != nil {
		return err
	}
	roleMapWriters, _ := r.fetchContacts(ctx, "_Post_writers", postIDs)
	roleMapPhotographers, _ := r.fetchContacts(ctx, "_Post_photographers", postIDs)
	roleMapCamera, _ := r.fetchContacts(ctx, "_Post_camera_man", postIDs)
	roleMapDesigners, _ := r.fetchContacts(ctx, "_Post_designers", postIDs)
	roleMapEngineers, _ := r.fetchContacts(ctx, "_Post_engineers", postIDs)
	roleMapVocals, _ := r.fetchContacts(ctx, "_Post_vocals", postIDs)

	tagsMap, _ := r.fetchTags(ctx, "_Post_tags", postIDs)
	tagsAlgoMap, _ := r.fetchTags(ctx, "_Post_tags_algo", postIDs)

	relatedsMap, relatedImageIDs, err := r.fetchRelatedPosts(ctx, postIDs)
	if err != nil {
		return err
	}
	imageIDs := append([]int{}, relatedImageIDs...)

	relatedOneIDs := []int{}
	relatedTwoIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "relatedsOneID"); id > 0 {
			relatedOneIDs = append(relatedOneIDs, id)
		}
		if id := getMetaInt(p.Metadata, "relatedsTwoID"); id > 0 {
			relatedTwoIDs = append(relatedTwoIDs, id)
		}
	}
	relatedSinglesIDs := append(relatedOneIDs, relatedTwoIDs...)
	relatedSinglePosts := map[int]Post{}
	if len(relatedSinglesIDs) > 0 {
		sps, imgIDs, err := r.fetchPostsByIDs(ctx, relatedSinglesIDs)
		if err != nil {
			return err
		}
		for _, sp := range sps {
			id, _ := strconv.Atoi(sp.ID)
			relatedSinglePosts[id] = sp
		}
		imageIDs = append(imageIDs, imgIDs...)
	}

	videoIDs := []int{}
	topicIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "heroVideoID"); id > 0 {
			videoIDs = append(videoIDs, id)
		}
		if id := getMetaInt(p.Metadata, "topicsID"); id > 0 {
			topicIDs = append(topicIDs, id)
		}
		if id := getMetaInt(p.Metadata, "heroImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
		if id := getMetaInt(p.Metadata, "ogImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
	}

	videoMap, videoImageIDs, _ := r.fetchVideos(ctx, videoIDs)
	imageIDs = append(imageIDs, videoImageIDs...)
	topicMap, _ := r.fetchTopics(ctx, topicIDs)
	imageMap, err := r.fetchImages(ctx, imageIDs)
	if err != nil {
		return err
	}

	for i := range posts {
		p := &posts[i]
		id, _ := strconv.Atoi(p.ID)
		p.Sections = sectionsMap[id]
		p.SectionsInInputOrder = sectionsMap[id]
		p.Categories = categoriesMap[id]
		p.CategoriesInInputOrder = categoriesMap[id]
		p.Writers = roleMapWriters[id]
		p.WritersInInputOrder = roleMapWriters[id]
		p.Photographers = roleMapPhotographers[id]
		p.CameraMan = roleMapCamera[id]
		p.Designers = roleMapDesigners[id]
		p.Engineers = roleMapEngineers[id]
		p.Vocals = roleMapVocals[id]
		p.Tags = tagsMap[id]
		p.TagsAlgo = tagsAlgoMap[id]
		p.Relateds = relatedsMap[id]
		p.RelatedsInInputOrder = relatedsMap[id]
		if idImg := getMetaInt(p.Metadata, "heroImageID"); idImg > 0 {
			p.HeroImage = imageMap[idImg]
		}
		if idImg := getMetaInt(p.Metadata, "ogImageID"); idImg > 0 {
			p.OgImage = imageMap[idImg]
		}
		if vid := getMetaInt(p.Metadata, "heroVideoID"); vid > 0 {
			p.HeroVideo = videoMap[vid]
		}
		if tid := getMetaInt(p.Metadata, "topicsID"); tid > 0 {
			if t, ok := topicMap[tid]; ok {
				p.Topics = &t
			}
		}
		if r1 := getMetaInt(p.Metadata, "relatedsOneID"); r1 > 0 {
			if rp, ok := relatedSinglePosts[r1]; ok {
				p.RelatedsOne = &rp
			}
		}
		if r2 := getMetaInt(p.Metadata, "relatedsTwoID"); r2 > 0 {
			if rp, ok := relatedSinglePosts[r2]; ok {
				p.RelatedsTwo = &rp
			}
		}
	}
	return nil
}

func (r *Repo) enrichTopics(ctx context.Context, topics []Topic) error {
	if len(topics) == 0 {
		return nil
	}
	topicIDs := make([]int, 0, len(topics))
	for _, t := range topics {
		id, _ := strconv.Atoi(t.ID)
		if id == 0 {
			continue
		}
		topicIDs = append(topicIDs, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 獲取 heroImage 和 og_image
	imageIDs := []int{}
	for _, t := range topics {
		if id := getMetaInt(t.Metadata, "heroImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
		if id := getMetaInt(t.Metadata, "ogImageID"); id > 0 {
			imageIDs = append(imageIDs, id)
		}
	}

	// 獲取 tags
	tagsMap, _ := r.fetchTopicTags(ctx, topicIDs)

	// 獲取 slideshow_images
	slideshowMap, slideshowImageIDs, _ := r.fetchTopicSlideshowImages(ctx, topicIDs)
	imageIDs = append(imageIDs, slideshowImageIDs...)

	// 獲取 images
	imageMap, err := r.fetchImages(ctx, imageIDs)
	if err != nil {
		return err
	}

	// 組裝資料
	for i := range topics {
		t := &topics[i]
		id, _ := strconv.Atoi(t.ID)

		// 設置 heroImage
		if idImg := getMetaInt(t.Metadata, "heroImageID"); idImg > 0 {
			t.HeroImage = imageMap[idImg]
		}

		// 設置 og_image
		if idImg := getMetaInt(t.Metadata, "ogImageID"); idImg > 0 {
			t.OgImage = imageMap[idImg]
		}

		// 設置 tags
		t.Tags = tagsMap[id]

		// 設置 slideshow_images
		t.SlideshowImages = slideshowMap[id]
		t.SlideshowImagesInOrder = slideshowMap[id]
	}
	return nil
}

func (r *Repo) fetchSections(ctx context.Context, postIDs []int) (map[int][]Section, error) {
	result := map[int][]Section{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT ps."A" as post_id, s.id, s.name, s.slug, s.state FROM "_Post_sections" ps JOIN "Section" s ON s.id = ps."B" WHERE ps."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var s Section
		if err := rows.Scan(&pid, &s.ID, &s.Name, &s.Slug, &s.State); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], s)
	}
	return result, rows.Err()
}

func (r *Repo) fetchCategories(ctx context.Context, postIDs []int) (map[int][]Category, error) {
	result := map[int][]Category{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `SELECT cp."B" as post_id, c.id, c.name, c.slug, c.state, c."isMemberOnly" FROM "_Category_posts" cp JOIN "Category" c ON c.id = cp."A" WHERE cp."B" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c Category
		if err := rows.Scan(&pid, &c.ID, &c.Name, &c.Slug, &c.State, &c.IsMemberOnly); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchContacts(ctx context.Context, table string, postIDs []int) (map[int][]Contact, error) {
	result := map[int][]Contact{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."B" as post_id, c.id, c.name FROM "%s" t JOIN "Contact" c ON c.id = t."A" WHERE t."B" = ANY($1)`, table)
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c Contact
		if err := rows.Scan(&pid, &c.ID, &c.Name); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchTags(ctx context.Context, table string, postIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := fmt.Sprintf(`SELECT t."A" as post_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table)
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var t Tag
		if err := rows.Scan(&pid, &t.ID, &t.Name, &t.Slug); err != nil {
			return result, err
		}
		result[pid] = append(result[pid], t)
	}
	return result, rows.Err()
}

func (r *Repo) fetchRelatedPosts(ctx context.Context, postIDs []int) (map[int][]Post, []int, error) {
	result := map[int][]Post{}
	imageIDs := []int{}
	if len(postIDs) == 0 {
		return result, imageIDs, nil
	}
	query := `
		SELECT r."A" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."B"
		WHERE r."A" = ANY($1)
		UNION
		SELECT r."B" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."A"
		WHERE r."B" = ANY($1)
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var rp Post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&pid, &dbID, &rp.Slug, &rp.Title, &heroID); err != nil {
			return result, imageIDs, err
		}
		rp.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			imageIDs = append(imageIDs, int(heroID.Int64))
			rp.Metadata = map[string]any{"heroImageID": int(heroID.Int64)}
		}
		result[pid] = append(result[pid], rp)
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchPostsByIDs(ctx context.Context, ids []int) ([]Post, []int, error) {
	result := []Post{}
	imageIDs := []int{}
	if len(ids) == 0 {
		return result, imageIDs, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Post
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &p.Slug, &p.Title, &hero); err != nil {
			return result, imageIDs, err
		}
		p.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			p.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result = append(result, p)
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchVideos(ctx context.Context, videoIDs []int) (map[int]*Video, []int, error) {
	result := map[int]*Video{}
	imageIDs := []int{}
	if len(videoIDs) == 0 {
		return result, imageIDs, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, "urlOriginal", "heroImage" FROM "Video" WHERE id = ANY($1)`, pqIntArray(videoIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var v Video
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&dbID, &v.VideoSrc, &hero); err != nil {
			return result, imageIDs, err
		}
		v.ID = strconv.Itoa(dbID)
		if hero.Valid {
			imageIDs = append(imageIDs, int(hero.Int64))
			v.HeroImage = &Photo{}
			v.HeroImage.ImageFile = ImageFile{}
			v.HeroImage.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result[dbID] = &v
	}
	return result, imageIDs, rows.Err()
}

func (r *Repo) fetchTopics(ctx context.Context, ids []int) (map[int]Topic, error) {
	result := map[int]Topic{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug FROM "Topic" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var t Topic
		if err := rows.Scan(&id, &t.Slug); err != nil {
			return result, err
		}
		result[id] = t
	}
	return result, rows.Err()
}

func (r *Repo) fetchImages(ctx context.Context, ids []int) (map[int]*Photo, error) {
	result := map[int]*Photo{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, COALESCE("imageFile_id", ''), COALESCE("imageFile_extension", ''), "imageFile_width", "imageFile_height" FROM "Image" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var im struct {
			id     int
			fileID string
			ext    string
			width  sql.NullInt64
			height sql.NullInt64
		}
		if err := rows.Scan(&im.id, &im.fileID, &im.ext, &im.width, &im.height); err != nil {
			return result, err
		}
		photo := Photo{
			ID: strconv.Itoa(im.id),
			ImageFile: ImageFile{
				Width:  int(im.width.Int64),
				Height: int(im.height.Int64),
			},
		}
		photo.Resized = r.buildResizedURLs(im.fileID, im.ext)
		photo.ResizedWebp = r.buildResizedURLs(im.fileID, "webp")
		result[im.id] = &photo
	}
	return result, rows.Err()
}

func (r *Repo) fetchPartners(ctx context.Context, ids []int) (map[int]*Partner, error) {
	result := map[int]*Partner{}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, name, "showOnIndex", COALESCE("showThumb", true), COALESCE("showBrief", false) FROM "Partner" WHERE id = ANY($1)`, pqIntArray(ids))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Partner
		var dbID int
		if err := rows.Scan(&dbID, &p.Slug, &p.Name, &p.ShowOnIndex, &p.ShowThumb, &p.ShowBrief); err != nil {
			return result, err
		}
		p.ID = strconv.Itoa(dbID)
		result[dbID] = &p
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalTags(ctx context.Context, table string, externalIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`SELECT t."A" as external_id, tg.id, tg.name, tg.slug FROM "%s" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`, table), pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var tg Tag
		if err := rows.Scan(&eid, &tg.ID, &tg.Name, &tg.Slug); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], tg)
	}
	return result, rows.Err()
}

func (r *Repo) fetchTopicTags(ctx context.Context, topicIDs []int) (map[int][]Tag, error) {
	result := map[int][]Tag{}
	if len(topicIDs) == 0 {
		return result, nil
	}
	query := `SELECT t."A" as topic_id, tg.id, tg.name, tg.slug FROM "Tag_topics" t JOIN "Tag" tg ON tg.id = t."B" WHERE t."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var tid int
		var tg Tag
		if err := rows.Scan(&tid, &tg.ID, &tg.Name, &tg.Slug); err != nil {
			return result, err
		}
		result[tid] = append(result[tid], tg)
	}
	return result, rows.Err()
}

func (r *Repo) fetchTopicSlideshowImages(ctx context.Context, topicIDs []int) (map[int][]Photo, []int, error) {
	result := map[int][]Photo{}
	imageIDs := []int{}
	if len(topicIDs) == 0 {
		return result, imageIDs, nil
	}
	query := `SELECT t."A" as topic_id, im.id, COALESCE(im."imageFile_id", ''), COALESCE(im."imageFile_extension", ''), im."imageFile_width", im."imageFile_height", COALESCE(im.name, '') as name, COALESCE(im."topicKeywords", '') as topicKeywords FROM "Topic_slideshow_images" t JOIN "Image" im ON im.id = t."B" WHERE t."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(topicIDs))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()
	for rows.Next() {
		var tid int
		var im struct {
			id            int
			fileID        string
			ext           string
			width         sql.NullInt64
			height        sql.NullInt64
			name          string
			topicKeywords string
		}
		if err := rows.Scan(&tid, &im.id, &im.fileID, &im.ext, &im.width, &im.height, &im.name, &im.topicKeywords); err != nil {
			return result, imageIDs, err
		}
		imageIDs = append(imageIDs, im.id)
		photo := Photo{
			ID: strconv.Itoa(im.id),
			ImageFile: ImageFile{
				Width:  int(im.width.Int64),
				Height: int(im.height.Int64),
			},
		}
		photo.Resized = r.buildResizedURLs(im.fileID, im.ext)
		photo.ResizedWebp = r.buildResizedURLs(im.fileID, "webp")
		photo.Metadata = map[string]any{
			"name":          im.name,
			"topicKeywords": im.topicKeywords,
		}
		result[tid] = append(result[tid], photo)
	}
	return result, imageIDs, rows.Err()
}

func pqIntArray(ids []int) interface{} {
	arr := make([]int64, len(ids))
	for i, id := range ids {
		arr[i] = int64(id)
	}
	return arr
}

func (r *Repo) buildResizedURLs(fileID, ext string) Resized {
	if fileID == "" {
		return Resized{}
	}
	if ext == "" {
		ext = "jpg"
	}
	host := r.staticsHost
	makeURL := func(size string, extension string) string {
		if size == "" {
			return fmt.Sprintf("%s/%s.%s", host, fileID, extension)
		}
		return fmt.Sprintf("%s/%s-%s.%s", host, fileID, size, extension)
	}
	return Resized{
		Original: makeURL("", ext),
		W480:     makeURL("w480", ext),
		W800:     makeURL("w800", ext),
		W1200:    makeURL("w1200", ext),
		W1600:    makeURL("w1600", ext),
		W2400:    makeURL("w2400", ext),
	}
}
