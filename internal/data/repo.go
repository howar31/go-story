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
	"golang.org/x/sync/errgroup"
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
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	TopicKeywords string         `json:"topicKeywords"`
	ImageFile     ImageFile      `json:"imageFile"`
	Resized       Resized        `json:"resized"`
	ResizedWebp   Resized        `json:"resizedWebp"`
	Metadata      map[string]any `json:"-"`
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

type Group struct {
	ID      string `json:"id"`
	Keyword string `json:"keyword"`
}

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
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
	ID                     string           `json:"id"`
	Slug                   string           `json:"slug"`
	Title                  string           `json:"title"`
	Subtitle               string           `json:"subtitle"`
	State                  string           `json:"state"`
	Style                  string           `json:"style"`
	PublishedDate          string           `json:"publishedDate"`
	UpdatedAt              string           `json:"updatedAt"`
	IsMember               bool             `json:"isMember"`
	IsAdult                bool             `json:"isAdult"`
	Sections               []Section        `json:"sections"`
	SectionsInInputOrder   []Section        `json:"sectionsInInputOrder"`
	Categories             []Category       `json:"categories"`
	CategoriesInInputOrder []Category       `json:"categoriesInInputOrder"`
	Writers                []Contact        `json:"writers"`
	WritersInInputOrder    []Contact        `json:"writersInInputOrder"`
	Photographers          []Contact        `json:"photographers"`
	CameraMan              []Contact        `json:"camera_man"`
	Designers              []Contact        `json:"designers"`
	Engineers              []Contact        `json:"engineers"`
	Vocals                 []Contact        `json:"vocals"`
	ExtendByline           string           `json:"extend_byline"`
	Tags                   []Tag            `json:"tags"`
	TagsAlgo               []Tag            `json:"tags_algo"`
	HeroVideo              *Video           `json:"heroVideo"`
	HeroImage              *Photo           `json:"heroImage"`
	HeroCaption            string           `json:"heroCaption"`
	Brief                  map[string]any   `json:"brief"`
	TrimmedContent         map[string]any   `json:"trimmedContent"`
	Content                map[string]any   `json:"content"`
	Relateds               []Post           `json:"relateds"`
	RelatedsInInputOrder   []Post           `json:"relatedsInInputOrder"`
	RelatedsOne            *Post            `json:"relatedsOne"`
	RelatedsTwo            *Post            `json:"relatedsTwo"`
	Redirect               string           `json:"redirect"`
	OgTitle                string           `json:"og_title"`
	OgImage                *Photo           `json:"og_image"`
	OgDescription          string           `json:"og_description"`
	HiddenAdvertised       bool             `json:"hiddenAdvertised"`
	IsAdvertised           bool             `json:"isAdvertised"`
	IsFeatured                 bool             `json:"isFeatured"`
	AutoFAQ                    bool             `json:"auto_faq"`
	FAQsAlgo                   any              `json:"faqs_algo"`
	Topics                     *Topic           `json:"topics"`
	RelatedVideos              []Video          `json:"related_videos"`
	RelatedVideosInInputOrder  []Video          `json:"relatedVideosInInputOrder"`
	ManualOrderOfSections      []map[string]any `json:"manualOrderOfSections"`
	ManualOrderOfCategories    []map[string]any `json:"manualOrderOfCategories"`
	ManualOrderOfWriters       []map[string]any `json:"manualOrderOfWriters"`
	ManualOrderOfRelateds      []map[string]any `json:"manualOrderOfRelateds"`
	ManualOrderOfRelatedVideos []map[string]any `json:"manualOrderOfRelatedVideos"`
	Metadata                   map[string]any   `json:"-"`
}

type External struct {
	ID                  string         `json:"id"`
	Slug                string         `json:"slug"`
	Partner             *Partner       `json:"partner"`
	Title               string         `json:"title"`
	State               string         `json:"state"`
	PublishedDate       string         `json:"publishedDate"`
	PublishedDateString string         `json:"publishedDateString"`
	ExtendByline        string         `json:"extend_byline"`
	Thumb               string         `json:"thumb"`
	ThumbCaption        string         `json:"thumbCaption"`
	Brief               string         `json:"brief"`
	Content             string         `json:"content"`
	Source              string         `json:"source"`
	Tags                []Tag          `json:"tags"`
	TagsAlgo            []Tag          `json:"tags_algo"`
	Sections            []Section      `json:"sections"`
	Categories          []Category     `json:"categories"`
	Relateds            []Post         `json:"relateds"`
	Groups              []Group        `json:"groups"`
	CreatedAt           string         `json:"createdAt"`
	UpdatedAt           string         `json:"updatedAt"`
	CreatedBy           *User          `json:"createdBy"`
	UpdatedBy           *User          `json:"updatedBy"`
	Metadata            map[string]any `json:"metadata"`
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
	AutoFAQ    *BooleanFilter              `mapstructure:"auto_faq"`
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

func NewDB(dsn string, maxOpenConns, maxIdleConns int, connMaxIdleTime time.Duration) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn := stdlib.OpenDB(*cfg)
	if maxOpenConns <= 0 {
		maxOpenConns = 20
	}
	if maxIdleConns <= 0 {
		maxIdleConns = 10
	}
	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 5 * time.Minute
	}
	conn.SetMaxOpenConns(maxOpenConns)
	conn.SetMaxIdleConns(maxIdleConns)
	conn.SetConnMaxIdleTime(connMaxIdleTime)
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
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo", "manualOrderOfSections", "manualOrderOfCategories", "manualOrderOfWriters", "manualOrderOfRelateds", "manualOrderOfRelatedVideos", "auto_faq", "faqs_algo" FROM "Post" p`)

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
		if where.AutoFAQ != nil && where.AutoFAQ.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"auto_faq" = $%d`, argIdx))
			args = append(args, *where.AutoFAQ.Equals)
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
			p                        Post
			dbID                     int
			publishedAt              sql.NullTime
			updatedAt                sql.NullTime
			heroImageID              sql.NullInt64
			heroVideoID              sql.NullInt64
			ogImageID                sql.NullInt64
			topicsID                 sql.NullInt64
			relatedsOneID            sql.NullInt64
			relatedsTwoID            sql.NullInt64
			briefRaw                 []byte
			contentRaw               []byte
			manualOrderOfSectionsRaw      []byte
			manualOrderOfCategoriesRaw    []byte
			manualOrderOfWritersRaw       []byte
			manualOrderOfRelatedsRaw      []byte
			manualOrderOfRelatedVideosRaw []byte
			autoFAQ                  bool
			faqsAlgoRaw              []byte
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
			&manualOrderOfSectionsRaw,
			&manualOrderOfCategoriesRaw,
			&manualOrderOfWritersRaw,
			&manualOrderOfRelatedsRaw,
			&manualOrderOfRelatedVideosRaw,
			&autoFAQ,
			&faqsAlgoRaw,
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
		p.ManualOrderOfSections = decodeJSONArray(manualOrderOfSectionsRaw)
		p.ManualOrderOfCategories = decodeJSONArray(manualOrderOfCategoriesRaw)
		p.ManualOrderOfWriters = decodeJSONArray(manualOrderOfWritersRaw)
		p.ManualOrderOfRelateds = decodeJSONArray(manualOrderOfRelatedsRaw)
		p.ManualOrderOfRelatedVideos = decodeJSONArray(manualOrderOfRelatedVideosRaw)
		p.AutoFAQ = autoFAQ
		p.FAQsAlgo = decodeJSONAny(faqsAlgoRaw)
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
		if where.AutoFAQ != nil && where.AutoFAQ.Equals != nil {
			conds = append(conds, fmt.Sprintf(`"auto_faq" = $%d`, argIdx))
			args = append(args, *where.AutoFAQ.Equals)
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
	sb.WriteString(`SELECT id, slug, title, subtitle, state, style, "isMember", "isAdult", "publishedDate", "updatedAt", COALESCE("heroCaption",'') as heroCaption, COALESCE("extend_byline",'') as extend_byline, "heroImage", "heroVideo", brief, content, COALESCE(redirect,'') as redirect, COALESCE(og_title,'') as og_title, COALESCE(og_description,'') as og_description, "hiddenAdvertised", "isAdvertised", "isFeatured", topics, "og_image", "relatedsOne", "relatedsTwo", "manualOrderOfSections", "manualOrderOfCategories", "manualOrderOfWriters", "manualOrderOfRelateds", "manualOrderOfRelatedVideos", "auto_faq", "faqs_algo" FROM "Post" p WHERE `)
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
	// 只回傳 state = 'published' 或 'invisible' 的文章
	sb.WriteString(" AND state IN ('published', 'invisible')")
	sb.WriteString(" LIMIT 1")

	var (
		p                        Post
		dbID                     int
		publishedAt              sql.NullTime
		updatedAt                sql.NullTime
		heroImageID              sql.NullInt64
		heroVideoID              sql.NullInt64
		ogImageID                sql.NullInt64
		topicsID                 sql.NullInt64
		relatedsOneID            sql.NullInt64
		relatedsTwoID            sql.NullInt64
		briefRaw                 []byte
		contentRaw               []byte
		manualOrderOfSectionsRaw      []byte
		manualOrderOfCategoriesRaw    []byte
		manualOrderOfWritersRaw       []byte
		manualOrderOfRelatedsRaw      []byte
		manualOrderOfRelatedVideosRaw []byte
		autoFAQ                  bool
		faqsAlgoRaw              []byte
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
		&manualOrderOfSectionsRaw,
		&manualOrderOfCategoriesRaw,
		&manualOrderOfWritersRaw,
		&manualOrderOfRelatedsRaw,
		&manualOrderOfRelatedVideosRaw,
		&autoFAQ,
		&faqsAlgoRaw,
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
	p.ManualOrderOfSections = decodeJSONArray(manualOrderOfSectionsRaw)
	p.ManualOrderOfCategories = decodeJSONArray(manualOrderOfCategoriesRaw)
	p.ManualOrderOfWriters = decodeJSONArray(manualOrderOfWritersRaw)
	p.ManualOrderOfRelateds = decodeJSONArray(manualOrderOfRelatedsRaw)
	p.ManualOrderOfRelatedVideos = decodeJSONArray(manualOrderOfRelatedVideosRaw)
	p.AutoFAQ = autoFAQ
	p.FAQsAlgo = decodeJSONAny(faqsAlgoRaw)
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
	sb.WriteString(`SELECT e.id, e.slug, e.title, e.state, e."publishedDate", e."publishedDateString", e."extend_byline", e.thumb, e."thumbCaption", e.brief, e.content, e.source, e.partner, e."createdAt", e."updatedAt" FROM "External" e`)

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
		var pubAt, createdAt, updAt sql.NullTime
		var publishedDateString sql.NullString
		if err := rows.Scan(&dbID, &ext.Slug, &ext.Title, &ext.State, &pubAt, &publishedDateString, &ext.ExtendByline, &ext.Thumb, &ext.ThumbCaption, &ext.Brief, &ext.Content, &ext.Source, &partnerID, &createdAt, &updAt); err != nil {
			return nil, err
		}
		ext.ID = strconv.Itoa(dbID)
		if pubAt.Valid {
			ext.PublishedDate = pubAt.Time.UTC().Format(timeLayoutMilli)
		}
		if publishedDateString.Valid {
			ext.PublishedDateString = publishedDateString.String
		}
		if createdAt.Valid {
			ext.CreatedAt = createdAt.Time.UTC().Format(timeLayoutMilli)
		}
		if updAt.Valid {
			ext.UpdatedAt = updAt.Time.UTC().Format(timeLayoutMilli)
		}
		externalIDs = append(externalIDs, dbID)
		if partnerID.Valid {
			if ext.Metadata == nil {
				ext.Metadata = map[string]any{}
			}
			ext.Metadata["partnerID"] = int(partnerID.Int64)
			partnerIDs = append(partnerIDs, int(partnerID.Int64))
		}
		result = append(result, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	partners, _ := r.fetchPartners(ctx, partnerIDs)
	tagsMap, _ := r.fetchExternalTags(ctx, "_External_tags", externalIDs)
	tagsAlgoMap, _ := r.fetchExternalTags(ctx, "_External_tags_algo", externalIDs)
	sectionsMap, _ := r.fetchExternalSections(ctx, externalIDs)
	categoriesMap, _ := r.fetchExternalCategories(ctx, externalIDs)
	groupsMap, _ := r.fetchExternalGroups(ctx, externalIDs)
	relatedsMap, _ := r.fetchExternalRelateds(ctx, externalIDs)
	relatedImageIDs := []int{}
	for _, relateds := range relatedsMap {
		for _, rp := range relateds {
			if idImg := getMetaInt(rp.Metadata, "heroImageID"); idImg > 0 {
				relatedImageIDs = append(relatedImageIDs, idImg)
			}
		}
	}
	relatedImageMap := map[int]*Photo{}
	if len(relatedImageIDs) > 0 {
		var err error
		relatedImageMap, err = r.fetchImages(ctx, relatedImageIDs)
		if err != nil {
			return nil, err
		}
	}
	for i := range result {
		if pid := getMetaInt(result[i].Metadata, "partnerID"); pid > 0 {
			result[i].Partner = partners[pid]
		}
		idInt, _ := strconv.Atoi(result[i].ID)
		result[i].Tags = tagsMap[idInt]
		result[i].TagsAlgo = tagsAlgoMap[idInt]
		result[i].Sections = sectionsMap[idInt]
		result[i].Categories = categoriesMap[idInt]
		result[i].Groups = groupsMap[idInt]
		relateds := relatedsMap[idInt]
		for j := range relateds {
			if idImg := getMetaInt(relateds[j].Metadata, "heroImageID"); idImg > 0 {
				relateds[j].HeroImage = relatedImageMap[idImg]
			}
		}
		result[i].Relateds = relateds
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
		cacheKey := GenerateCacheKey("topics:v2", map[string]interface{}{
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
			heroURL     sql.NullString
			leading     sql.NullString
			ogTitle     sql.NullString
			ogDesc      sql.NullString
			titleStyle  sql.NullString
			typeVal     sql.NullString
			styleVal    sql.NullString
			javascript  sql.NullString
			dfp         sql.NullString
			mobileDfp   sql.NullString
		)
		if err := rows.Scan(
			&dbID,
			&t.Name,
			&t.Slug,
			&sortOrder,
			&t.State,
			&briefRaw,
			&heroImageID,
			&heroURL,
			&leading,
			&ogTitle,
			&ogDesc,
			&ogImageID,
			&t.IsFeatured,
			&titleStyle,
			&typeVal,
			&styleVal,
			&javascript,
			&dfp,
			&mobileDfp,
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
		if heroURL.Valid {
			t.HeroURL = heroURL.String
		}
		if leading.Valid {
			t.Leading = leading.String
		}
		if ogTitle.Valid {
			t.OgTitle = ogTitle.String
		}
		if ogDesc.Valid {
			t.OgDescription = ogDesc.String
		}
		if titleStyle.Valid {
			t.TitleStyle = titleStyle.String
		}
		if typeVal.Valid {
			t.Type = typeVal.String
		}
		if styleVal.Valid {
			t.Style = styleVal.String
		}
		if javascript.Valid {
			t.Javascript = javascript.String
		}
		if dfp.Valid {
			t.Dfp = dfp.String
		}
		if mobileDfp.Valid {
			t.MobileDfp = mobileDfp.String
		}
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
		cacheKey := GenerateCacheKey("topicsCount:v2", where)
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
		cacheKey := GenerateCacheKey("topicsCount:v2", where)
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
		cacheKey := GenerateCacheKey("topic:unique:v2", where)
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
		heroURL     sql.NullString
		leading     sql.NullString
		ogTitle     sql.NullString
		ogDesc      sql.NullString
		titleStyle  sql.NullString
		typeVal     sql.NullString
		styleVal    sql.NullString
		javascript  sql.NullString
		dfp         sql.NullString
		mobileDfp   sql.NullString
	)

	err := r.db.QueryRowContext(ctx, sb.String(), args...).Scan(
		&dbID,
		&t.Name,
		&t.Slug,
		&sortOrder,
		&t.State,
		&briefRaw,
		&heroImageID,
		&heroURL,
		&leading,
		&ogTitle,
		&ogDesc,
		&ogImageID,
		&t.IsFeatured,
		&titleStyle,
		&typeVal,
		&styleVal,
		&javascript,
		&dfp,
		&mobileDfp,
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
	if heroURL.Valid {
		t.HeroURL = heroURL.String
	}
	if leading.Valid {
		t.Leading = leading.String
	}
	if ogTitle.Valid {
		t.OgTitle = ogTitle.String
	}
	if ogDesc.Valid {
		t.OgDescription = ogDesc.String
	}
	if titleStyle.Valid {
		t.TitleStyle = titleStyle.String
	}
	if typeVal.Valid {
		t.Type = typeVal.String
	}
	if styleVal.Valid {
		t.Style = styleVal.String
	}
	if javascript.Valid {
		t.Javascript = javascript.String
	}
	if dfp.Valid {
		t.Dfp = dfp.String
	}
	if mobileDfp.Valid {
		t.MobileDfp = mobileDfp.String
	}
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
		where.State = &StringFilter{In: []string{"published", "invisible"}}
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

func decodeJSONArray(raw []byte) []map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil
	}
	return arr
}

// decodeJSONAny decodes a raw JSON value without presuming object or array shape.
// Used for generic JSON columns (e.g. Post.faqs_algo) whose structure is defined
// externally and may be either object or array.
func decodeJSONAny(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}

func orderSectionsByManual(sections []Section, manual []map[string]any) []Section {
	if len(manual) == 0 {
		return sections
	}
	byID := make(map[string]Section, len(sections))
	for _, s := range sections {
		byID[s.ID] = s
	}
	out := make([]Section, 0, len(manual))
	for _, item := range manual {
		if idStr, ok := item["id"].(string); ok {
			if s, ok := byID[idStr]; ok {
				out = append(out, s)
			}
		}
	}
	return out
}

func orderCategoriesByManual(categories []Category, manual []map[string]any) []Category {
	if len(manual) == 0 {
		return categories
	}
	byID := make(map[string]Category, len(categories))
	for _, c := range categories {
		byID[c.ID] = c
	}
	out := make([]Category, 0, len(manual))
	for _, item := range manual {
		if idStr, ok := item["id"].(string); ok {
			if c, ok := byID[idStr]; ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func orderContactsByManual(contacts []Contact, manual []map[string]any) []Contact {
	if len(manual) == 0 {
		return contacts
	}
	byID := make(map[string]Contact, len(contacts))
	for _, c := range contacts {
		byID[c.ID] = c
	}
	out := make([]Contact, 0, len(manual))
	for _, item := range manual {
		if idStr, ok := item["id"].(string); ok {
			if c, ok := byID[idStr]; ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func orderVideosByManual(videos []Video, manual []map[string]any) []Video {
	if len(manual) == 0 {
		return videos
	}
	byID := make(map[string]Video, len(videos))
	for _, v := range videos {
		byID[v.ID] = v
	}
	out := make([]Video, 0, len(manual))
	for _, item := range manual {
		if idStr, ok := item["id"].(string); ok {
			if v, ok := byID[idStr]; ok {
				out = append(out, v)
			}
		}
	}
	return out
}

func mergeVideosFromMap(list []Video, videoMap map[int]*Video) []Video {
	if len(list) == 0 {
		return []Video{}
	}
	out := make([]Video, len(list))
	for i, v := range list {
		if id, err := strconv.Atoi(v.ID); err == nil {
			if full, ok := videoMap[id]; ok && full != nil {
				out[i] = *full
				continue
			}
		}
		out[i] = v
	}
	return out
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
	case "auto_faq":
		return fmt.Sprintf(`"auto_faq" %s`, dir)
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

	var g errgroup.Group
	var (
		sectionsMap          map[int][]Section
		categoriesMap        map[int][]Category
		roleMapWriters       map[int][]Contact
		roleMapPhotographers map[int][]Contact
		roleMapCamera        map[int][]Contact
		roleMapDesigners     map[int][]Contact
		roleMapEngineers     map[int][]Contact
		roleMapVocals        map[int][]Contact
		roleMaps             map[string]map[int][]Contact
		tagsMap              map[int][]Tag
		tagsAlgoMap          map[int][]Tag
		relatedsMap          map[int][]Post
		relatedImageIDs      []int
		relatedVideosMap     map[int][]Video
	)
	g.Go(func() error {
		var err error
		sectionsMap, err = r.fetchSections(ctx, postIDs)
		return err
	})
	g.Go(func() error {
		var err error
		categoriesMap, err = r.fetchCategories(ctx, postIDs)
		return err
	})
	g.Go(func() error {
		roleMaps, _ = r.fetchContactsByRole(ctx, postIDs)
		return nil
	})
	g.Go(func() error {
		tagsMap, _ = r.fetchTags(ctx, "_Post_tags", postIDs)
		return nil
	})
	g.Go(func() error {
		tagsAlgoMap, _ = r.fetchTags(ctx, "_Post_tags_algo", postIDs)
		return nil
	})
	g.Go(func() error {
		var err error
		relatedsMap, relatedImageIDs, err = r.fetchRelatedPosts(ctx, postIDs)
		return err
	})
	g.Go(func() error {
		var err error
		relatedVideosMap, err = r.fetchRelatedVideosForPosts(ctx, postIDs)
		return err
	})
	if err := g.Wait(); err != nil {
		return err
	}
	if relatedVideosMap == nil {
		relatedVideosMap = map[int][]Video{}
	}
	if roleMaps == nil {
		roleMaps = map[string]map[int][]Contact{}
	}
	roleMapWriters = roleMaps["_Post_writers"]
	roleMapPhotographers = roleMaps["_Post_photographers"]
	roleMapCamera = roleMaps["_Post_camera_man"]
	roleMapDesigners = roleMaps["_Post_designers"]
	roleMapEngineers = roleMaps["_Post_engineers"]
	roleMapVocals = roleMaps["_Post_vocals"]
	imageIDs := append([]int{}, relatedImageIDs...)

	// Fetch relatedsInInputOrder based on manualOrderOfRelateds for each post
	relatedsInInputOrderMap := make(map[int][]Post)
	relatedsInInputOrderImageIDs := []int{}
	manualOrders := map[int][]int{}
	uniqueRelatedIDs := map[int]struct{}{}
	for _, p := range posts {
		postID, _ := strconv.Atoi(p.ID)
		if len(p.ManualOrderOfRelateds) == 0 {
			continue
		}
		ids := []int{}
		for _, item := range p.ManualOrderOfRelateds {
			if idStr, ok := item["id"].(string); ok {
				if id, err := strconv.Atoi(idStr); err == nil {
					ids = append(ids, id)
					uniqueRelatedIDs[id] = struct{}{}
				}
			}
		}
		if len(ids) > 0 {
			manualOrders[postID] = ids
		}
	}
	relatedOneIDs := []int{}
	relatedTwoIDs := []int{}
	for _, p := range posts {
		if id := getMetaInt(p.Metadata, "relatedsOneID"); id > 0 {
			relatedOneIDs = append(relatedOneIDs, id)
			uniqueRelatedIDs[id] = struct{}{}
		}
		if id := getMetaInt(p.Metadata, "relatedsTwoID"); id > 0 {
			relatedTwoIDs = append(relatedTwoIDs, id)
			uniqueRelatedIDs[id] = struct{}{}
		}
	}
	relatedSinglePosts := map[int]Post{}

	if len(uniqueRelatedIDs) > 0 {
		uniqueIDs := make([]int, 0, len(uniqueRelatedIDs))
		for id := range uniqueRelatedIDs {
			uniqueIDs = append(uniqueIDs, id)
		}
		rows, err := r.db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1) AND state IN ('published', 'invisible')`, pqIntArray(uniqueIDs))
		if err != nil {
			return err
		}
		postsMap := make(map[int]Post)
		for rows.Next() {
			var p Post
			var dbID int
			var hero sql.NullInt64
			if err := rows.Scan(&dbID, &p.Slug, &p.Title, &hero); err != nil {
				rows.Close()
				return err
			}
			p.ID = strconv.Itoa(dbID)
			if hero.Valid {
				relatedsInInputOrderImageIDs = append(relatedsInInputOrderImageIDs, int(hero.Int64))
				p.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
			}
			postsMap[dbID] = p
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		_ = rows.Close()

		for postID, order := range manualOrders {
			ordered := []Post{}
			for _, id := range order {
				if p, ok := postsMap[id]; ok {
					ordered = append(ordered, p)
				}
			}
			relatedsInInputOrderMap[postID] = ordered
		}

		for _, id := range relatedOneIDs {
			if p, ok := postsMap[id]; ok {
				relatedSinglePosts[id] = p
			}
		}
		for _, id := range relatedTwoIDs {
			if p, ok := postsMap[id]; ok {
				relatedSinglePosts[id] = p
			}
		}
	}
	imageIDs = append(imageIDs, relatedsInInputOrderImageIDs...)

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
	for _, vlist := range relatedVideosMap {
		for _, v := range vlist {
			if id, err := strconv.Atoi(v.ID); err == nil && id > 0 {
				videoIDs = append(videoIDs, id)
			}
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
		if len(p.ManualOrderOfSections) > 0 {
			p.SectionsInInputOrder = orderSectionsByManual(sectionsMap[id], p.ManualOrderOfSections)
		} else {
			p.SectionsInInputOrder = sectionsMap[id]
		}
		p.Categories = categoriesMap[id]
		if len(p.ManualOrderOfCategories) > 0 {
			p.CategoriesInInputOrder = orderCategoriesByManual(categoriesMap[id], p.ManualOrderOfCategories)
		} else {
			p.CategoriesInInputOrder = categoriesMap[id]
		}
		p.Writers = roleMapWriters[id]
		if len(p.ManualOrderOfWriters) > 0 {
			p.WritersInInputOrder = orderContactsByManual(roleMapWriters[id], p.ManualOrderOfWriters)
		} else {
			p.WritersInInputOrder = roleMapWriters[id]
		}
		p.Photographers = roleMapPhotographers[id]
		p.CameraMan = roleMapCamera[id]
		p.Designers = roleMapDesigners[id]
		p.Engineers = roleMapEngineers[id]
		p.Vocals = roleMapVocals[id]
		p.Tags = tagsMap[id]
		p.TagsAlgo = tagsAlgoMap[id]
		p.Relateds = relatedsMap[id]
		p.RelatedsInInputOrder = relatedsInInputOrderMap[id]
		if p.RelatedsInInputOrder == nil {
			p.RelatedsInInputOrder = []Post{}
		}
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
				if idImg := getMetaInt(rp.Metadata, "heroImageID"); idImg > 0 {
					rp.HeroImage = imageMap[idImg]
				}
				p.RelatedsOne = &rp
			}
		}
		if r2 := getMetaInt(p.Metadata, "relatedsTwoID"); r2 > 0 {
			if rp, ok := relatedSinglePosts[r2]; ok {
				if idImg := getMetaInt(rp.Metadata, "heroImageID"); idImg > 0 {
					rp.HeroImage = imageMap[idImg]
				}
				p.RelatedsTwo = &rp
			}
		}

		// Set heroImage for related posts
		for j := range p.Relateds {
			if idImg := getMetaInt(p.Relateds[j].Metadata, "heroImageID"); idImg > 0 {
				p.Relateds[j].HeroImage = imageMap[idImg]
			}
		}
		for j := range p.RelatedsInInputOrder {
			if idImg := getMetaInt(p.RelatedsInInputOrder[j].Metadata, "heroImageID"); idImg > 0 {
				p.RelatedsInInputOrder[j].HeroImage = imageMap[idImg]
			}
		}

		relVideos := relatedVideosMap[id]
		p.RelatedVideos = mergeVideosFromMap(relVideos, videoMap)
		if len(p.ManualOrderOfRelatedVideos) > 0 {
			p.RelatedVideosInInputOrder = orderVideosByManual(p.RelatedVideos, p.ManualOrderOfRelatedVideos)
		} else {
			p.RelatedVideosInInputOrder = p.RelatedVideos
		}
		if p.RelatedVideosInInputOrder == nil {
			p.RelatedVideosInInputOrder = []Video{}
		}
		for j := range p.RelatedVideos {
			if p.RelatedVideos[j].HeroImage != nil {
				if idImg := getMetaInt(p.RelatedVideos[j].HeroImage.Metadata, "heroImageID"); idImg > 0 {
					if img := imageMap[idImg]; img != nil {
						p.RelatedVideos[j].HeroImage = img
					}
				}
			}
		}
		for j := range p.RelatedVideosInInputOrder {
			if p.RelatedVideosInInputOrder[j].HeroImage != nil {
				if idImg := getMetaInt(p.RelatedVideosInInputOrder[j].HeroImage.Metadata, "heroImageID"); idImg > 0 {
					if img := imageMap[idImg]; img != nil {
						p.RelatedVideosInInputOrder[j].HeroImage = img
					}
				}
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

func (r *Repo) fetchContactsByRole(ctx context.Context, postIDs []int) (map[string]map[int][]Contact, error) {
	result := map[string]map[int][]Contact{
		"_Post_writers":       {},
		"_Post_photographers": {},
		"_Post_camera_man":    {},
		"_Post_designers":     {},
		"_Post_engineers":     {},
		"_Post_vocals":        {},
	}
	if len(postIDs) == 0 {
		return result, nil
	}
	query := `
		SELECT t."B" as post_id, c.id, c.name, '_Post_writers' as role
		FROM "_Post_writers" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
		UNION ALL
		SELECT t."B" as post_id, c.id, c.name, '_Post_photographers' as role
		FROM "_Post_photographers" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
		UNION ALL
		SELECT t."B" as post_id, c.id, c.name, '_Post_camera_man' as role
		FROM "_Post_camera_man" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
		UNION ALL
		SELECT t."B" as post_id, c.id, c.name, '_Post_designers' as role
		FROM "_Post_designers" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
		UNION ALL
		SELECT t."B" as post_id, c.id, c.name, '_Post_engineers' as role
		FROM "_Post_engineers" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
		UNION ALL
		SELECT t."B" as post_id, c.id, c.name, '_Post_vocals' as role
		FROM "_Post_vocals" t
		JOIN "Contact" c ON c.id = t."A"
		WHERE t."B" = ANY($1)
	`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var c Contact
		var role string
		if err := rows.Scan(&pid, &c.ID, &c.Name, &role); err != nil {
			return result, err
		}
		if _, ok := result[role]; !ok {
			result[role] = map[int][]Contact{}
		}
		result[role][pid] = append(result[role][pid], c)
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
		WHERE r."A" = ANY($1) AND p.state IN ('published', 'invisible')
		UNION
		SELECT r."B" as post_id, p.id, p.slug, p.title, p."heroImage"
		FROM "_Post_relateds" r
		JOIN "Post" p ON p.id = r."A"
		WHERE r."B" = ANY($1) AND p.state IN ('published', 'invisible')
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

func (r *Repo) fetchRelatedVideosForPosts(ctx context.Context, postIDs []int) (map[int][]Video, error) {
	result := map[int][]Video{}
	if len(postIDs) == 0 {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT rv."A" as post_id, v.id, v."urlOriginal", v."heroImage"
		FROM "_Post_related_videos" rv
		JOIN "Video" v ON v.id = rv."B"
		WHERE rv."A" = ANY($1)`, pqIntArray(postIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var v Video
		var dbID int
		var hero sql.NullInt64
		if err := rows.Scan(&pid, &dbID, &v.VideoSrc, &hero); err != nil {
			return result, err
		}
		v.ID = strconv.Itoa(dbID)
		if hero.Valid {
			v.HeroImage = &Photo{}
			v.HeroImage.ImageFile = ImageFile{}
			v.HeroImage.Metadata = map[string]any{"heroImageID": int(hero.Int64)}
		}
		result[pid] = append(result[pid], v)
	}
	return result, rows.Err()
}

func (r *Repo) fetchPostsByIDs(ctx context.Context, ids []int) ([]Post, []int, error) {
	result := []Post{}
	imageIDs := []int{}
	if len(ids) == 0 {
		return result, imageIDs, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1) AND state IN ('published', 'invisible')`, pqIntArray(ids))
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

func (r *Repo) fetchRelatedsByManualOrder(ctx context.Context, manualOrder []map[string]any) ([]Post, []int, error) {
	result := []Post{}
	imageIDs := []int{}

	// If manualOrder is empty, return empty array (matching Lilith behavior)
	if len(manualOrder) == 0 {
		return result, imageIDs, nil
	}

	// Extract ids from manualOrder
	ids := []int{}
	for _, item := range manualOrder {
		if idStr, ok := item["id"].(string); ok {
			if id, err := strconv.Atoi(idStr); err == nil {
				ids = append(ids, id)
			}
		}
	}

	if len(ids) == 0 {
		return result, imageIDs, nil
	}

	// Query posts by ids
	rows, err := r.db.QueryContext(ctx, `SELECT id, slug, title, "heroImage" FROM "Post" WHERE id = ANY($1) AND state IN ('published', 'invisible')`, pqIntArray(ids))
	if err != nil {
		return result, imageIDs, err
	}
	defer rows.Close()

	// Create a map of id -> Post for quick lookup
	postsMap := make(map[int]Post)
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
		postsMap[dbID] = p
	}
	if err := rows.Err(); err != nil {
		return result, imageIDs, err
	}

	// Sort according to manualOrder
	for _, item := range manualOrder {
		if idStr, ok := item["id"].(string); ok {
			if id, err := strconv.Atoi(idStr); err == nil {
				if p, exists := postsMap[id]; exists {
					result = append(result, p)
				}
			}
		}
	}

	return result, imageIDs, nil
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
		photo.Resized = r.buildResizedURLs(im.fileID, im.ext, int(im.width.Int64), int(im.height.Int64))
		photo.ResizedWebp = r.buildResizedURLs(im.fileID, "webP", int(im.width.Int64), int(im.height.Int64))
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

func (r *Repo) fetchExternalSections(ctx context.Context, externalIDs []int) (map[int][]Section, error) {
	result := map[int][]Section{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	query := `SELECT es."A" as external_id, s.id, s.name, s.slug, s.state FROM "_External_sections" es JOIN "Section" s ON s.id = es."B" WHERE es."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var s Section
		if err := rows.Scan(&eid, &s.ID, &s.Name, &s.Slug, &s.State); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], s)
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalCategories(ctx context.Context, externalIDs []int) (map[int][]Category, error) {
	result := map[int][]Category{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	query := `SELECT ce."B" as external_id, c.id, c.name, c.slug, c.state, c."isMemberOnly" FROM "_Category_externals" ce JOIN "Category" c ON c.id = ce."A" WHERE ce."B" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var c Category
		if err := rows.Scan(&eid, &c.ID, &c.Name, &c.Slug, &c.State, &c.IsMemberOnly); err != nil {
			return result, err
		}
		result[eid] = append(result[eid], c)
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalGroups(ctx context.Context, externalIDs []int) (map[int][]Group, error) {
	result := map[int][]Group{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	query := `SELECT eg."A" as external_id, g.id, g.keyword FROM "_External_groups" eg JOIN "Group" g ON g.id = eg."B" WHERE eg."A" = ANY($1)`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var g Group
		var dbID int
		if err := rows.Scan(&eid, &dbID, &g.Keyword); err != nil {
			return result, err
		}
		g.ID = strconv.Itoa(dbID)
		result[eid] = append(result[eid], g)
	}
	return result, rows.Err()
}

func (r *Repo) fetchExternalRelateds(ctx context.Context, externalIDs []int) (map[int][]Post, error) {
	result := map[int][]Post{}
	if len(externalIDs) == 0 {
		return result, nil
	}
	query := `SELECT er."A" as external_id, p.id, p.slug, p.title, p."heroImage" FROM "_External_relateds" er JOIN "Post" p ON p.id = er."B" WHERE er."A" = ANY($1) AND p.state IN ('published', 'invisible')`
	rows, err := r.db.QueryContext(ctx, query, pqIntArray(externalIDs))
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid int
		var rp Post
		var dbID int
		var heroID sql.NullInt64
		if err := rows.Scan(&eid, &dbID, &rp.Slug, &rp.Title, &heroID); err != nil {
			return result, err
		}
		rp.ID = strconv.Itoa(dbID)
		if heroID.Valid {
			if rp.Metadata == nil {
				rp.Metadata = map[string]any{}
			}
			rp.Metadata["heroImageID"] = int(heroID.Int64)
		}
		result[eid] = append(result[eid], rp)
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
			ID:            strconv.Itoa(im.id),
			Name:          im.name,
			TopicKeywords: im.topicKeywords,
			ImageFile: ImageFile{
				Width:  int(im.width.Int64),
				Height: int(im.height.Int64),
			},
		}
		photo.Resized = r.buildResizedURLs(im.fileID, im.ext, int(im.width.Int64), int(im.height.Int64))
		photo.ResizedWebp = r.buildResizedURLs(im.fileID, "webP", int(im.width.Int64), int(im.height.Int64))
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

func (r *Repo) buildResizedURLs(fileID, ext string, width, height int) Resized {
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
	isLandscape := width >= height
	return Resized{
		Original: makeURL("", ext),
		W480:     makeURL("w480", ext),
		W800:     makeURL("w800", ext),
		W1200: func() string {
			if isLandscape {
				return ""
			}
			return makeURL("w1200", ext)
		}(),
		W1600: makeURL("w1600", ext),
		W2400: func() string {
			if isLandscape {
				return makeURL("w2400", ext)
			}
			return ""
		}(),
	}
}
