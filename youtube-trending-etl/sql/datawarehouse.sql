-- Tạo SEQUENCE cho các bảng
CREATE SEQUENCE video_seq;
CREATE SEQUENCE channel_seq;

-- Tạo các bảng
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id INTEGER DEFAULT NEXTVAL('channel_seq') PRIMARY KEY,
    channel_title TEXT UNIQUE,
    channel_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_category(
	category_id INTEGER PRIMARY KEY,
	category_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_video(
	video_sk INTEGER DEFAULT NEXTVAL('video_seq') PRIMARY KEY,
	video_id TEXT UNIQUE,
	title TEXT,
	publish_time TIMESTAMP,
	tags TEXT,
	thumbnail_link TEXT,
	description TEXT,
	comments_disabled bool,
	ratings_disabled bool,
	video_error_or_removed bool,
	channel_id INTEGER,
	category_id INTEGER,

	CONSTRAINT fk_channel FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
	CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE IF NOT EXISTS dim_time (
	trending_date DATE PRIMARY KEY,
	day_of_week VARCHAR(10),
	month VARCHAR(10),
	quarter VARCHAR(10),
	year INTEGER
);

CREATE TABLE IF NOT EXISTS fact_views (
    video_sk INTEGER,
    trending_date DATE,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    country_code TEXT,

    CONSTRAINT fk_video FOREIGN KEY (video_sk) REFERENCES dim_video(video_sk),
    CONSTRAINT fk_time FOREIGN KEY (trending_date) REFERENCES dim_time(trending_date),
    PRIMARY KEY (video_sk, trending_date)
);