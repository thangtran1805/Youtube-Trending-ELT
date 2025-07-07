# 📋 Table Change Log

This document keeps track of schema changes for all tables in the data warehouse.

---

## 🔸 dim_channel

| Date       | Change                             | Notes                        |
|------------|------------------------------------|------------------------------|
| 2025-07-05 | Added table `dim_channel`          | With `channel_title UNIQUE` |

---

## 🔸 dim_category

| Date       | Change                             | Notes            |
|------------|------------------------------------|------------------|
| 2025-07-05 | Created table with PK `category_id`| Category mapping |

---

## 🔸 dim_video

| Date       | Change                             | Notes                         |
|------------|------------------------------------|-------------------------------|
| 2025-07-05 | Created table with FK `channel_id` and `category_id` | Added surrogate key `video_sk` |

---

## 🔸 dim_time

| Date       | Change                         | Notes                   |
|------------|--------------------------------|-------------------------|
| 2025-07-06 | Created with `trending_date PK`| Based on trending_date |

---

## 🔸 fact_views

| Date       | Change                             | Notes                                 |
|------------|------------------------------------|---------------------------------------|
| 2025-07-06 | Created with FK `video_sk`, `trending_date` | Handles view metrics per date-country |
| 2025-07-07 | Added `ON CONFLICT(video_sk, trending_date) DO NOTHING` | Avoid duplicate loads                |

---

## 🔧 Sequence Tables

- `video_seq` for `dim_video.video_sk`
- `channel_seq` for `dim_channel.channel_id`

---