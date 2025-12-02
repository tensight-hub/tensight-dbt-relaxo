# Relaxo dbt Project

A comprehensive dbt (data build tool) project for transforming and analyzing multi-channel marketplace data, inventory management, and sales analytics for Relaxo Footwear.


This dbt project transforms raw data from multiple sources into a unified analytics platform for Relaxo Footwear.

- **Marketplace Data**: Product listings, prices, ratings, and reviews from Amazon, Flipkart, Myntra, and Ajio
- **Inventory Data**: Stock levels and sales from Unicommerce across multiple warehouse facilities
- **Returns Data**: Return analytics from various sales channels
- **Master Data**: Product master and price parity reference data

The project enables comprehensive analysis of:
- Price parity across e-commerce channels
- Product ratings and customer reviews
- Stock availability and inventory optimization
- Sales performance and returns analysis
- Competitor discount tracking

---

## 🏗️ Project Architecture

This project follows a **staged transformation** approach with three layers:

```
Raw Data Sources
      ↓
  Staging Layer (Views)
      ↓
Intermediate Layer (Ephemeral)
      ↓
  Overview Layer (Tables)
      ↓
Analytics & Reporting
```

## 📊 Data Sources

### 1. Buybox (Marketplace Data)

**Tables**:
- `rating_and_reviews`: Product ratings, reviews, prices, and availability across channels
- `competitor_discount_analysis`: Competitor pricing and discount information
- Aggregated views for price, rating, review, and availability across channels (latest month)

**Channels Tracked**: Amazon, Flipkart, Myntra, Ajio

### 2. Unicommerce (Warehouse/Inventory System)

**Source Names**: 
- `unicommerce`: Consolidated source for all Unicommerce data
- `unicommerce_facility_wise`: Facility-specific source for granular sales data

**Sales Data** (facility-wise):
- `uniware_sales_banglore`
- `uniware_sales_bhiwandi`
- `uniware_sales_ghevra`
- `uniware_sales_gurgram`
- `uniware_sales_hyderabad`
- `uniware_sales_kolkata`
- `uniware_sales_mundkanew`

**Inventory Data** (facility-wise):
- `uniware_inventory_banglore`
- `uniware_inventory_bhiwandi`
- `uniware_inventory_ghevra`
- `uniware_inventory_gurgram`
- `uniware_inventory_hyderabad`
- `uniware_inventory_kolkata`
- `uniware_inventory_mundkanew`

**Returns Data** (facility-wise):
- `returns_uniware_bangalore`
- `returns_uniware_bhiwandi`
- `returns_uniware_ghevra`
- `returns_uniware_gurugram`
- `returns_uniware_hyderabad`
- `returns_uniware_kolkata`
- `returns_uniware_mundkha_new`

**Other**:
- `rating_and_reviews`: Channel-specific rating and review data

### 3. Channel Returns

- `ajio_returns`
- `flipkart_returns`
- `myntra_returns`

### 4. Master/Lookup Data

- `product_master`: Product catalog with SKU details, categories, sizes, gender
- `price_parity_master`: Price parity reference data

---

## 📁 Project Structure

```
tensight-dbt-relaxo/
│
├── models/
│   ├── staging/             
│   │   ├── ajio/
│   │   ├── buybox/
│   │   ├── flipkart/
│   │   ├── lookups/
│   │   ├── myntra/
│   │   ├── unicommerce/
│   │   └── unicommerce_facility_wise/
│   │
│   ├── intermediate/        
│   │   ├── buybox/
│   │   └── uniware/
│   │
│   └── overview/            
│       ├── marketplace/     
│       └── uniware/         
│
├── macros/                  
│   ├── categorize_product.sql
│   ├── channel_avg_rating.sql
│   ├── channel_price.sql
│   ├── channel_product_url.sql
│   └── channel_reviews.sql
│
├── analyses/                
├── seeds/                 
├── snapshots/              
├── tests/                 
│
├── dbt_project.yml         
└── README.md             
```

---

## 🗂️ Model Layers

### 1. Staging Layer (`models/staging/`)

**Purpose**: Clean and standardize raw data from source systems

**Key Models**:

#### Buybox Staging
- `stg_buybox_rating_and_reviews`: Product ratings, reviews, and pricing across channels
- `stg_competitor_discount_analysis`: Competitor pricing analysis
- `stg_weekly_wise_price`: Weekly price aggregation
- `stg_weekly_wise_rating`: Weekly rating aggregation
- `stg_weekly_wise_review`: Weekly review count aggregation
- `stg_week_wise_stock_availability`: Weekly stock availability

#### Unicommerce Staging
- `stg_unicommerce_inventory`: Consolidated inventory data from all facilities. Combines inventory data from all 7 facilities with standardized field names.

- `stg_unicommerce_orders`: Consolidated sales order data from all facilities. This is a UNION of all facility-wise sales models (`stg_unicommerce_facility_wise_*`), providing a unified view of sales orders across all facilities. Contains all the same fields as the facility-wise models.

- `stg_unicommerce_returns`: Consolidated returns data from all Unicommerce facilities. This model combines returns data from all 7 facilities (Bangalore, Bhiwandi, Ghevra, Gurugram, Hyderabad, Kolkata, Mundka New) into a unified view with standardized field names and normalized channel mappings.


#### Unicommerce Facility-wise Staging
Facility-specific sales staging models for granular facility-level analysis. Each model contains sales order details, product information, order status, shipping details, pricing, and dates specific to that facility.

**Sales Models** (by facility):
- `stg_unicommerce_facility_wise_banglore`: Sales data from Bangalore facility (facility_code: 'Bangalore1')
- `stg_unicommerce_facility_wise_bhiwandi`: Sales data from Bhiwandi facility (facility_code: 'Bhiwandi1')
- `stg_unicommerce_facility_wise_ghevra`: Sales data from Ghevra facility (facility_code: 'Ghevra1')
- `stg_unicommerce_facility_wise_gurgram`: Sales data from Gurugram facility (facility_code: 'Gurugram1')
- `stg_unicommerce_facility_wise_hyderabad`: Sales data from Hyderabad facility (facility_code: 'Hyderabad1')
- `stg_unicommerce_facility_wise_kolkata`: Sales data from Kolkata facility (facility_code: 'Kolkata1')
- `stg_unicommerce_facility_wise_mundkanew`: Sales data from Mundka New facility (facility_code: 'Mundkanew1')

#### Channel Returns Staging
- `stg_ajio_returns`
- `stg_flipkart_returns`
- `stg_myntra_returns`

#### Lookups/Master Data Staging
- `stg_product_master`: Product catalog master
- `stg_price_parity_master`: Price parity reference

---

### 2. Intermediate Layer (`models/intermediate/`)

**Purpose**: Business logic transformations and data enrichment

**Key Models**:

#### Buybox Intermediate
- `int_buybox_rating_and_reviews`: Enriched rating/review data with product master join

#### Uniware Intermediate
- `int_allchannel_returns`: Consolidated returns across all channels
- `int_uniware_returns`: Unified Unicommerce returns
- `int_uniware_sales_sku_wise_rating`: Sales data with rating integration



### 3. Overview Layer (`models/overview/`)

**Purpose**: Final analytics tables for reporting and dashboards

#### Marketplace Analytics (`models/overview/marketplace/`)

**Price Analytics**:
- `price_across_channels`: Historical price comparison across channels
- `price_across_channels_latest_date`: Latest price snapshot
- `price_across_channels_latest_month`: Monthly price aggregation
- `weekly_wise_price`: Weekly price trends

**Rating Analytics**:
- `rating_across_channels`: Historical ratings across channels
- `rating_across_channels_latest_date`: Latest rating snapshot
- `rating_across_channels_latest_month`: Monthly rating aggregation
- `weekly_wise_rating`: Weekly rating trends
- `grouped_rating_views`: Grouped rating visualizations
- `combined_rating_and_review`: Combined rating and review metrics

**Review Analytics**:
- `review_across_channels`: Historical review counts
- `review_across_channels_latest_date`: Latest review snapshot
- `review_across_channels_latest_month`: Monthly review aggregation
- `weekly_wise_review`: Weekly review trends

**Stock/Availability Analytics**:
- `stock_availability_across_channels`: Stock availability by channel
- `stock_availability_latest_date`: Latest stock snapshot
- `product_availability_latest_month`: Monthly availability aggregation
- `week_wise_stock_availability`: Weekly stock trends

**Buybox Analytics**:
- `buybox_across_channels`: Buybox seller information by channel
- `download_buybox_across_channels`: Download-ready buybox data

**Other Analytics**:
- `brand_discount_analysis`: Brand-level discount analysis
- `download_price_parity_across_channels`: Price parity export
- `download_product_availability`: Product availability export
- `download_rating_across_channels`: Rating data export
- `download_review_across_channels`: Review data export

#### Uniware Analytics (`models/overview/uniware/`)

- `uniware_inventory_sales_report`: Inventory levels with sales metrics
- `inventory_sales_combined`: Combined inventory and sales view
- `uniware_non_moving_stock`: Identification of slow-moving inventory
- `uniware_returns_and_channel_combined`: Combined returns analysis
- `uniware_sales_sku_wise_rating`: Sales performance with rating correlation

## 🔧 Macros

Reusable SQL macros for common transformations:

### `categorize_product(product_name_col)`
Categorizes products based on name patterns:
- Sandals
- Flip Flops
- Sneakers
- Running Shoes
- Casual Shoes
- Shoes (generic)
- Slipper
- Clogs

### `channel_avg_rating(source_col, rating_col, channel, alias)`
Extracts average rating for a specific channel, handling null/nan values and casting to decimal(10, 2).

**Parameters**:
- `source_col`: Column containing the channel/source name
- `rating_col`: Column containing the rating value
- `channel`: Channel name to filter (e.g., 'amazon', 'flipkart')
- `alias`: Output column alias

### `channel_price(source_col, price_col, channel, alias)`
Extracts price for a specific channel, handling null/nan values and casting to decimal(10, 2).

**Parameters**:
- `source_col`: Column containing the channel/source name
- `price_col`: Column containing the price value
- `channel`: Channel name to filter (e.g., 'amazon', 'flipkart')
- `alias`: Output column alias

### `channel_product_url(source_col, url_col, platform, alias)`
Extracts product URL for a specific platform using MAX aggregation.

**Parameters**:
- `source_col`: Column containing the channel/source name
- `url_col`: Column containing the product URL
- `platform`: Platform name to filter (e.g., 'amazon', 'flipkart')
- `alias`: Output column alias

### `channel_reviews(source_col, review_col, channel, alias)`
Extracts review count for a specific channel, handling null/nan values and casting to decimal(10, 2).

**Parameters**:
- `source_col`: Column containing the channel/source name
- `review_col`: Column containing the review count
- `channel`: Channel name to filter (e.g., 'amazon', 'flipkart')
- `alias`: Output column alias

**Usage Example**:
```sql
{{ channel_price('source', 'product_price', 'amazon', 'amazon_price') }}
```


## 📚 Model Catalog

### Marketplace Models

#### Price Models
| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `price_across_channels` | Historical price comparison | `amazon_price`, `flipkart_price`, `myntra_price`, `ajio_price` |
| `price_across_channels_latest_date` | Latest price snapshot | Latest prices by channel |
| `price_across_channels_latest_month` | Monthly price aggregation | Monthly average prices |
| `weekly_wise_price` | Weekly price trends | Weekly aggregated prices |

#### Rating Models
| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `rating_across_channels` | Historical ratings | `amazon_rating`, `flipkart_rating`, etc. |
| `rating_across_channels_latest_date` | Latest rating snapshot | Current ratings |
| `rating_across_channels_latest_month` | Monthly rating aggregation | Monthly average ratings |
| `weekly_wise_rating` | Weekly rating trends | Weekly aggregated ratings |
| `combined_rating_and_review` | Combined metrics | Rating + review counts |

#### Review Models
| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `review_across_channels` | Historical review counts | Review counts by channel |
| `review_across_channels_latest_date` | Latest review snapshot | Current review counts |
| `review_across_channels_latest_month` | Monthly review aggregation | Monthly review totals |
| `weekly_wise_review` | Weekly review trends | Weekly review counts |

#### Stock/Availability Models
| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `stock_availability_across_channels` | Stock status by channel | Availability flags |
| `stock_availability_latest_date` | Latest stock snapshot | Current stock status |
| `product_availability_latest_month` | Monthly availability | Monthly availability metrics |
| `week_wise_stock_availability` | Weekly stock trends | Weekly availability |

#### Buybox Models
| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `buybox_across_channels` | Buybox seller info | Seller names, product URLs |
| `download_buybox_across_channels` | Export-ready buybox data | Formatted buybox data |

### Uniware Models

| Model Name | Description | Key Fields |
|------------|-------------|------------|
| `uniware_inventory_sales_report` | Inventory with sales metrics | `soh`, `units_sold`, `ros_30`, `doh` |
| `inventory_sales_combined` | Combined inventory/sales view | Inventory and sales combined |
| `uniware_non_moving_stock` | Slow-moving inventory | Non-moving SKUs |
| `uniware_returns_and_channel_combined` | Combined returns analysis | Returns across channels/facilities |
| `uniware_sales_sku_wise_rating` | Sales with rating correlation | Sales data with ratings |

---

## 🔍 Key Metrics Explained

### Rate of Sale (ROS)
Average daily units sold over a specified period (typically 30 days).

### Days of Inventory (DOH)
Current stock divided by daily sales rate. Indicates how many days of inventory remain.

### Inventory Status
- **Overstock**: DOH > 90 days
- **Optimal**: DOH between 45-90 days
- **Understock**: DOH < 45 days

### Inventory Recommendation
Recommended stock to maintain 45 days of inventory: `(ROS × 45) - Current Stock`

---

## 📝 Notes

- All staging models are materialized as views for real-time data access
- Intermediate models are ephemeral to optimize query performance
- Overview models are materialized as tables for fast reporting
- The project uses Parquet format for efficient storage
- Date fields are standardized across all models

---

