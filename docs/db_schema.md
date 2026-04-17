# EconAtlas DB Schema (public)

37 tables, 504 columns. Snapshot from `information_schema`.

Query live via `POST https://api.velqon.xyz/ops/sql` with body `{"query":"..."}`.

## artha_educational_concepts
**PK:** id

  - id: bigint NOT NULL
  - slug: text NOT NULL
  - title: text NOT NULL
  - body: text NOT NULL
  - category: text
  - embedding: USER-DEFINED
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## broker_charges
**PK:** broker, segment

  - broker: text NOT NULL
  - segment: text NOT NULL
  - brokerage_mode: text NOT NULL
  - brokerage_pct: double precision
  - brokerage_cap: double precision
  - brokerage_flat: double precision
  - min_charge: double precision
  - dp_charge: double precision
  - dp_includes_gst: boolean
  - tagline: text
  - amc_yearly: double precision
  - account_opening_fee: double precision
  - call_trade_fee: double precision
  - source_url: text
  - scraped_at: timestamp with time zone

## chat_messages
**PK:** id
**FK:**
  - session_id -> chat_sessions.id

  - id: text NOT NULL
  - session_id: text NOT NULL
  - role: text NOT NULL
  - content: text NOT NULL
  - tool_calls: jsonb
  - stock_cards: jsonb
  - mf_cards: jsonb
  - feedback: integer
  - created_at: timestamp with time zone NOT NULL
  - thinking_text: text
  - follow_up_suggestions: jsonb
  - data_cards: jsonb

## chat_rate_limits
**PK:** device_id, date

  - device_id: text NOT NULL
  - date: date NOT NULL
  - count: integer NOT NULL

## chat_sessions
**PK:** id

  - id: text NOT NULL
  - device_id: text NOT NULL
  - title: text
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## chat_tool_invocations
**PK:** id

  - id: bigint NOT NULL
  - session_id: text
  - message_id: text
  - tool_name: text NOT NULL
  - params: jsonb
  - success: boolean NOT NULL
  - refused: boolean NOT NULL
  - result_size: integer
  - latency_ms: integer
  - error_message: text
  - created_at: timestamp with time zone NOT NULL

## device_ipo_alerts
**PK:** id

  - id: uuid NOT NULL
  - device_id: text NOT NULL
  - symbol: text NOT NULL
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## device_preferences
**PK:** device_id

  - device_id: text NOT NULL
  - verbosity: text NOT NULL
  - risk_tolerance: text NOT NULL
  - language: text NOT NULL
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## device_tokens
**PK:** device_id

  - device_id: text NOT NULL
  - fcm_token: text NOT NULL
  - platform: text
  - updated_at: timestamp with time zone NOT NULL

## device_watchlists
**PK:** id

  - id: uuid NOT NULL
  - device_id: text NOT NULL
  - asset: text NOT NULL
  - position: integer NOT NULL
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## devices
**PK:** id

  - id: uuid NOT NULL
  - user_id: text
  - device_token: text
  - platform: text

## discover_mf_nav_history
**PK:** id

  - id: uuid NOT NULL
  - scheme_code: text NOT NULL
  - nav_date: date NOT NULL
  - nav: double precision NOT NULL
  - source: text
  - ingested_at: timestamp with time zone NOT NULL

## discover_mutual_fund_snapshots
**PK:** id

  - id: uuid NOT NULL
  - scheme_code: text NOT NULL
  - scheme_name: text NOT NULL
  - amc: text
  - category: text
  - sub_category: text
  - plan_type: text NOT NULL
  - option_type: text
  - nav: double precision NOT NULL
  - nav_date: date
  - expense_ratio: double precision
  - aum_cr: double precision
  - risk_level: text
  - returns_1y: double precision
  - returns_3y: double precision
  - returns_5y: double precision
  - std_dev: double precision
  - sharpe: double precision
  - sortino: double precision
  - score: double precision
  - score_return: double precision
  - score_risk: double precision
  - score_cost: double precision
  - score_consistency: double precision
  - score_breakdown: jsonb
  - tags: jsonb
  - source_status: text NOT NULL
  - source_timestamp: timestamp with time zone NOT NULL
  - ingested_at: timestamp with time zone NOT NULL
  - primary_source: text
  - secondary_source: text
  - category_rank: integer
  - category_total: integer
  - fund_age_years: double precision
  - sub_category_rank: integer
  - sub_category_total: integer
  - max_drawdown: double precision
  - rolling_return_consistency: double precision
  - alpha: double precision
  - beta: double precision
  - score_alpha: double precision
  - score_beta: double precision
  - score_performance: double precision
  - score_category_fit: double precision
  - sub_category_percentile: double precision
  - fund_classification: text
  - tags_v2: jsonb NOT NULL
  - top_holdings: jsonb
  - sector_allocation: jsonb
  - asset_allocation: jsonb
  - holdings_as_of: date
  - fund_managers: jsonb
  - fund_type: text
  - returns_1m: double precision
  - returns_3m: double precision
  - returns_6m: double precision
  - returns_10y: double precision

## discover_stock_intraday
**PK:** symbol, ts

  - symbol: text NOT NULL
  - ts: timestamp with time zone NOT NULL
  - price: double precision NOT NULL
  - volume: bigint
  - percent_change: double precision

## discover_stock_price_history
**PK:** id

  - id: uuid NOT NULL
  - symbol: text NOT NULL
  - trade_date: date NOT NULL
  - close: double precision NOT NULL
  - volume: bigint
  - source: text
  - ingested_at: timestamp with time zone NOT NULL

## discover_stock_score_history
**PK:** id

  - id: uuid NOT NULL
  - symbol: text NOT NULL
  - score: double precision NOT NULL
  - scored_at: timestamp with time zone NOT NULL

## discover_stock_snapshots
**PK:** id

  - id: uuid NOT NULL
  - market: text NOT NULL
  - symbol: text NOT NULL
  - display_name: text NOT NULL
  - sector: text
  - last_price: double precision NOT NULL
  - point_change: double precision
  - percent_change: double precision
  - volume: bigint
  - traded_value: double precision
  - pe_ratio: double precision
  - roe: double precision
  - roce: double precision
  - debt_to_equity: double precision
  - price_to_book: double precision
  - eps: double precision
  - score: double precision
  - score_momentum: double precision
  - score_liquidity: double precision
  - score_fundamentals: double precision
  - score_breakdown: jsonb
  - tags: jsonb
  - source_status: text NOT NULL
  - source_timestamp: timestamp with time zone NOT NULL
  - ingested_at: timestamp with time zone NOT NULL
  - primary_source: text
  - secondary_source: text
  - high_52w: double precision
  - low_52w: double precision
  - market_cap: double precision
  - dividend_yield: double precision
  - score_volatility: double precision
  - score_growth: double precision
  - percent_change_3m: double precision
  - percent_change_1w: double precision
  - percent_change_1y: double precision
  - percent_change_3y: double precision
  - promoter_holding: double precision
  - fii_holding: double precision
  - dii_holding: double precision
  - government_holding: double precision
  - public_holding: double precision
  - num_shareholders: bigint
  - promoter_holding_change: double precision
  - fii_holding_change: double precision
  - dii_holding_change: double precision
  - beta: double precision
  - free_cash_flow: double precision
  - operating_cash_flow: double precision
  - total_cash: double precision
  - total_debt: double precision
  - total_revenue: double precision
  - gross_margins: double precision
  - operating_margins: double precision
  - profit_margins: double precision
  - revenue_growth: double precision
  - earnings_growth: double precision
  - forward_pe: double precision
  - analyst_target_mean: double precision
  - analyst_count: integer
  - analyst_recommendation: text
  - analyst_recommendation_mean: double precision
  - score_ownership: double precision
  - score_financial_health: double precision
  - score_analyst: double precision
  - industry: text
  - payout_ratio: double precision
  - pledged_promoter_pct: double precision
  - sales_growth_yoy: double precision
  - profit_growth_yoy: double precision
  - opm_change: double precision
  - interest_coverage: double precision
  - compounded_sales_growth_3y: double precision
  - compounded_profit_growth_3y: double precision
  - total_assets: double precision
  - asset_growth_yoy: double precision
  - reserves_growth: double precision
  - debt_direction: double precision
  - cwip: double precision
  - cash_from_operations: double precision
  - cash_from_investing: double precision
  - cash_from_financing: double precision
  - num_shareholders_change_qoq: double precision
  - num_shareholders_change_yoy: double precision
  - synthetic_forward_pe: double precision
  - score_valuation: double precision
  - score_earnings_quality: double precision
  - score_smart_money: double precision
  - pl_annual: jsonb
  - bs_annual: jsonb
  - cf_annual: jsonb
  - shareholding_quarterly: jsonb
  - percent_change_5y: double precision
  - sector_percentile: double precision
  - lynch_classification: text
  - score_quality: double precision
  - score_institutional: double precision
  - score_risk: double precision
  - technical_score: double precision
  - rsi_14: double precision
  - action_tag: text
  - action_tag_reasoning: text
  - score_confidence: text
  - trend_alignment: text
  - breakout_signal: text
  - tags_v2: jsonb NOT NULL
  - entry_exit_signal: text
  - risk_reward_ratio: double precision
  - risk_reward_tag: text
  - growth_ranges: jsonb
  - percent_change_1m: double precision
  - percent_change_6m: double precision
  - ai_narrative_embedding: USER-DEFINED
  - why_narrative_embedding: USER-DEFINED
  - max_drawdown_1y: double precision
  - max_drawdown_3y: double precision
  - dividend_consistency_years: integer
  - ai_narrative: text
  - ai_narrative_at: timestamp with time zone
  - scored_at: timestamp with time zone

## economic_calendar
**PK:** id

  - id: uuid NOT NULL
  - event_name: text NOT NULL
  - institution: text NOT NULL
  - event_date: date NOT NULL
  - country: text NOT NULL
  - event_type: text NOT NULL
  - description: text
  - source: text
  - created_at: timestamp with time zone NOT NULL
  - importance: text
  - previous: double precision
  - consensus: double precision
  - actual: double precision
  - surprise: double precision
  - status: text
  - revised_at: timestamp with time zone

## economic_events
**PK:** id

  - id: uuid NOT NULL
  - event_type: text NOT NULL
  - entity: text NOT NULL
  - impact: text NOT NULL
  - confidence: double precision NOT NULL
  - created_at: timestamp with time zone NOT NULL
  - event_embedding: USER-DEFINED

## feedback_submissions
**PK:** id

  - id: uuid NOT NULL
  - device_id: text NOT NULL
  - category: text NOT NULL
  - message: text NOT NULL
  - app_version: text
  - platform: text
  - status: text NOT NULL
  - created_at: timestamp with time zone NOT NULL

## fixed_income_rates
**PK:** id

  - id: bigint NOT NULL
  - instrument_type: text NOT NULL
  - rate_source: text NOT NULL
  - tenor: text
  - rate_pct: double precision NOT NULL
  - min_amount: double precision
  - tax_status: text
  - lock_in_years: double precision
  - notes: text
  - updated_at: timestamp with time zone NOT NULL

## ipo_snapshots
**PK:** id

  - id: uuid NOT NULL
  - symbol: text NOT NULL
  - company_name: text NOT NULL
  - market: text NOT NULL
  - status: text NOT NULL
  - ipo_type: text NOT NULL
  - issue_size_cr: double precision
  - price_band: text
  - gmp_percent: double precision
  - subscription_multiple: double precision
  - open_date: date
  - close_date: date
  - listing_date: date
  - source_timestamp: timestamp with time zone NOT NULL
  - ingested_at: timestamp with time zone NOT NULL
  - source: text
  - notes: text
  - listing_price: double precision
  - listing_gain_pct: double precision
  - outcome_state: text
  - archived_at: timestamp with time zone

## job_dead_letters
**PK:** id

  - id: uuid NOT NULL
  - job_name: text NOT NULL
  - error_message: text NOT NULL
  - traceback: text
  - retry_count: integer NOT NULL
  - status: text NOT NULL
  - failed_at: timestamp with time zone NOT NULL
  - retried_at: timestamp with time zone
  - created_at: timestamp with time zone NOT NULL

## job_run_log
**PK:** job_name

  - job_name: text NOT NULL
  - last_success_at: timestamp with time zone
  - last_failure_at: timestamp with time zone
  - last_duration_s: double precision
  - last_error: text
  - success_count: integer NOT NULL
  - failure_count: integer NOT NULL
  - consecutive_failures: integer NOT NULL
  - last_trigger: text
  - updated_at: timestamp with time zone NOT NULL

## macro_forecasts
**PK:** id

  - id: uuid NOT NULL
  - indicator_name: text NOT NULL
  - country: text NOT NULL
  - forecast_year: integer NOT NULL
  - value: double precision NOT NULL
  - source: text
  - fetched_at: timestamp with time zone NOT NULL

## macro_indicators
**PK:** id

  - id: uuid NOT NULL
  - indicator_name: text NOT NULL
  - value: double precision NOT NULL
  - country: text NOT NULL
  - timestamp: timestamp with time zone NOT NULL
  - unit: text
  - source: text

## market_prices
**PK:** id

  - id: uuid NOT NULL
  - asset: text NOT NULL
  - price: double precision NOT NULL
  - timestamp: timestamp with time zone NOT NULL
  - source: text
  - instrument_type: text
  - unit: text
  - change_percent: double precision
  - previous_close: double precision

## market_prices_intraday
**PK:** id

  - id: uuid NOT NULL
  - asset: text NOT NULL
  - instrument_type: text NOT NULL
  - price: double precision NOT NULL
  - timestamp: timestamp with time zone NOT NULL
  - source_timestamp: timestamp with time zone NOT NULL
  - ingested_at: timestamp with time zone NOT NULL
  - provider: text NOT NULL
  - provider_priority: integer NOT NULL
  - confidence_level: double precision
  - is_fallback: boolean NOT NULL
  - quality: text
  - is_predictive: boolean NOT NULL
  - session_source: text

## market_scores
**PK:** asset, instrument_type

  - asset: text NOT NULL
  - instrument_type: text NOT NULL
  - score_trend: double precision
  - score_volatility: double precision
  - score_momentum: double precision
  - verdict: text
  - action_tag: text
  - action_tag_reasoning: text
  - driver_tags: jsonb
  - type_extras: jsonb
  - computed_at: timestamp with time zone

## news_articles
**PK:** id

  - id: uuid NOT NULL
  - title: text NOT NULL
  - summary: text
  - body: text
  - timestamp: timestamp with time zone NOT NULL
  - source: text
  - url: text
  - primary_entity: text
  - impact: text
  - confidence: double precision
  - embedding: USER-DEFINED

## notification_log
**PK:** id

  - id: integer NOT NULL
  - notification_type: text NOT NULL
  - sent_at: timestamp with time zone NOT NULL
  - title: text
  - dedup_key: text NOT NULL
  - fcm_message_id: text
  - fcm_topic: text

## ops_logs
**PK:** id

  - id: bigint NOT NULL
  - timestamp: timestamp with time zone NOT NULL
  - level: text NOT NULL
  - logger: text NOT NULL
  - message: text NOT NULL
  - module: text
  - function: text
  - line: integer
  - exception: text

## statutory_charges
**PK:** segment, exchange

  - segment: text NOT NULL
  - exchange: text NOT NULL
  - stt_buy_rate: double precision
  - stt_sell_rate: double precision
  - exchange_txn_rate: double precision
  - stamp_duty_buy_rate: double precision
  - ipft_rate: double precision
  - sebi_fee_rate: double precision
  - gst_rate: double precision
  - effective_from: date
  - source_url: text
  - scraped_at: timestamp with time zone

## stock_future_prospect_documents
**PK:** id

  - id: uuid NOT NULL
  - symbol: text NOT NULL
  - display_name: text NOT NULL
  - source_kind: text NOT NULL
  - source_name: text
  - source_url: text
  - source_title: text
  - document_key: text NOT NULL
  - document_type: text NOT NULL
  - source_published_at: timestamp with time zone
  - recency_bucket: text
  - extracted_fields: jsonb NOT NULL
  - extraction_confidence: double precision
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL

## stock_future_prospect_passages
**PK:** id
**FK:**
  - document_id -> stock_future_prospect_documents.id

  - id: uuid NOT NULL
  - document_id: uuid NOT NULL
  - symbol: text NOT NULL
  - passage_index: integer NOT NULL
  - passage_type: text
  - passage_text: text NOT NULL
  - source_published_at: timestamp with time zone
  - embedding_status: text NOT NULL
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL
  - embedding: USER-DEFINED

## stock_snapshots
**PK:** id

  - id: uuid NOT NULL
  - market: text NOT NULL
  - symbol: text NOT NULL
  - display_name: text NOT NULL
  - sector: text
  - last_price: double precision NOT NULL
  - point_change: double precision
  - percent_change: double precision
  - volume: bigint
  - traded_value: double precision
  - source_timestamp: timestamp with time zone NOT NULL
  - ingested_at: timestamp with time zone NOT NULL
  - source: text

## tax_config_versions
**PK:** id

  - id: uuid NOT NULL
  - version: text NOT NULL
  - default_fy: text NOT NULL
  - disclaimer: text NOT NULL
  - supported_fy: jsonb NOT NULL
  - rounding_policy: jsonb NOT NULL
  - rules_by_fy: jsonb NOT NULL
  - content_hash: text NOT NULL
  - source: text
  - source_mode: text
  - is_active: boolean NOT NULL
  - archived_at: timestamp with time zone
  - last_validation_status: text
  - last_validation_reason: text
  - last_sync_attempt_at: timestamp with time zone
  - last_sync_success_at: timestamp with time zone
  - created_at: timestamp with time zone NOT NULL
  - updated_at: timestamp with time zone NOT NULL
  - helper_points: jsonb NOT NULL
