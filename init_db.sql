-- Créer les schémas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS public;

-- Table Bronze : Données brutes Binance
CREATE TABLE IF NOT EXISTS bronze.btc_ohlc (
    id SERIAL PRIMARY KEY,
    open_time BIGINT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume DOUBLE PRECISION NOT NULL,
    number_of_trades INTEGER NOT NULL,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(open_time)
);

CREATE INDEX idx_bronze_open_time ON bronze.btc_ohlc(open_time);

-- Table Silver : Données enrichies avec features
CREATE TABLE IF NOT EXISTS silver.btc_features (
    id SERIAL PRIMARY KEY,
    open_time BIGINT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume DOUBLE PRECISION NOT NULL,
    number_of_trades INTEGER NOT NULL,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
    -- Features calculées
    return_1m DOUBLE PRECISION,
    ma_5 DOUBLE PRECISION,
    ma_10 DOUBLE PRECISION,
    taker_ratio DOUBLE PRECISION,
    close_t_plus_10 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(open_time)
);

CREATE INDEX idx_silver_open_time ON silver.btc_features(open_time);

-- Table Users
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,  -- ← Assurez-vous que c'est bien password_hash
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Predictions
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    prediction_for TIMESTAMP NOT NULL,
    current_price FLOAT NOT NULL,
    predicted_price FLOAT NOT NULL,
    actual_price FLOAT,
    confidence_lower FLOAT,
    confidence_upper FLOAT,
    model_version VARCHAR(50) DEFAULT 'v1.0.0',
    error FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_predictions_user ON public.predictions(user_id);
CREATE INDEX idx_predictions_time ON public.predictions(prediction_for);

-- Insérer un utilisateur admin par défaut (mot de passe: admin123)
INSERT INTO public.users (email, username, password_hash)
VALUES ('admin@quantai.com', 'admin', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyWui/W4Jl/y')
ON CONFLICT DO NOTHING;
