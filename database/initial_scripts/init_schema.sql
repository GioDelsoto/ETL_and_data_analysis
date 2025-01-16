CREATE SCHEMA IF NOT EXISTS etl_schema;

-- Tabela das lojas
CREATE TABLE etl_schema."stores" (
  "id" SERIAL PRIMARY KEY,
  "store_name" VARCHAR(100) NOT NULL,
  "yampi_alias" VARCHAR(100) NOT NULL,
  "created_at" DATE NOT NULL,
  "fb_ad_account" VARCHAR(100) NOT NULL
);

-- Populating the stores table
INSERT INTO etl_schema."stores" ("store_name", "yampi_alias", "created_at", "fb_ad_account")
VALUES
  ('BelaBelinda', 'belabelinda2', '2020-11-01', '255967664900512'),
  ('PinkPerfect', 'pinkperfect', '2021-02-01', '2707580749468687'),
  ('LeMoritz', 'lemoritz', '2023-01-01', '5585673474853626'),
  ('BrainJuice', 'brain-juice', '2024-03-01', '915553960187187');

CREATE TABLE etl_schema."customers" (
  "id" SERIAL PRIMARY KEY,                      -- Identificador único do cliente
  "name" VARCHAR(200) NOT NULL,                 -- Nome do cliente
  "email" VARCHAR(50) NOT NULL,                 -- E-mail único
  "cpf" VARCHAR(14) NOT NULL,                   -- CPF com tamanho fixo e obrigatório
  "phone" VARCHAR(20),                          -- Telefone opcional
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id"), -- Relaciona a loja
  "created_at" TIMESTAMPTZ NOT NULL,            -- Data de criação do registro
  CONSTRAINT unique_cpf_store UNIQUE ("cpf", "store_id")  -- Combinação única de cpf e store_id
);

-- Tabela de produtos
CREATE TABLE etl_schema."products" (
  "sku" VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, -- SKU único
  "name" VARCHAR(100) NOT NULL,                   -- Nome do produto
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id") -- Relaciona a loja
);

-- Tabela de kits
CREATE TABLE etl_schema."kits" (
  "sku" VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, -- SKU do kit único
  "name" VARCHAR(300) NOT NULL,                   -- Nome do kit
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id") -- Relaciona a loja
);

-- Tabela de conteúdo de kits
CREATE TABLE etl_schema."kits_content" (
  "kit_sku" VARCHAR(100) NOT NULL,                 -- Relaciona o kit
  "product_sku" VARCHAR(100) NOT NULL,             -- Relaciona o produto
  "quantity" INTEGER NOT NULL CHECK (quantity > 0),-- Quantidade do produto no kit
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id"), -- Relaciona a loja
  PRIMARY KEY ("kit_sku", "product_sku"),          -- Chave composta para evitar duplicações
  FOREIGN KEY ("kit_sku") REFERENCES etl_schema."kits" ("sku"),
  FOREIGN KEY ("product_sku") REFERENCES etl_schema."products" ("sku")
);

-- Tabela de pedidos
CREATE TABLE etl_schema."orders" (
  "id" SERIAL PRIMARY KEY,                         -- Identificador único da linha do pedido
  "order_id" BIGINT,                               -- Identificador do pedido principal
  "order_date" TIMESTAMPTZ NOT NULL,               -- Data do pedido em UTC
  "customer_id" INTEGER NOT NULL REFERENCES etl_schema."customers" ("id"), -- Relaciona o cliente
  "status" VARCHAR(50) NOT NULL,                   -- Status do pedido
  "payment_method" VARCHAR(50),                    -- Método de pagamento
  "kit_sku" VARCHAR(100) REFERENCES etl_schema."kits" ("sku"), -- Relaciona o kit
  "quantity" INTEGER NOT NULL CHECK (quantity > 0),-- Quantidade
  "total_value" DECIMAL(10, 2) NOT NULL CHECK (total_value >= 0), -- Valor total
  "total_product" DECIMAL(10, 2) NOT NULL CHECK (total_product >= 0), -- Valor dos produtos
  "total_shipment" DECIMAL(10, 2) NOT NULL CHECK (total_shipment >= 0), -- Valor do frete
  "coupom_code" VARCHAR(50),                       -- Código do cupom
  "coupom_value" DECIMAL(10, 2),                   -- Valor do cupom
  "utm_source" VARCHAR(300),                       -- UTM source
  "utm_medium" VARCHAR(300),                       -- UTM medium
  "utm_campaign" VARCHAR(300),                     -- UTM campaign
  "transaction_installments" INTEGER,              -- Parcelas
  "transaction_value" DECIMAL(10, 2) NOT NULL,     -- Valor da transação
  "delivery_state" VARCHAR(50),                    -- Estado de entrega
  "delivery_city" VARCHAR(100),                    -- Cidade de entrega
  "delivery_street" VARCHAR(300),                  -- Rua de entrega
  "delivery_number" VARCHAR(50),                   -- Número do endereço de entrega
  "delivery_complement" VARCHAR(300),              -- Complemento do endereço de entrega
  "delivery_zipcode" VARCHAR(20),                  -- CEP de entrega
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id") -- Relaciona a loja
);

-- Tabela de campanhas de Facebook Ads
CREATE TABLE etl_schema."facebook_ads_campaigns" (
  "id" VARCHAR(100) PRIMARY KEY,
  "name" VARCHAR(100) NOT NULL,
  "campaign_objective" VARCHAR(100) NOT NULL,
  "store_id" INTEGER NOT NULL REFERENCES etl_schema."stores" ("id")
);

-- Tabela de resultados de campanhas de Facebook Ads
CREATE TABLE etl_schema."facebook_ads_campaigns_results" (
  "spend" DECIMAL(10, 2) NOT NULL,
  "results" DECIMAL(10, 2) NOT NULL,
  "cpm" DECIMAL(10, 2) NOT NULL,
  "cpr" DECIMAL(10, 2) NOT NULL,
  "date" DATE NOT NULL,
  "campaign_id" VARCHAR(100) NOT NULL REFERENCES etl_schema."facebook_ads_campaigns" ("id"),
  PRIMARY KEY ("campaign_id", "date") -- Chave primária composta
);
