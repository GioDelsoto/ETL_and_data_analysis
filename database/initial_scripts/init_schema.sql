CREATE SCHEMA IF NOT EXISTS app_schema;

-- Tabela de clientes
CREATE TABLE app_schema."customers" (
  "id" SERIAL PRIMARY KEY,                -- Identificador único do cliente
  "name" VARCHAR(100) NOT NULL,           -- Nome do cliente
  "email" VARCHAR(50) UNIQUE NOT NULL,    -- E-mail único
  "cpf" CHAR(14) UNIQUE NOT NULL,         -- CPF único e obrigatório
  "phone" VARCHAR(20)                     -- Telefone opcional
);

-- Tabela de produtos
CREATE TABLE app_schema."products" (
  "id" SERIAL PRIMARY KEY,                -- Identificador único do produto
  "sku" VARCHAR(30) UNIQUE NOT NULL,      -- SKU único
  "name" VARCHAR(100)           -- Nome do produto
);

-- Tabela de kits
CREATE TABLE app_schema."kits" (
  "id" SERIAL PRIMARY KEY,                -- Identificador único do kit
  "sku" VARCHAR(30) UNIQUE NOT NULL,      -- SKU do kit único
  "name" VARCHAR(100)           -- Nome do kit
);

-- Tabela de conteúdo de kits
CREATE TABLE app_schema."kits_content" (
  "kit_id" INTEGER NOT NULL,              -- Relaciona o kit
  "product_id" INTEGER NOT NULL,          -- Relaciona o produto
  "quantity" INTEGER NOT NULL CHECK (quantity > 0), -- Quantidade do produto no kit
  PRIMARY KEY ("kit_id", "product_id"),   -- Chave composta para evitar duplicações
  FOREIGN KEY ("kit_id") REFERENCES app_schema."kits" ("id"),
  FOREIGN KEY ("product_id") REFERENCES app_schema."products" ("id")
);

-- Tabela de pedidos
CREATE TABLE app_schema."orders" (
  "id" SERIAL PRIMARY KEY,                -- Identificador único da linha do pedido
  "order_id" INTEGER NOT NULL,            -- Identificador do pedido principal
  "order_date" TIMESTAMPTZ NOT NULL,      -- Data do pedido em UTC
  "customer_id" INTEGER NOT NULL,         -- Relaciona o cliente
  "status" VARCHAR(50) NOT NULL,          -- Status do pedido
  "payment_method" VARCHAR(50),           -- Método de pagamento
  "kit_id" INTEGER REFERENCES app_schema."kits" ("id"), -- Relaciona o kit
  "quantity" INTEGER NOT NULL CHECK (quantity > 0), -- Quantidade
  "total_value" DECIMAL(10, 2) NOT NULL CHECK (total_value >= 0), -- Valor total
  "total_product" DECIMAL(10, 2) NOT NULL CHECK (total_product >= 0), -- Valor dos produtos
  "total_shippment" DECIMAL(10, 2) NOT NULL CHECK (total_ship >= 0), -- Valor do frete
  "coupom_code" VARCHAR(50),              -- Código do cupom
  "coupom_value" DECIMAL(10, 2),          -- Valor do cupom
  "delivery_state" VARCHAR(50),           -- Estado de entrega
  "utm_source" VARCHAR(200),              -- UTM source
  "utm_medium" VARCHAR(200),              -- UTM medium
  "utm_campaign" VARCHAR(200),            -- UTM campaign
  "transaction_installments" INTEGER CHECK (transaction_installments > 0), -- Parcelas
  "transaction_value" DECIMAL(10, 2) NOT NULL CHECK (transaction_value >= 0), -- Valor da transação
  FOREIGN KEY ("customer_id") REFERENCES app_schema."customers" ("id"),
  FOREIGN KEY ("order_id") REFERENCES app_schema."orders" ("id") -- Relaciona ao pedido principal
);
