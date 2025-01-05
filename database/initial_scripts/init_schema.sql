CREATE SCHEMA IF NOT EXISTS etl_schema;

-- Tabela de clientes
CREATE TABLE etl_schema."customers" (
  "id" SERIAL PRIMARY KEY,                      -- Identificador único do cliente
  "name" VARCHAR(100) NOT NULL,                 -- Nome do cliente
  "email" VARCHAR(50) NOT NULL,          -- E-mail único
  "cpf" VARCHAR(14) UNIQUE NOT NULL UNIQUE,               -- CPF com tamanho fixo e obrigatório
  "phone" VARCHAR(20)                           -- Telefone opcional
);

-- Tabela de produtos
CREATE TABLE etl_schema."products" (
  "sku" VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, -- SKU único
  "name" VARCHAR(100) NOT NULL                   -- Nome do produto
);

-- Inserção de produto (para testes)
INSERT INTO etl_schema."products" ("sku", "name")
VALUES ('UNKNOWN', 'UNKNOWN');

-- Tabela de kits
CREATE TABLE etl_schema."kits" (
  "sku" VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, -- SKU do kit único
  "name" VARCHAR(300) NOT NULL                   -- Nome do kit
);

-- Tabela de conteúdo de kits
CREATE TABLE etl_schema."kits_content" (
  "kit_sku" VARCHAR(100) NOT NULL,                 -- Relaciona o kit
  "product_sku" VARCHAR(100) NOT NULL,             -- Relaciona o produto
  "quantity" INTEGER NOT NULL CHECK (quantity > 0),-- Quantidade do produto no kit
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
  "delivery_state" VARCHAR(50),                    -- Estado de entrega
  "utm_source" VARCHAR(300),                       -- UTM source
  "utm_medium" VARCHAR(300),                       -- UTM medium
  "utm_campaign" VARCHAR(300),                     -- UTM campaign
  "transaction_installments" INTEGER,              -- Parcelas
  "transaction_value" DECIMAL(10, 2) NOT NULL      -- Valor da transação
);
