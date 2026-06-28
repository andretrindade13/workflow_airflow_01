CREATE TABLE IF NOT EXISTS analise_produtos (
    categoria VARCHAR(255) PRIMARY KEY,
    preco_medio FLOAT,
    preco_minimo FLOAT,
    preco_maximo FLOAT,
    quantidade_produtos INT
);