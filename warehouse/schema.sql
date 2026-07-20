-- Criar tipos ENUM
CREATE TYPE condicao_dia_enum AS ENUM ('Amanhecer', 'Pleno dia', 'Anoitecer', 'Plena Noite');
CREATE TYPE tipo_pista_enum AS ENUM ('Simples', 'Multipla', 'Dupla');
CREATE TYPE sentido_via_enum AS ENUM ('Crescente', 'Decrescente');
CREATE TYPE uso_solo_enum AS ENUM ('Sim', 'Nao');
CREATE TYPE sexo_enum AS ENUM ('Masculino', 'Feminino');
CREATE TYPE tipo_envolvido_enum AS ENUM ('Cavaleiro', 'Condutor', 'Passageiro', 'Pedestre', 'Testemunha');
CREATE TYPE estado_fisico_enum AS ENUM ('Ileso', 'Lesoes Graves', 'Lesoes Leves', 'Obito');
CREATE TYPE classificacao_enum AS ENUM ('Com Vitimas Fatais', 'Com Vitimas Feridas', 'Sem Vitimas');

-- Dimensões
CREATE TABLE IF NOT EXISTS dim_pessoa (
    pk_pessoa SERIAL PRIMARY KEY,    -- Chave gerada pelo banco (Autoincremento)
    id_acidente_original INT,        -- Trazido do parquet (row.id)
    pesid_original INT,              -- Trazido do parquet (row.pesid)
    idade INT,
    sexo sexo_enum,
    tipo_envolvido tipo_envolvido_enum,
    estado_fisico estado_fisico_enum,

    -- Essa regra garante que nunca terá a mesma pessoa do mesmo acidente repetida
    UNIQUE(id_acidente_original, pesid_original)
);

CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data_hora TIMESTAMP NOT NULL,
    dia_semana VARCHAR(15),
    fase_dia condicao_dia_enum,

    UNIQUE(data_hora)
);

CREATE TABLE IF NOT EXISTS dim_local (
    id_local SERIAL PRIMARY KEY,
    uf CHAR(2) NOT NULL,
    municipio VARCHAR(50) NOT NULL,
    br INT,
    km FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    regional VARCHAR(100),
    delegacia VARCHAR(50),
    uop VARCHAR(50),

    UNIQUE (uf, municipio)
);

CREATE TABLE IF NOT EXISTS dim_pista (
    id_estrada SERIAL PRIMARY KEY,
    tipo_pista tipo_pista_enum,
    sentido_via sentido_via_enum,
    tracado_via VARCHAR(200),
    uso_solo uso_solo_enum,

    UNIQUE (tipo_pista, sentido_via, tracado_via, uso_solo)
);

CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima SERIAL PRIMARY KEY,
    condicao_meteorologica VARCHAR(100),

    UNIQUE(condicao_meteorologica)
);

CREATE TABLE IF NOT EXISTS dim_classificacao (
    id_classificacao SERIAL PRIMARY KEY,
    tipo_acidente VARCHAR(100),
    causa_acidente VARCHAR(200),
    classificacao_acidente classificacao_enum,

    UNIQUE(tipo_acidente, causa_acidente, classificacao_acidente)
);

CREATE TABLE IF NOT EXISTS dim_veiculo (
    pk_veiculo SERIAL PRIMARY KEY,
    id_acidente_original INT, --recebe id (do acidente) do .parquet
    id_veiculo_original INT, --recebe id_veiculo do .parquet
    tipo_veiculo VARCHAR(100),
    marca VARCHAR(100),
    ano_fabricacao_veiculo INT,
    
    -- Essa regra garante que o mesmo carro de um mesmo acidente seja inserido apenas uma vez na dimensão
    UNIQUE(id_acidente_original, id_veiculo_original)
);
-- Fato (criar DEPOIS de todas as dimensões)
CREATE TABLE IF NOT EXISTS fato (
    id_fato SERIAL PRIMARY KEY,
    fk_pesid INT NOT NULL,
    fk_tempo INT NOT NULL,
    fk_local INT NOT NULL,
    fk_classificacao INT,
    fk_clima INT,
    fk_veiculo INT,
    fk_estrada INT,
    
    UNIQUE(fk_pesid, fk_tempo, fk_local),
    
    FOREIGN KEY (fk_pesid) REFERENCES dim_pessoa(pk_pessoa) ON DELETE RESTRICT,
    FOREIGN KEY (fk_tempo) REFERENCES dim_tempo(id_tempo) ON DELETE RESTRICT,
    FOREIGN KEY (fk_local) REFERENCES dim_local(id_local) ON DELETE RESTRICT,
    FOREIGN KEY (fk_classificacao) REFERENCES dim_classificacao(id_classificacao) ON DELETE SET NULL,
    FOREIGN KEY (fk_clima) REFERENCES dim_clima(id_clima) ON DELETE SET NULL,
    FOREIGN KEY (fk_veiculo) REFERENCES dim_veiculo(pk_veiculo) ON DELETE SET NULL,
    FOREIGN KEY (fk_estrada) REFERENCES dim_pista(id_estrada) ON DELETE SET NULL
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_fato_pesid ON fato(fk_pesid);
CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato(fk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_local ON fato(fk_local);
CREATE INDEX IF NOT EXISTS idx_fato_classificacao ON fato(fk_classificacao);