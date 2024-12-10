CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    gender VARCHAR(10),
    address TEXT,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    per_capita_income VARCHAR(20),
    yearly_income VARCHAR(20),
    total_debt VARCHAR(20),
    credit_score INT,
    num_credit_cards INT
);

CREATE TABLE cards (
    id SERIAL PRIMARY KEY,
    client_id INT REFERENCES users(id),
    card_brand VARCHAR(50),
    card_type VARCHAR(50),
    expires VARCHAR(7),
    cvv VARCHAR(4),
    has_chip VARCHAR(3),
    num_cards_issued INT,
    credit_limit VARCHAR(20),
    acct_open_date VARCHAR(10),
    year_pin_last_changed INT,
    card_on_dark_web VARCHAR(3)
);

CREATE TABLE merchants (
    id SERIAL PRIMARY KEY,
    merchant_city VARCHAR(100),
    merchant_state VARCHAR(50),
    zip VARCHAR(10),
    mcc VARCHAR(10)
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    client_id INT REFERENCES users(id),
    card_id INT REFERENCES cards(id),
	merchant_id INT REFERENCES merchants(id),
    amount DECIMAL(10, 2)  
);


CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    nome_tabela VARCHAR(50) NOT NULL,
    id_afetado INT,
    tipo_movimentacao VARCHAR(10) NOT NULL,
    hora_movimentacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);


-- triggers para insercao automatica na tabela logs

create or replace function registrar_log()
returns trigger as $$
begin
    if (tg_op = 'insert') then  -- se a acao for insert
        insert into logs (nome_tabela, id_afetado, tipo_movimentacao)
        values (tg_table_name, new.id, 'insert');
        return new;
    elsif (tg_op = 'update') then  -- se a acao for update
        insert into logs (nome_tabela, id_afetado, tipo_movimentacao)
        values (tg_table_name, new.id, 'update');
        return new;
    elsif (tg_op = 'delete') then  -- se a acao for delete
        insert into logs (nome_tabela, id_afetado, tipo_movimentacao)
        values (tg_table_name, old.id, 'delete');
        return old;
    end if;
    return null; 
end;
$$ language plpgsql;

-- agora, a funcao generica registrar_log aplicada a cada uma das tabelas
-- users
create trigger log_users
after insert or update or delete on users
for each row
execute function registrar_log();

-- cards
create trigger log_cards
after insert or update or delete on cards
for each row
execute function registrar_log();

-- merchants
create trigger log_merchants
after insert or update or delete on merchants
for each row
execute function registrar_log();

-- transactions
create trigger log_transactions
after insert or update or delete on transactions
for each row
execute function registrar_log();



-- TESTES DA AUTOMACAO DO BANCO
-- users
update users
set address = 'Main Street 444'
where id = 825;

select * from logs

-- cards
update cards
set card_type = 'Credit', cvv = '921', has_chip = 'NO'
where id = 4524;

select * from logs

-- merchants
insert into merchants (id, merchant_city, merchant_state, zip, mcc)
values (230000, 'Belem', 'PA', 66040140, 1111)

select * from logs

-- transactions
delete from transactions where id = 7475327

select * from logs