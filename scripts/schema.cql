CREATE KEYSPACE [MY_KEYSPACE_NAME] WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2};

USE [MY_KEYSPACE_NAME];

CREATE TYPE tx_input_output (
    address list<text>,
    value bigint,
    address_type smallint
);

CREATE TYPE tx_summary (
    tx_hash blob,
    no_inputs int,
    no_outputs int,
    total_input bigint,
    total_output bigint
);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    eur float,
    usd float
);

CREATE TABLE transaction (
    tx_prefix text,
    tx_hash blob,
    coinbase boolean,
    coinjoin boolean,
    height int,
    inputs list<frozen<tx_input_output>>,
    outputs list<frozen<tx_input_output>>,
    timestamp int,
    total_input bigint,
    total_output bigint,
    tx_index bigint,
    PRIMARY KEY (tx_prefix, tx_hash)
);

CREATE TABLE summary_statistics (
    id text PRIMARY KEY,
    no_blocks int,
    no_txs bigint,
    timestamp int
);

CREATE TABLE block (
    height int PRIMARY KEY,
    block_hash blob,
    no_transactions int,
    timestamp int
);

CREATE TABLE block_transactions (
    height int PRIMARY KEY,
    txs list<frozen<tx_summary>>
);
