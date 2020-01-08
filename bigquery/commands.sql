

-- vorläufige feature Tabelle 

select * from `masterarbeit-245718.ethereum_us.top40k_week1777_features_weiSent_weiReceived` 
inner join `masterarbeit-245718.ethereum_us.top40k_week1777_features_txSent_txReceived`  using(address) 
inner join `masterarbeit-245718.ethereum_us.top40k_week1777_features_avgTimeDiffBetweenSentTx` using(address) 
inner join `masterarbeit-245718.ethereum_us.top40k_week1777_features_avgTimeDiffBetweenReceivedTx` using(address) 

-- avg time between received transcations

with receivedTx as (
  SELECT to_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces` 
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
  group by to_address),
timeStampDiffs as (
  SELECT to_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
  FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces`
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
  group by to_address
) select to_address as address, 
CASE 
  when (numberOfTranscationsSent - 1)  > 0 then timestampDiff / (numberOfTranscationsSent - 1) 
  else 0
end as avgTimeDiffBetweenReceivedTransactions
   from receivedTx inner join  timeStampDiffs using(to_address)

-- avg time between sent transcations

with sentTx as (
  SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces` 
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
  group by from_address),
timeStampDiffs as (
  SELECT from_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
  FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces`
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
  group by from_address
) select from_address as address, 
CASE 
  when (numberOfTranscationsSent - 1)  > 0 then timestampDiff / (numberOfTranscationsSent - 1) 
  else 0
end as avgTimeDiffBetweenSentTransactions
   from sentTx inner join  timeStampDiffs using(from_address)


-- todo median berechnen

with dailySentTx as (select from_address, DATETIME_TRUNC(DATETIME(block_timestamp), DAY) as day1, count("hash") as txSent from `masterarbeit-245718.ethereum_us.top40k_week1777_transactions`  group by DATETIME_TRUNC(DATETIME(block_timestamp), DAY), from_address),
dailyReceivedTx as (select to_address, DATETIME_TRUNC(DATETIME(block_timestamp), DAY) as day2, count("hash") as txReceived from `masterarbeit-245718.ethereum_us.top40k_week1777_transactions` group by DATETIME_TRUNC(DATETIME(block_timestamp), DAY), to_address)
SELECT 
CASE 
    WHEN to_address IS NOT NULL THEN to_address
    WHEN from_address IS NOT NULL THEN from_address
END AS address,
CASE
  when day1 is not null then day1
  when day2 is not null then day2
end as day,
IFNULL(txReceived, 0) as txReceived, 
IFNULL(txSent, 0) as txSent
from dailySentTx FULL OUTER JOIN dailyReceivedTx on to_address = from_address AND day1 = day2
order by IFNULL(txReceived, 0) + IFNULL(txSent, 0) DESC

-- tx received, tx sent

with txSent as (
  SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces` 
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
  group by from_address
), txReceived as (
  SELECT to_address, count(*) as numberOfTranscationsReceived FROM `masterarbeit-245718.ethereum_us.top40k_week1777_traces` 
  where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
  group by to_address
) SELECT 
CASE  
  WHEN to_address IS NOT NULL THEN to_address
  WHEN from_address IS NOT NULL THEN from_address
END AS address,
IFNULL(numberOfTranscationsReceived, 0) as numberOfTranscationsReceived, 
IFNULL(numberOfTranscationsSent, 0) as numberOfTranscationsSent
from txReceived FULL OUTER JOIN txSent on to_address = from_address


-- wei sent and wei received

  with weiReceivedView as (
    -- debits
    select to_address, sum(ifnull(value, 0)) as weiReceived
    from `masterarbeit-245718.ethereum_us.top40k_week1777_traces`
    where to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by to_address
), weiSentView as (
    -- credits
    select from_address, sum(ifnull(value, 0)) as weiSent
    from `masterarbeit-245718.ethereum_us.top40k_week1777_traces`
    where from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by from_address
) select 
CASE 
  when to_address is not null then to_address
  when from_address is not null then from_address
end as address, ifnull(weiReceived,0) as weiReceived, ifnull(weiSent,0) as weiSent
from weiReceivedView full outer join weiSentView on from_address = to_address

-- used to create tables

SELECT from_address
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE RAND() < 40000/(SELECT COUNT(*) FROM `bigquery-public-data.crypto_ethereum.transactions`)
union all 
SELECT to_address
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE RAND() < 40000/(SELECT COUNT(*) FROM `bigquery-public-data.crypto_ethereum.transactions`)


with blocks as (select *
from `bigquery-public-data.crypto_ethereum.blocks`
where DATE(`timestamp`) >= '2019-07-1' AND DATE(`timestamp`) <= '2019-7-7')
select `timestamp`, `number`, `hash`, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, transaction_count
from blocks
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` on address = miner


select address, bytecode, function_sighashes, is_erc20, is_erc721, block_timestamp, block_number, block_hash
 from `masterarbeit-245718.ethereum_us.top40k_addresses` 
inner join `bigquery-public-data.crypto_ethereum.contracts` using(address)

with logs as (select *
from `bigquery-public-data.crypto_ethereum.logs`
where DATE(block_timestamp) >= '2019-07-1' AND DATE(block_timestamp) <= '2019-7-7')
select log_index,transaction_hash,transaction_index,address,data,topics,block_timestamp,block_number,block_hash from logs
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` using(address)

with tokens as (select *
from `bigquery-public-data.crypto_ethereum.tokens`)
select address, symbol, name, decimals, total_supply from tokens
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` using(address)


with token_transfers as (select *
from `bigquery-public-data.crypto_ethereum.token_transfers`
where DATE(block_timestamp) >= '2019-07-1' AND DATE(block_timestamp) <= '2019-7-7')
select distinct token_address,  from_address,   to_address, value,  transaction_hash,   log_index,  block_timestamp ,block_number,  block_hash from token_transfers
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` on from_address = address or to_address = address


with transactions as (select *
from `bigquery-public-data.crypto_ethereum.transactions`
where receipt_status = 1
and DATE(block_timestamp) >= '2019-07-1' AND DATE(block_timestamp) <= '2019-7-7')
select distinct `hash`, nonce, transaction_index, from_address, to_address, value, gas, gas_price, input, receipt_cumulative_gas_used, receipt_gas_used, receipt_contract_address, receipt_root, receipt_status, block_timestamp, block_number, block_hash from transactions
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` on from_address = address or to_address = address

with traces as (select *
from `bigquery-public-data.crypto_ethereum.traces`
where from_address is not null
and status = 1
and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
and DATE(block_timestamp) >= '2019-07-1' AND DATE(block_timestamp) <= '2019-7-7')
select distinct transaction_hash, transaction_index, from_address, to_address, value, input, output, trace_type, call_type, reward_type, gas, gas_used, subtraces , trace_address, error , status, block_timestamp, block_number, block_hash from traces
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` on from_address = address or to_address = address

-- end


with traces as (select * from `bigquery-public-data.crypto_ethereum.traces`
where status = 1
and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
and DATE(block_timestamp) >= '2019-07-12' AND DATE(block_timestamp) <= '2019-12-31')
select transaction_hash, transaction_index, from_address, to_address, value, input, output, trace_type, call_type, reward_type, gas, gas_used, subtraces , trace_address, error , status, block_timestamp, block_number, block_hash from traces
INNER join `masterarbeit-245718.ethereum_us.top40k_addresses` on from_address = address or to_address = address



-- https://github.com/blockchain-etl/ethereum-etl-airflow/blob/master/sqls/ether_balances.sql
#standardSQL
-- MIT License
-- Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
with double_entry_book as (
    -- debits
    select to_address as address, value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- credits
    select from_address as address, -value as value
    from `bigquery-public-data.crypto_ethereum.traces`
    where from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- transaction fees debits
    select miner as address, sum(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value
    from `bigquery-public-data.crypto_ethereum.transactions` as transactions
    join `bigquery-public-data.crypto_ethereum.blocks` as blocks on blocks.number = transactions.block_number
    group by blocks.miner
    union all
    -- transaction fees credits
    select from_address as address, -(cast(receipt_gas_used as numeric) * cast(gas_price as numeric)) as value
    from `bigquery-public-data.crypto_ethereum.transactions`
)
select address, sum(value) as balance
from double_entry_book
group by address
order by balance desc
limit 40000


 -- Saved Query: Launcher Q2.1 - ERC-20 daily transaction data ($OMG)  
SELECT 
  from_address AS Source,
  to_address as Target,
  CAST(value AS NUMERIC)/POWER(10,18) AS Weight
FROM 
  `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers, 
  `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
WHERE token_transfers.block_number = blocks.number
AND token_transfers.token_address = '0xd26114cd6ee289accf82350c8d8487fedb8a0c07'
ORDER BY timestamp
LIMIT 50000


 -- Saved Query: Launcher Q2 - Top 10 ERC20 Contracts  
SELECT contracts.address, COUNT(1) AS tx_count
FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
JOIN `bigquery-public-data.ethereum_blockchain.transactions` AS transactions ON (transactions.to_address = contracts.address)
WHERE contracts.is_erc20 = TRUE
GROUP BY contracts.address
ORDER BY tx_count DESC
LIMIT 10


-- Saved Query: Launcher Q0 - Ethereum transactions and gas price per day
SELECT 
  SUM(value/POWER(10,18)) AS sum_tx_ether,
  AVG(gas_price*(receipt_gas_used/POWER(10,18))) AS avg_tx_gas_cost,
  DATE(timestamp) AS tx_date
FROM
  `bigquery-public-data.ethereum_blockchain.transactions` AS transactions,
  `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
WHERE TRUE
  AND transactions.block_number = blocks.number
  AND receipt_status = 1
  AND value > 0
GROUP BY tx_date
HAVING tx_date >= '2018-01-01' AND tx_date <= '2018-12-31'
ORDER BY tx_date

-- Saved Query: Launcher Q1 - Top 10 ERC721 Contracts  
SELECT contracts.address, COUNT(1) AS tx_count
FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
JOIN `bigquery-public-data.ethereum_blockchain.transactions` AS transactions ON (transactions.to_address = contracts.address)
WHERE contracts.is_erc721 = TRUE
GROUP BY contracts.address
ORDER BY tx_count DESC
LIMIT 10

-- Saved Query: Q3 contract similarity search
CREATE TEMPORARY FUNCTION jaccard(v1 ARRAY<STRING>,v2 ARRAY<STRING>)
RETURNS FLOAT64
LANGUAGE js AS """
  var u1 = {};
  var u2 = {};
  var uu = {};
  for (var i = 0 ; i < v1.length; i++) { u1[v1[i]] = 1; uu[v1[i]] = 1 }
  for (var i = 0 ; i < v2.length; i++) { u2[v2[i]] = 1; uu[v2[i]] = 1 }
  var numerator = 0.0;
  for (var k in uu) { if (u1[k] == u2[k]) { numerator++ } }
  var denominator = Object.keys(uu).length;
  return numerator/denominator;
""";

CREATE TEMPORARY FUNCTION Levenshtein(a STRING, b STRING)
RETURNS FLOAT64
LANGUAGE js AS """
  var n = a.length;
  var m = b.length;
  if ( n > m ) {
    // Make sure n <= m, to use O(min(n,m)) space
    var c = a; a = b; b = c;
    var o = n; n = m; m = o;
  }

  var cur = [...Array(n+1).keys()];
  var o1 = [...Array(m+1).keys()];
  o1.shift();

  for (k1 in o1) {
    var i = o1[k1];
    
    var prv = cur;
    cur = [i];
    for (i in [...Array(n).keys()]) { cur.push(0); }
    
    var o2 = [...Array(n+1).keys()];
    o2.shift();
    
    for (k2 in o2) {
      var j = o2[k2];

      var add = prv[j]+1;
      var del = cur[j-1]+1;

      var chg = prv[j-1];

      if ( a[j-1] != b[i-1] ) {
        chg = chg + 1;
      }

      cur[j] = add < del ? add : del;
      cur[j] = cur[j] < chg ? cur[j] : chg;
    }
  }
  return cur[n];
""";

/*
SELECT source_address,target_address,Levenshtein(a.bytecode,b.bytecode) AS distance
FROM
(SELECT address AS source_address,bytecode FROM `ethereum-etl-dev.ethereum_blockchain.contracts` WHERE address = '0xf97e0a5b616dffc913e72455fde9ea8bbe946a2b') AS a,
(SELECT address AS target_address,bytecode FROM `ethereum-etl-dev.ethereum_blockchain.contracts`) AS b

ORDER BY distance ASC
*/

SELECT address,d,function_count FROM (
  SELECT
    address,
    jaccard(
      (SELECT function_sighashes 
       FROM `bigquery-public-data.ethereum_blockchain.contracts` 
       WHERE address = '0xf97e0a5b616dffc913e72455fde9ea8bbe946a2b'),
       function_sighashes
    ) AS d,
    ARRAY_LENGTH(function_sighashes) AS function_count,
    sighash  
  FROM 
    `bigquery-public-data.ethereum_blockchain.contracts` JOIN UNNEST(function_sighashes) AS sighash
) AS distances
--LEFT JOIN `ethereum_aux.4byte_directory` AS methods ON distances.sighash = methods.function_4byte
WHERE distances.d > 0
ORDER BY 
  d DESC
  ,address 
  --, function_signature
LIMIT 500

