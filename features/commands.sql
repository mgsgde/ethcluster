with double_entry_book as (
    -- received wei
    select to_address as address, value
    from `bigquery-public-data.crypto_ethereum.traces`
    where to_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    union all
    -- sent wei
    select from_address as address, value
    from `bigquery-public-data.crypto_ethereum.traces`
    where from_address is not null
    and status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
)
select address, sum(value) as balance
from double_entry_book
group by address
order by balance desc
limit 40000

SELECT * from `bigquery-public-data.crypto_ethereum.traces` 
      where DATE(block_timestamp) >= '2020-2-1' 
      and DATE(block_timestamp) <= '2020-2-1'

with traces_clean as (
    select * from `masterarbeit-245718.ethereum_us.traces_sampleData` where 
    status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
    
), tx_received as (
    select 
        TIMESTAMP_TRUNC(block_timestamp, MONTH, 'UTC') as month, 
        to_address,
        count(*) as number_tx_received
    from traces_clean
    group by TIMESTAMP_TRUNC(block_timestamp, MONTH, 'UTC'), to_address
    
), tx_sent as (
    select 
        TIMESTAMP_TRUNC(block_timestamp, MONTH, 'UTC') as month, 
        from_address,
        count(*) as number_tx_sent
    from traces_clean
    group by TIMESTAMP_TRUNC(block_timestamp, MONTH, 'UTC'), from_address
    
), monthly_tx as (
    select 
        CASE  
          WHEN tx_sent.from_address IS NOT NULL THEN tx_sent.from_address
          WHEN tx_received.to_address IS NOT NULL THEN tx_received.to_address
        END AS address,
        CASE  
          WHEN tx_sent.month IS NOT NULL THEN tx_sent.month
          WHEN tx_received.month IS NOT NULL THEN tx_received.month
        END AS month,
        ifnull(number_tx_sent,0) as number_tx_sent, 
        ifnull(number_tx_received,0) as number_tx_received 
    from tx_sent full join tx_received 
        on (tx_sent.from_address = tx_received.to_address and tx_sent.month = tx_received.month)
)

select address, countif(number_tx_sent > 0 or number_tx_received > 0) as active_months from monthly_tx group by address order by address ASC 

SELECT from_address, count(*) OVER (PARTITION BY )

SELECT *
FROM `bigquery-public-data.crypto_ethereum.balances`
WHERE RAND() < 10/(SELECT COUNT(*) FROM `bigquery-public-data.crypto_ethereum.balances`)


# top100k_traces_usd

select * from `masterarbeit-245718.ethereum_us.top100k_traces` as traces inner join `masterarbeit-245718.ethereum_us.usd_eth` as usd_eth 
  on (TIMESTAMP_DIFF(usd_eth.timestamp, traces.block_timestamp, DAY) = 0)


select * from `masterarbeit-245718.ethereum_us.traces_sampleData` as traces left join `masterarbeit-245718.ethereum_us.usd_eth` as usd_eth 
  on (TIMESTAMP_TRUNC(usd_eth.timestamp, DAY, 'UTC') = TIMESTAMP_TRUNC(traces.block_timestamp, DAY, 'UTC'))



with traces as (
  select *
    from `bigquery-public-data.crypto_ethereum.traces`
    where status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    and DATE(block_timestamp) >= '2019-1-1' 
    and DATE(block_timestamp) <= '2020-1-1'

)
select distinct transaction_hash, transaction_index, from_address, to_address, value, input, output, trace_type, call_type, reward_type, gas, gas_used, subtraces , trace_address, error , status, block_timestamp, block_number, block_hash from traces
  INNER join `masterarbeit-245718.ethereum_us.top250k_addresses_04_02_20` on from_address = address or to_address = address


## temporary (neue adressen erstellen)

with txSent as (
      SELECT from_address, count(*) as numberOfTranscationsSent FROM `bigquery-public-data.crypto_ethereum.traces` 
      where to_address is not null 
      and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      and DATE(block_timestamp) >= '2019-1-1' 
      and DATE(block_timestamp) <= '2020-1-1'
      group by from_address
    ), txReceived as (
      SELECT to_address, count(*) as numberOfTranscationsReceived FROM `bigquery-public-data.crypto_ethereum.traces`
      where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      and DATE(block_timestamp) >= '2019-1-1' 
      and DATE(block_timestamp) <= '2020-1-1'
      group by to_address
    ) 
    SELECT 
    CASE  
      WHEN to_address IS NOT NULL THEN to_address
      WHEN from_address IS NOT NULL THEN from_address
    END AS address,
    IFNULL(numberOfTranscationsReceived, 0) as numberOfTranscationsReceived, 
    IFNULL(numberOfTranscationsSent, 0) as numberOfTranscationsSent
    from txReceived FULL OUTER JOIN txSent on to_address = from_address order by numberOfTranscationsSent + numberOfTranscationsReceived DESC LIMIT 30000


# Tablename: top100k_contracts 

with contractsView as(
  select address, is_erc20, is_erc721, true as is_contract from `ethereum_us.top100k_addresses_05_02_20` inner join `bigquery-public-data.crypto_ethereum.contracts` using(address) 
)
select address, is_erc20, is_erc721, is_contract from contractsView right join `ethereum_us.top100k_addresses_05_02_20`

# Tablename: top100k_var_timediff_receivedtx 

with timestamp_var as (
    
    with timestamps_diffs as (
        
        with timestamps_preceding_tx as (
            
            with timestamps_received_tx as (
                select to_address, block_timestamp from `masterarbeit-245718.ethereum_us.top100k_traces`
                    where to_address is not null 
                      and status = 1 
                      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
            )
            
            select to_address, block_timestamp,
                lag(block_timestamp) OVER (partition by to_address order by block_timestamp asc) as preceding_block_timestamp 
            from timestamps_received_tx
        )
        
        select to_address, block_timestamp, preceding_block_timestamp, 
            TIMESTAMP_DIFF(block_timestamp, preceding_block_timestamp, second) as timestampdiff
        from timestamps_preceding_tx
    )
    select to_address as address, STDDEV_SAMP(timestampdiff) as var_timediff_receivedtx
    from timestamps_diffs group by to_address 
) 
select address, var_timediff_receivedtx  from timestamp_var right join `ethereum_us.top100k_addresses_05_02_20` using(address)

# Tablename: top100k_var_timediff_senttx 

with timestamp_var as (
    with timestamps_diffs as (
        
        with timestamps_preceding_tx as (
            
            with timestamps_sent_tx as (
                select from_address, block_timestamp from `masterarbeit-245718.ethereum_us.top100k_traces`
                    where from_address is not null 
                      and status = 1 
                      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
            )
            
            select from_address, block_timestamp,
                lag(block_timestamp) OVER (partition by from_address order by block_timestamp asc) as preceding_block_timestamp 
            from timestamps_sent_tx
        )
        
        select from_address, block_timestamp, preceding_block_timestamp, 
            TIMESTAMP_DIFF(block_timestamp, preceding_block_timestamp, second) as timestampdiff
        from timestamps_preceding_tx
    )
    select from_address as address, STDDEV_SAMP(timestampdiff) as var_timediff_senttx  
    from timestamps_diffs group by from_address 
) 
select address, var_timediff_senttx from timestamp_var right join `ethereum_us.top100k_addresses_05_02_20` using(address)

# Tablename: top100k_features 

select * from `masterarbeit-245718.ethereum_us.top100k_wei` 
inner join `masterarbeit-245718.ethereum_us.top100k_tx` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_timediff_txsent` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_timediff_txreceived` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_minedblocks` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_var_timediff_receivedtx` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_var_timediff_senttx` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_contracts` using(address) 
inner join `masterarbeit-245718.ethereum_us.top100k_addresses_05_02_20` using(address) 

# Tablename: top100k_minedblocks 
# umfasst auch uncles

with minedBlocksView as (
    SELECT to_address as address, count(*) as mined_blocks FROM `masterarbeit-245718.ethereum_us.top100k_traces` 
        where trace_type = "reward"
        and status = 1
        group by to_address
    )
select address, ifnull(mined_blocks,0) as mined_blocks from minedBlocksView right join `ethereum_us.top100k_addresses_05_02_20`  using(address)



# Tablename: top100k_timediff_txreceived 

with timeRecView as (

  with receivedTx as (
    SELECT to_address, count(*) as numberOfTranscationsReceived FROM `masterarbeit-245718.ethereum_us.top100k_traces` 
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by to_address),
  
  timeStampDiffs as (
    SELECT to_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
    FROM `masterarbeit-245718.ethereum_us.top100k_traces`
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by to_address
  
  ) select to_address as address, 
  CASE 
    when (numberOfTranscationsReceived - 1)  > 0 then timestampDiff / (numberOfTranscationsReceived - 1) 
    else 0
  end as avg_time_diff_received_tx
  from receivedTx inner join  timeStampDiffs using(to_address)
)

select address, ifnull(avg_time_diff_received_tx,0) as avg_time_diff_received_tx from timeRecView right join `ethereum_us.top100k_addresses_05_02_20` using(address)

# Tablename: top100k_timediff_txsent 

with timeSentView as (

  with sentTx as (
    SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top100k_traces` 
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by from_address),
  timeStampDiffs as (
    SELECT from_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
    FROM `masterarbeit-245718.ethereum_us.top100k_traces`
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by from_address
  ) select from_address as address, 
  CASE 
    when (numberOfTranscationsSent - 1)  > 0 then timestampDiff / (numberOfTranscationsSent - 1) 
    else 0
  end as avg_time_diff_sent_tx
     from sentTx inner join  timeStampDiffs using(from_address)
)

select address, ifnull(avg_time_diff_sent_tx,0) as avg_time_diff_sent_tx from timeSentView right join `ethereum_us.top100k_addresses_05_02_20` using(address)

# Tablename: top100k_tx

with txView as (

  with txSent as (
      SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top100k_traces` 
      where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by from_address
    ), txReceived as (
      SELECT to_address, count(*) as numberOfTranscationsReceived FROM `masterarbeit-245718.ethereum_us.top100k_traces` 
      where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by to_address
    ) 
    SELECT 
    CASE  
      WHEN to_address IS NOT NULL THEN to_address
      WHEN from_address IS NOT NULL THEN from_address
    END AS address,
    IFNULL(numberOfTranscationsReceived, 0) as numberOfTranscationsReceived, 
    IFNULL(numberOfTranscationsSent, 0) as numberOfTranscationsSent
    from txReceived FULL OUTER JOIN txSent on to_address = from_address
) 

select address, numberOfTranscationsReceived, numberOfTranscationsSent from txView right join `ethereum_us.top100k_addresses_05_02_20` using(address)

# Tablename: top100k_wei 

with weiView as (

  with weiReceivedView as (
        
      -- debits
      select to_address, sum(ifnull(value, 0)) as weiReceived
      from `ethereum_us.top100k_traces` 
      where to_address is not null
      and status = 1
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by to_address
        
  ), weiSentView as (
  
      -- credits
      select from_address, sum(ifnull(value, 0)) as weiSent
      from  `ethereum_us.top100k_traces` 
      where from_address is not null
      and status = 1
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by from_address
  ) 
  select 
  CASE 
    when to_address is not null then to_address
    when from_address is not null then from_address
  end as address, 
  ifnull(weiReceived,0) as weiReceived, 
  ifnull(weiSent,0) as weiSent
  from weiReceivedView full outer join weiSentView on from_address = to_address
) 
select address, weiReceived, weiSent from weiView right join `ethereum_us.top100k_addresses_05_02_20`  using(address)

# Tablename: top100k_traces

with traces as (
  select *
    from `bigquery-public-data.crypto_ethereum.traces`
    where status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
)
select distinct transaction_hash, transaction_index, from_address, to_address, value, input, output, trace_type, call_type, reward_type, gas, gas_used, subtraces , trace_address, error , status, block_timestamp, block_number, block_hash from traces
  INNER join `masterarbeit-245718.ethereum_us.top100k_addresses_05_02_20` on from_address = address or to_address = address

# Tablename: top100k_addresses_22_1_20

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

