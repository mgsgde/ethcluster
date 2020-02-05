
# Tablename: top40k_var_timediff_receivedtx 

with timestamps_diffs as (
    
    with timestamps_preceding_tx as (
        
        with timestamps_received_tx as (
            select to_address, block_timestamp from `masterarbeit-245718.ethereum_us.traces_sampleData`
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

select to_address, STDDEV_SAMP(timestampdiff) as stddev_sample  
from timestamps_diffs group by to_address

# Tablename: top40k_var_timediff_senttx 

with timestamps_diffs as (
    with timestamps_preceding_tx as (
        
        with timestamps_sent_tx as (
            select from_address, block_timestamp from `masterarbeit-245718.ethereum_us.traces_sampleData`
                where to_address is not null 
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
select from_address, ifnull(STDDEV_SAMP(timestampdiff),0) as stddev_sample  
from timestamps_diffs group by from_address

# Tablename: top40k_features 

select * from `masterarbeit-245718.ethereum_us.top40k_wei` 
inner join `masterarbeit-245718.ethereum_us.top40k_tx` using(address) 
inner join `masterarbeit-245718.ethereum_us.top40k_timediff_txsent` using(address) 
inner join `masterarbeit-245718.ethereum_us.top40k_timediff_txreceived` using(address) 
inner join `masterarbeit-245718.ethereum_us.top40k_minedblocks` using(address) 

# Tablename: top40k_minedblocks 
# umfasst auch uncles

with minedBlocksView as (
    SELECT to_address as address, count(*) as mined_blocks FROM `masterarbeit-245718.ethereum_us.top40k_traces` 
        where trace_type = "reward"
        and status = 1
        group by to_address
    )
select address, ifnull(mined_blocks,0) as minedBlocks from minedBlocksView right join `ethereum_us.top40k_addresses_22_1_2020`  using(address)



# Tablename: top40k_timediff_txreceived 

with timeRecView as (

  with receivedTx as (
    SELECT to_address, count(*) as numberOfTranscationsReceived FROM `masterarbeit-245718.ethereum_us.top40k_traces` 
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by to_address),
  
  timeStampDiffs as (
    SELECT to_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
    FROM `masterarbeit-245718.ethereum_us.top40k_traces`
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by to_address
  
  ) select to_address as address, 
  CASE 
    when (numberOfTranscationsReceived - 1)  > 0 then timestampDiff / (numberOfTranscationsReceived - 1) 
    else 0
  end as avgTimeDiffBetweenReceivedTransactions
  from receivedTx inner join  timeStampDiffs using(to_address)
)

select address, ifnull(avgTimeDiffBetweenReceivedTransactions,0) as avgTimeDiffBetweenReceivedTransactions from timeRecView right join `ethereum_us.top250k_addresses_04_02_20` using(address)

# Tablename: top40k_timediff_txsent 

with timeSentView as (

  with sentTx as (
    SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top40k_traces` 
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by from_address),
  timeStampDiffs as (
    SELECT from_address, TIMESTAMP_DIFF(MAX(block_timestamp), MIN( block_timestamp ), second ) as timestampDiff
    FROM `masterarbeit-245718.ethereum_us.top40k_traces`
    where to_address is not null 
      and status = 1 
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    group by from_address
  ) select from_address as address, 
  CASE 
    when (numberOfTranscationsSent - 1)  > 0 then timestampDiff / (numberOfTranscationsSent - 1) 
    else 0
  end as avgTimeDiffBetweenSentTransactions
     from sentTx inner join  timeStampDiffs using(from_address)
)

select address, ifnull(avgTimeDiffBetweenSentTransactions,0) as avgTimeDiffBetweenSentTransactions from timeSentView right join `ethereum_us.top250k_addresses_04_02_20` using(address)

# Tablename: top40k_tx

with txView as (

  with txSent as (
      SELECT from_address, count(*) as numberOfTranscationsSent FROM `masterarbeit-245718.ethereum_us.top40k_traces` 
      where to_address is not null and status = 1 and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by from_address
    ), txReceived as (
      SELECT to_address, count(*) as numberOfTranscationsReceived FROM `masterarbeit-245718.ethereum_us.top40k_traces` 
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

select address, numberOfTranscationsReceived, numberOfTranscationsSent from txView right join `ethereum_us.top250k_addresses_04_02_20` using(address)

# Tablename: top40k_wei 

with weiView as (

  with weiReceivedView as (
        
      -- debits
      select to_address, sum(ifnull(value, 0)) as weiReceived
      from `ethereum_us.top40k_traces` 
      where to_address is not null
      and status = 1
      and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null) 
      group by to_address
        
  ), weiSentView as (
  
      -- credits
      select from_address, sum(ifnull(value, 0)) as weiSent
      from  `ethereum_us.top40k_traces` 
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
select address, weiReceived, weiSent from weiView right join `ethereum_us.top250k_addresses_04_02_20`  using(address)

# Tablename: top40k_traces

with traces as (
  select *
    from `bigquery-public-data.crypto_ethereum.traces`
    where status = 1
    and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
)
select distinct transaction_hash, transaction_index, from_address, to_address, value, input, output, trace_type, call_type, reward_type, gas, gas_used, subtraces , trace_address, error , status, block_timestamp, block_number, block_hash from traces
  INNER join `masterarbeit-245718.ethereum_us.top250k_addresses_04_02_20` on from_address = address or to_address = address

# Tablename: top40k_addresses_22_1_20

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

