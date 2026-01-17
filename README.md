# Big-Data-Ethereum-Analysis
A coursework on Ethereum Analysis, as a part of curriculum at Queen Mary University of London.

Analysis of Ethereum Transactions and Smart Contracts using Apache Spark

## Dataset overview
Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

## Dataset Schema - blocks
number: The block number

hash: Hash of the block

parent_hash: Hash of the parent of the block

nonce: Nonce that satisfies the difficulty target

sha3_uncles: Combined has of all uncles for a given parent

logs_bloom: Data structure containing event logs

transactions_root: Root hash of the transactions in the payload

state_root: Root hash of the state object

receipts_root: hash of the transaction receipts tree

miner: The address of the beneficiary to whom the mining rewards were given

difficulty: Integer of the difficulty for this block

total_difficulty: Total difficulty of the chain until this block

size: The size of this block in bytes

extra_data: Arbitrary additional data as raw bytes

gas_limit: The maximum gas allowed in this block

gas_used: The total used gas by all transactions in this block

timestamp: The timestamp for when the block was collated

transaction_count: The number of transactions in the block

base_fee_per_gas: Base fee value

## Dataset Schema - transactions
hash: Hash of the block

nonce: Nonce that satisfies the difficulty target

block_hash: Hash of the block where the transaction is in

block_number: Block number where this transaction was in

transaction_index: Transactions index position in the block.

from_address: Address of the sender

to_address: Address of the receiver. null when it is a contract creation transaction

value: Value transferred in Wei (the smallest denomination of ether)

gas: Gas provided by the sender

gas_price : Gas price provided by the sender in Wei

input: Extra data for Ethereum functions

block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

max_fee_per_gas: Sum of base fee and max priority fee

max_priority_fee_per_gas: Tip for mining the transaction

transaction_type: Value used to indicate if the transaction is related to a contract or other specialised transaction

## Dataset Schema - contracts
address: Address of the contract

bytecode: Code for Ethereum Contract

function_sighashes: Function signature hashes of a contract

is_erc20: Whether this contract is an ERC20 contract

is_erc721: Whether this contract is an ERC721 contract

block_number: Block number where this contract was created

## Dataset Schema - scams.json
id: Unique ID for the reported scam

name: Name of the Scam

url: Hosting URL

coin: Currency the scam is attempting to gain

category: Category of scam - Phishing, Ransomware, Trust Trade, etc.

subcategory: Subdivisions of Category

description: Description of the scam provided by the reporter and datasource

addresses: List of known addresses associated with the scam

reporter: User/company who reported the scam first

ip: IP address of the reporter

status: If the scam is currently active, inactive or has been taken offline

## Part A. Time Analysis (25%)
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

## Part B. Top Ten Most Popular Services (25%)
Evaluate the top 10 smart contracts by total Ether received. You will need to join address field in the contracts dataset to the to_address in the transactions dataset to determine how much ether a contract has received.

## Part C. Top Ten Most Active Miners (10%)
Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.

## Part D. Data exploration (40%)
### Popular Scams
Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certain known scams going offline/inactive? To obtain the marks for this catergory you should provide the id of the most lucrative scam and a graph showing how the ether received has changed over time for the dataset. (20/40)

### Gas Guzzlers
For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B. To obtain these marks you should provide a graph showing how gas price has changed over time, a graph showing how gas_used for contract transactions has changed over time and identify if the most popular contracts use more or less than the average gas_used.(15/40)
