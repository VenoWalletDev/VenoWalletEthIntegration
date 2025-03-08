import os
import json
import base64
from web3 import Web3
from eth_account import Account
from typing import Dict, Optional, List
import sqlite3
from cryptography.fernet import Fernet
import logging
from dotenv import load_dotenv
import uuid
from datetime import datetime

# Set up logging at the module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CryptoWallet:
    def __init__(self, rpc_url: str, db_path: str = "wallets.db"):
        """Initialize wallet system with Ethereum RPC and database"""
        # Validate RPC URL
        if not rpc_url or not isinstance(rpc_url, str):
            raise ValueError("Invalid RPC URL")
        
        # Connect to Ethereum node
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {rpc_url}")
            
        self.logger = logger  # Assign the module-level logger to the instance
        self.logger.info("Connected to Ethereum node successfully")
        self.db_path = db_path
        self._setup_database()
        self.encryption_key = self._load_or_create_encryption_key()
        self.cipher = Fernet(self.encryption_key)
        
    def _setup_database(self):
        """Initialize SQLite database with wallets and transactions tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wallets (
                    user_id TEXT PRIMARY KEY,
                    address TEXT NOT NULL UNIQUE,
                    encrypted_private_key TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_hash TEXT PRIMARY KEY,
                    user_id TEXT,
                    recipient TEXT,
                    amount REAL,
                    timestamp INTEGER,
                    status TEXT,
                    FOREIGN KEY (user_id) REFERENCES wallets(user_id)
                )
            """)
            conn.commit()
            self.logger.info("Database initialized")

    def _load_or_create_encryption_key(self) -> bytes:
        """Load or create encryption key for private keys"""
        key_file = "encryption.key"
        if os.path.exists(key_file):
            with open(key_file, "rb") as f:
                return f.read()
        key = Fernet.generate_key()
        with open(key_file, "wb") as f:
            f.write(key)
        self.logger.info("New encryption key generated")
        return key
    
    def create_wallet(self, user_id: str) -> Optional[Dict]:
        """Create a new wallet for a unique user_id"""
        try:
            # Validate user_id
            if not user_id or not isinstance(user_id, str):
                raise ValueError("Invalid user_id")
            
            # Check if wallet already exists
            if self.get_wallet_info(user_id):
                self.logger.warning(f"Wallet for user {user_id} already exists")
                return None

            # Generate new account
            account = Account.create()
            encrypted_key = self.cipher.encrypt(account.key.hex().encode())
            encrypted_key_str = base64.b64encode(encrypted_key).decode('utf-8')
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO wallets (user_id, address, encrypted_private_key) VALUES (?, ?, ?)",
                    (user_id, account.address, encrypted_key_str)
                )
                conn.commit()
            
            self.logger.info(f"Created wallet for user {user_id} with address {account.address}")
            return {
                "user_id": user_id,
                "address": account.address,
                "balance": self.get_balance(account.address),
                "created_at": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error creating wallet for {user_id}: {e}")
            return None
    
    def get_balance(self, address: str) -> float:
        """Get wallet balance in ETH with retry logic"""
        try:
            if not self.web3.is_address(address):
                raise ValueError("Invalid Ethereum address")
            balance_wei = self.web3.eth.get_balance(address)
            return float(self.web3.from_wei(balance_wei, 'ether'))
        except Exception as e:
            self.logger.error(f"Error getting balance for {address}: {e}")
            return 0.0
    
    def send_transaction(self, user_id: str, recipient: str, amount_eth: float) -> Dict:
        """Send ETH transaction with validation"""
        try:
            if not self.web3.is_address(recipient) or amount_eth <= 0:
                raise ValueError("Invalid recipient or amount")
            
            wallet = self.get_wallet_info(user_id)
            if not wallet:
                return {"status": "error", "message": "Wallet not found"}
                
            private_key = self.cipher.decrypt(base64.b64decode(wallet["encrypted_private_key"])).decode()
            
            tx = {
                "to": self.web3.to_checksum_address(recipient),
                "value": self.web3.to_wei(amount_eth, "ether"),
                "gas": 21000,
                "gasPrice": self.web3.eth.gas_price,
                "nonce": self.web3.eth.get_transaction_count(wallet["address"]),
                "chainId": self.web3.eth.chain_id
            }
            
            signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO transactions (tx_hash, user_id, recipient, amount, timestamp, status) VALUES (?, ?, ?, ?, ?, ?)",
                    (tx_hash.hex(), user_id, recipient, amount_eth, int(time.time()), "pending")
                )
                conn.commit()
            
            self.logger.info(f"Transaction sent: {tx_hash.hex()} for user {user_id}")
            return {"status": "success", "tx_hash": tx_hash.hex()}
            
        except Exception as e:
            self.logger.error(f"Transaction error for {user_id}: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_wallet_info(self, user_id: str) -> Optional[Dict]:
        """Get wallet information with additional details"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT address, encrypted_private_key, created_at FROM wallets WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                if result:
                    address, encrypted_key_str, created_at = result
                    return {
                        "user_id": user_id,
                        "address": address,
                        "balance": self.get_balance(address),
                        "encrypted_private_key": encrypted_key_str,
                        "created_at": created_at.isoformat() if created_at else None
                    }
            return None
        except Exception as e:
            self.logger.error(f"Error getting wallet info for {user_id}: {e}")
            return None
    
    def get_transaction_history(self, user_id: str) -> List[Dict]:
        """Retrieve transaction history for a user"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT tx_hash, recipient, amount, timestamp, status FROM transactions WHERE user_id = ?", (user_id,))
                transactions = [{
                    "tx_hash": row[0],
                    "recipient": row[1],
                    "amount_eth": row[2],
                    "timestamp": datetime.fromtimestamp(row[3]).isoformat(),
                    "status": row[4]
                } for row in cursor.fetchall()]
                return transactions
        except Exception as e:
            self.logger.error(f"Error getting transaction history for {user_id}: {e}")
            return []

def main():
    print("Starting main()", flush=True)
    load_dotenv()
    rpc_url = os.getenv("RPC_URL")
    print(f"RPC_URL: {rpc_url}", flush=True)
    
    if not rpc_url:
        print("Error: RPC_URL not set in .env file", flush=True)
        return
    
    try:
        print("Initializing CryptoWallet", flush=True)
        wallet_system = CryptoWallet(rpc_url)
        
        # Generate a unique user_id for each run
        unique_user_id = f"user_{uuid.uuid4()}"
        print(f"Creating wallet for unique user: {unique_user_id}", flush=True)
        user_wallet = wallet_system.create_wallet(unique_user_id)
        if user_wallet:
            print(f"Created wallet: {json.dumps(user_wallet, indent=2)}", flush=True)
        else:
            print("Wallet creation failed", flush=True)
        
        print("Getting wallet info", flush=True)
        wallet_info = wallet_system.get_wallet_info(unique_user_id)
        if wallet_info:
            print(f"Wallet info: {json.dumps(wallet_info, indent=2)}", flush=True)
        else:
            print("No wallet info returned", flush=True)
            
        # Example transaction (uncomment to test)
        # if wallet_info:
        #     tx_result = wallet_system.send_transaction(unique_user_id, "0xRecipientAddress", 0.01)
        #     print(f"Transaction result: {json.dumps(tx_result, indent=2)}", flush=True)
        #     history = wallet_system.get_transaction_history(unique_user_id)
        #     print(f"Transaction history: {json.dumps(history, indent=2)}", flush=True)
            
    except Exception as e:
        print(f"Error in main: {e}", flush=True)

if __name__ == "__main__":
    main()