import os
from web3 import Web3
from eth_account import Account
import json
from typing import Dict, Optional
import sqlite3
from cryptography.fernet import Fernet
import logging

class CryptoWallet:
    def __init__(self, rpc_url: str, db_path: str = "wallets.db"):
        """Initialize wallet system with Ethereum RPC and database"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Connect to Ethereum node
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.web3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum node")
            
        # Setup database
        self.db_path = db_path
        self._setup_database()
        
        # Setup encryption
        self.encryption_key = self._load_or_create_encryption_key()
        self.cipher = Fernet(self.encryption_key)
        
    def _setup_database(self):
        """Initialize SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wallets (
                    user_id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    encrypted_private_key TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_hash TEXT PRIMARY KEY,
                    user_id TEXT,
                    recipient TEXT,
                    amount REAL,
                    timestamp INTEGER,
                    status TEXT
                )
            """)
            conn.commit()
    
    def _load_or_create_encryption_key(self) -> bytes:
        """Load or create encryption key for private keys"""
        key_file = "encryption.key"
        if os.path.exists(key_file):
            with open(key_file, "rb") as f:
                return f.read()
        key = Fernet.generate_key()
        with open(key_file, "wb") as f:
            f.write(key)
        return key
    
    def create_wallet(self, user_id: str) -> Optional[Dict]:
        """Create a new wallet for a user"""
        try:
            # Check if user already has a wallet
            if self.get_wallet_info(user_id):
                return None
                
            # Generate new account
            account = Account.create()
            encrypted_key = self.cipher.encrypt(account.key)
            
            # Store in database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO wallets (user_id, address, encrypted_private_key) VALUES (?, ?, ?)",
                    (user_id, account.address, encrypted_key)
                )
                conn.commit()
                
            self.logger.info(f"Created wallet for user {user_id}")
            return {
                "address": account.address,
                "balance": self.get_balance(account.address)
            }
            
        except Exception as e:
            self.logger.error(f"Error creating wallet: {e}")
            return None
    
    def get_balance(self, address: str) -> float:
        """Get wallet balance in ETH"""
        try:
            balance_wei = self.web3.eth.get_balance(address)
            return float(self.web3.from_wei(balance_wei, 'ether'))
        except Exception as e:
            self.logger.error(f"Error getting balance: {e}")
            return 0.0
    
    def send_transaction(self, user_id: str, recipient: str, amount_eth: float) -> Dict:
        """Send ETH transaction"""
        try:
            # Get sender wallet
            wallet = self.get_wallet_info(user_id)
            if not wallet:
                return {"status": "error", "message": "Wallet not found"}
                
            # Decrypt private key
            private_key = self.cipher.decrypt(wallet["encrypted_private_key"]).decode()
            
            # Build transaction
            tx = {
                "to": self.web3.to_checksum_address(recipient),
                "value": self.web3.to_wei(amount_eth, "ether"),
                "gas": 21000,
                "gasPrice": self.web3.eth.gas_price,
                "nonce": self.web3.eth.get_transaction_count(wallet["address"]),
                "chainId": self.web3.eth.chain_id
            }
            
            # Sign and send
            signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Store transaction
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO transactions (tx_hash, user_id, recipient, amount, timestamp, status) VALUES (?, ?, ?, ?, ?, ?)",
                    (tx_hash.hex(), user_id, recipient, amount_eth, int(time.time()), "pending")
                )
                conn.commit()
                
            return {"status": "success", "tx_hash": tx_hash.hex()}
            
        except Exception as e:
            self.logger.error(f"Transaction error: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_wallet_info(self, user_id: str) -> Optional[Dict]:
        """Get wallet information"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT address, encrypted_private_key FROM wallets WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                
                if result:
                    address, encrypted_key = result
                    return {
                        "address": address,
                        "balance": self.get_balance(address),
                        "encrypted_private_key": encrypted_key
                    }
            return None
        except Exception as e:
            self.logger.error(f"Error getting wallet info: {e}")
            return None

# Example usage
def main():
    # Use a testnet for development (Sepolia in this case)
    rpc_url = "https://sepolia.infura.io/v3/YOUR_INFURA_PROJECT_ID"
    
    try:
        wallet_system = CryptoWallet(rpc_url)
        
        # Create a wallet
        user_wallet = wallet_system.create_wallet("user1")
        if user_wallet:
            print(f"Created wallet: {json.dumps(user_wallet, indent=2)}")
            
        # Check wallet info
        wallet_info = wallet_system.get_wallet_info("user1")
        if wallet_info:
            print(f"Wallet info: {json.dumps(wallet_info, indent=2)}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import time
    main()