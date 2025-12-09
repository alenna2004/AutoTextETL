#!/usr/bin/env python3
"""
Cryptographic Service - Secure encryption/decryption for sensitive data
Handles: script encryption, configuration encryption, database security
"""

import os
import base64
from typing import Union, Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
import secrets
import hashlib

class CryptoService:
    """
    Cryptographic service for secure data handling
    """
    
    def __init__(self, master_key: Optional[bytes] = None):
        """
        Initialize with master key or generate one
        Args:
            master_key: Pre-existing master key (if None, generates new one)
        """
        if master_key is None:
            self._master_key = self._generate_master_key()
        else:
            self._master_key = master_key
        
        # Create Fernet instance for encryption/decryption
        self._fernet = Fernet(self._master_key)
    
    @staticmethod
    def _generate_master_key() -> bytes:
        """
        Generate a new master encryption key
        Returns:
            bytes: Base64-encoded encryption key
        """
        return Fernet.generate_key()
    
    @staticmethod
    def derive_key_from_password(password: str, salt: Optional[bytes] = None) -> tuple:
        """
        Derive encryption key from password using PBKDF2
        Args:
            password: User password
            salt: Salt for key derivation (generates new if None)
        Returns:
            tuple: (derived_key, salt_used)
        """
        if salt is None:
            salt = os.urandom(16)  # 128-bit salt
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256-bit key
            salt=salt,
            iterations=100000,  # Recommended iteration count
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key, salt
    
    @staticmethod
    def derive_key_scrypt(password: str, salt: Optional[bytes] = None) -> tuple:
        """
        Derive encryption key from password using scrypt (more secure)
        Args:
            password: User password
            salt: Salt for key derivation (generates new if None)
        Returns:
            tuple: (derived_key, salt_used)
        """
        if salt is None:
            salt = os.urandom(16)
        
        kdf = Scrypt(
            salt=salt,
            length=32,
            n=2**14,  # 2^14 = 16384
            r=8,
            p=1,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key, salt
    
    def encrypt(self, data: Union[str, bytes]) -> str:
        """
        Encrypt data using Fernet symmetric encryption
        Args:
            data: String or bytes to encrypt
        Returns:
            str: Base64-encoded encrypted data
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        encrypted_data = self._fernet.encrypt(data)
        return base64.b64encode(encrypted_data).decode('utf-8')
    
    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt data using Fernet symmetric encryption
        Args:
            encrypted_data: Base64-encoded encrypted data
        Returns:
            str: Decrypted string
        """
        encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
        decrypted_bytes = self._fernet.decrypt(encrypted_bytes)
        return decrypted_bytes.decode('utf-8')
    
    def hash_password(self, password: str, salt: Optional[bytes] = None) -> tuple:
        """
        Hash password with salt using SHA-256
        Args:
            password: Password to hash
            salt: Salt for hashing (generates new if None)
        Returns:
            tuple: (hashed_password, salt_used)
        """
        if salt is None:
            salt = os.urandom(32)  # 256-bit salt
        
        pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
        return pwd_hash.hex(), salt
    
    def verify_password(self, password: str, hashed_password: str, salt: bytes) -> bool:
        """
        Verify password against hash
        Args:
            password: Password to verify
            hashed_password: Stored hash
            salt: Salt used for original hashing
        Returns:
            bool: True if password matches hash
        """
        pwd_hash, _ = self.hash_password(password, salt)
        return pwd_hash == hashed_password
    
    def encrypt_file(self, input_path: str, output_path: str):
        """
        Encrypt entire file
        Args:
            input_path: Path to input file
            output_path: Path to output encrypted file
        """
        with open(input_path, 'rb') as infile:
            plaintext = infile.read()
        
        encrypted_data = self._fernet.encrypt(plaintext)
        
        with open(output_path, 'wb') as outfile:
            outfile.write(encrypted_data)
    
    def decrypt_file(self, input_path: str, output_path: str):
        """
        Decrypt entire file
        Args:
            input_path: Path to encrypted file
            output_path: Path to output decrypted file
        """
        with open(input_path, 'rb') as infile:
            encrypted_data = infile.read()
        
        decrypted_data = self._fernet.decrypt(encrypted_data)
        
        with open(output_path, 'wb') as outfile:
            outfile.write(decrypted_data)
    
    def generate_secure_token(self, length: int = 32) -> str:
        """
        Generate cryptographically secure random token
        Args:
            length: Length of token in bytes (default 32 = 256 bits)
        Returns:
            str: Hex-encoded secure token
        """
        return secrets.token_hex(length)
    
    def generate_secure_password(self, length: int = 16) -> str:
        """
        Generate cryptographically secure password
        Args:
            length: Length of password
        Returns:
            str: Generated password
        """
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    def create_encrypted_backup(self, db_path: str, backup_path: str, password: str):
        """
        Create encrypted backup of database
        Args:
            db_path: Path to source database
            backup_path: Path to encrypted backup
            password: Password for encryption
        """
        # Read database file
        with open(db_path, 'rb') as f:
            db_content = f.read()
        
        # Derive key from password
        key, salt = self.derive_key_from_password(password)
        
        # Create new crypto service with derived key
        crypto_service = CryptoService(key)
        
        # Encrypt database content
        encrypted_content = crypto_service.encrypt(db_content)
        
        # Write encrypted backup with salt
        with open(backup_path, 'wb') as f:
            f.write(salt)  # Write salt first
            f.write(encrypted_content.encode('utf-8'))
    
    def restore_from_encrypted_backup(self, backup_path: str, db_path: str, password: str):
        """
        Restore database from encrypted backup
        Args:
            backup_path: Path to encrypted backup
            db_path: Path to restore database
            password: Password for decryption
        """
        with open(backup_path, 'rb') as f:
            # Read salt (first 16 bytes)
            salt = f.read(16)
            # Read encrypted content
            encrypted_content = f.read().decode('utf-8')
        
        # Derive key from password and salt
        key, _ = self.derive_key_from_password(password, salt)
        
        # Create crypto service with derived key
        crypto_service = CryptoService(key)
        
        # Decrypt content
        decrypted_content = crypto_service.decrypt(encrypted_content)
        
        # Write to database file
        with open(db_path, 'wb') as f:
            f.write(decrypted_content.encode('utf-8'))

# Global crypto service instance (singleton pattern)
_crypto_service_instance = None

def get_crypto_service() -> CryptoService:
    """
    Get global crypto service instance (singleton)
    """
    global _crypto_service_instance
    if _crypto_service_instance is None:
        # Try to load from environment or create new
        master_key_env = os.environ.get('MASTER_ENCRYPTION_KEY')
        if master_key_env:
            master_key = base64.urlsafe_b64decode(master_key_env.encode())
            _crypto_service_instance = CryptoService(master_key)
        else:
            # Generate new key and store in environment for reuse
            _crypto_service_instance = CryptoService()
            # Store in environment for reuse (in production, store securely elsewhere)
            os.environ['MASTER_ENCRYPTION_KEY'] = base64.urlsafe_b64encode(_crypto_service_instance._master_key).decode()
    
    return _crypto_service_instance

# For backward compatibility
__all__ = [
    'CryptoService',
    'get_crypto_service'
]