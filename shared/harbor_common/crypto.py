import os
import base64
from cryptography.fernet import Fernet
from harbor_common.errors import HarborError

def get_fernet() -> Fernet:
    """Initialize Fernet from HARBOR_ENCRYPTION_KEY environment variable.
    
    Raises:
        HarborError: HARBOR_ENCRYPTION_KEY_REQUIRED (500) if missing
        HarborError: HARBOR_ENCRYPTION_KEY_INVALID (500) if invalid
    """
    key = os.environ.get("HARBOR_ENCRYPTION_KEY")
    if not key:
        raise HarborError(
            code="HARBOR_ENCRYPTION_KEY_REQUIRED",
            message="Application encryption key is missing (HARBOR_ENCRYPTION_KEY).",
            status_code=500,
        )
    
    try:
        # Validate key format
        return Fernet(key.encode())
    except Exception as e:
        raise HarborError(
            code="HARBOR_ENCRYPTION_KEY_INVALID",
            message=f"Application encryption key is invalid: {str(e)}",
            status_code=500,
        )

def encrypt_str(plaintext: str) -> str:
    """Encrypt a string using Fernet. Returns base64 encoded string."""
    if not plaintext:
        return ""
    f = get_fernet()
    return f.encrypt(plaintext.encode()).decode()

def decrypt_str(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted string.
    
    Raises:
        HarborError: TOKEN_DECRYPTION_FAILED (500)
    """
    if not ciphertext:
        return ""
    f = get_fernet()
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except Exception as e:
        raise HarborError(
            code="TOKEN_DECRYPTION_FAILED",
            message=f"Failed to decrypt token: {str(e)}",
            status_code=500,
        )

def is_fernet_token(value: str) -> bool:
    """Best-effort check if a string is a Fernet-encrypted token.
    Fernet tokens usually start with 'gAAAAA'.
    """
    if not value:
        return False
    # Fernet tokens are always urlsafe base64 and start with version 0x80 (base64 'g')
    return value.startswith("gAAAAA")
