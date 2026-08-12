"""Password hashing kept in one place so credentials never enter the database."""

from passlib.context import CryptContext

# pbkdf2_sha256 is provided by passlib itself and works in the minimal local
# install as well as production.  A native bcrypt backend can be selected by a
# deployment if its CryptContext policy is changed there.
password_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return password_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return password_context.verify(password, password_hash)
    except (ValueError, TypeError):
        return False
