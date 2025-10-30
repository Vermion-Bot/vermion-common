import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta, timezone
import uuid
import threading

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, dbname, user, password, host="localhost", port="5432"):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        if self._initialized:
            return 
        
        self._initialized = True
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        
        self.pool = psycopg2.pool.SimpleConnectionPool(1, 20, **self.connection_params)
        self._create_tables_if_not_exists()
    
    def _get_connection(self):
        return self.pool.getconn()
    
    def _return_connection(self, conn):
        self.pool.putconn(conn)
    
    def _create_tables_if_not_exists(self):
        conn = self._get_connection()

        try:
            create_tables_query = """
            CREATE TABLE IF NOT EXISTS guild_messages (
                guild_id BIGINT PRIMARY KEY,
                test_message VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS config_tokens (
                token VARCHAR(255) PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                used_at TIMESTAMP DEFAULT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_token_guild 
            ON config_tokens(guild_id, used, expires_at);
            """

            with conn.cursor() as cursor:
                cursor.execute(create_tables_query)
                conn.commit()
        except Exception as e:
            print(f"❌ Hiba a táblák létrehozása során: {e}")
        finally:
            self._return_connection(conn)
    
    def generate_config_token(self, guild_id, expires_in_minutes=5):
        token = str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=expires_in_minutes)
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO config_tokens (token, guild_id, expires_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token) DO NOTHING
                """, (token, guild_id, expires_at))

                conn.commit()
            return token
        except Exception as e:
            print(f"❌ Hiba a token generálása során: {e}")
            return None
        finally:
            self._return_connection(conn)
    
    def check_token_valid(self, token, guild_id):
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT token FROM config_tokens 
                    WHERE token = %s 
                    AND guild_id = %s 
                    AND expires_at > CURRENT_TIMESTAMP 
                    AND used = FALSE
                """, (token, guild_id))

                result = cursor.fetchone()
                
                if result:
                    return True
                else:
                    return False
                
        except Exception as e:
            print(f"❌ Hiba a token ellenőrzése során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def validate_and_use_token(self, token, guild_id):
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE config_tokens 
                    SET used = TRUE, used_at = CURRENT_TIMESTAMP 
                    WHERE token = %s 
                    AND guild_id = %s 
                    AND expires_at > CURRENT_TIMESTAMP 
                    AND used = FALSE
                    RETURNING token
                """, (token, guild_id))

                result = cursor.fetchone()
                conn.commit()
                
                if result:
                    return True
                else:
                    return False
                
        except Exception as e:
            print(f"❌ Hiba a token validálása során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def get_test_message(self, guild_id):
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT test_message FROM guild_messages WHERE guild_id = %s",
                    (guild_id,)
                )

                result = cursor.fetchone()
                return result[0] if result else None
            
        except Exception as e:
            print(f"❌ Hiba a test_message lekérése során: {e}")
            return None
        finally:
            self._return_connection(conn)
    
    def insert_or_update_message(self, guild_id, test_message):
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO guild_messages (guild_id, test_message, updated_at) 
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (guild_id) 
                    DO UPDATE SET 
                        test_message = EXCLUDED.test_message,
                        updated_at = CURRENT_TIMESTAMP
                """, (guild_id, test_message))
                conn.commit()

            return True
        except Exception as e:
            print(f"❌ Hiba az adatok mentése során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def close(self):
        if hasattr(self, 'pool') and self.pool:
            self.pool.closeall()