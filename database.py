import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta, timezone
import uuid
import threading
import secrets

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
            
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR(64) PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username VARCHAR(255) NOT NULL,
                discriminator VARCHAR(10),
                avatar VARCHAR(255),
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                token_type VARCHAR(50),
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS user_guilds (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                guild_name VARCHAR(255),
                guild_icon VARCHAR(255),
                owner BOOLEAN DEFAULT FALSE,
                permissions BIGINT,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, guild_id)
            );
            
            CREATE TABLE IF NOT EXISTS bot_guilds (
                guild_id BIGINT PRIMARY KEY,
                guild_name VARCHAR(255),
                member_count INTEGER DEFAULT 0,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                action VARCHAR(100) NOT NULL,
                details TEXT,
                ip_address VARCHAR(45),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_sessions_user 
            ON user_sessions(user_id, expires_at);
            
            CREATE INDEX IF NOT EXISTS idx_guilds_user 
            ON user_guilds(user_id, owner);
            
            CREATE INDEX IF NOT EXISTS idx_audit_guild 
            ON audit_logs(guild_id, created_at);
            """

            with conn.cursor() as cursor:
                cursor.execute(create_tables_query)
                conn.commit()
        except Exception as e:
            print(f"❌ Hiba a táblák létrehozása során: {e}")
        finally:
            self._return_connection(conn)
    
    def create_session(self, user_data, token_data):
        session_id = secrets.token_urlsafe(48)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=token_data.get('expires_in', 604800))
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO user_sessions 
                    (session_id, user_id, username, discriminator, avatar, 
                     access_token, refresh_token, token_type, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        access_token = EXCLUDED.access_token,
                        expires_at = EXCLUDED.expires_at,
                        last_activity = CURRENT_TIMESTAMP
                    RETURNING session_id
                """, (
                    session_id,
                    int(user_data['id']),
                    user_data['username'],
                    user_data.get('discriminator', '0'),
                    user_data.get('avatar'),
                    token_data['access_token'],
                    token_data.get('refresh_token'),
                    token_data.get('token_type', 'Bearer'),
                    expires_at
                ))
                conn.commit()
                return session_id
        except Exception as e:
            print(f"❌ Hiba a session létrehozása során: {e}")
            return None
        finally:
            self._return_connection(conn)
    
    def get_session(self, session_id):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, username, discriminator, avatar, access_token, 
                           token_type, expires_at
                    FROM user_sessions 
                    WHERE session_id = %s 
                    AND expires_at > CURRENT_TIMESTAMP
                """, (session_id,))
                
                result = cursor.fetchone()
                if result:
                    cursor.execute("""
                        UPDATE user_sessions 
                        SET last_activity = CURRENT_TIMESTAMP 
                        WHERE session_id = %s
                    """, (session_id,))
                    conn.commit()
                    
                    return {
                        'user_id': result[0],
                        'username': result[1],
                        'discriminator': result[2],
                        'avatar': result[3],
                        'access_token': result[4],
                        'token_type': result[5],
                        'expires_at': result[6]
                    }
                return None
        except Exception as e:
            print(f"❌ Hiba a session lekérése során: {e}")
            return None
        finally:
            self._return_connection(conn)
    
    def delete_session(self, session_id):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM user_sessions WHERE session_id = %s", (session_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba a session törlése során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def sync_user_guilds(self, user_id, guilds_data):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM user_guilds WHERE user_id = %s", (user_id,))
                
                for guild in guilds_data:
                    cursor.execute("""
                        INSERT INTO user_guilds 
                        (user_id, guild_id, guild_name, guild_icon, owner, permissions)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        user_id,
                        int(guild['id']),
                        guild['name'],
                        guild.get('icon'),
                        guild.get('owner', False),
                        int(guild.get('permissions', 0))
                    ))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba a guilds szinkronizálása során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def sync_bot_guilds(self, guilds_data):
        """Frissíti a bot_guilds táblát a bot által látott szerverekkel"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM bot_guilds")
                
                for guild in guilds_data:
                    cursor.execute("""
                        INSERT INTO bot_guilds (guild_id, guild_name, member_count)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (guild_id) DO UPDATE SET
                            guild_name = EXCLUDED.guild_name,
                            member_count = EXCLUDED.member_count,
                            synced_at = CURRENT_TIMESTAMP
                    """, (
                        guild['id'],
                        guild['name'],
                        guild.get('member_count', 0)
                    ))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba a bot guilds szinkronizálása során: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self._return_connection(conn)
    
    def add_bot_guild(self, guild_id, guild_name, member_count=0):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO bot_guilds (guild_id, guild_name, member_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (guild_id) DO UPDATE SET
                        guild_name = EXCLUDED.guild_name,
                        member_count = EXCLUDED.member_count,
                        synced_at = CURRENT_TIMESTAMP
                """, (guild_id, guild_name, member_count))
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba a bot guild hozzáadása során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def remove_bot_guild(self, guild_id):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM bot_guilds WHERE guild_id = %s", (guild_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba a bot guild törlése során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def is_bot_in_guild(self, guild_id):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS(SELECT 1 FROM bot_guilds WHERE guild_id = %s)
                """, (guild_id,))
                result = cursor.fetchone()
                return result[0] if result else False
        except Exception as e:
            print(f"❌ Hiba a bot guild ellenőrzés során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def get_user_guilds(self, user_id, manageable_only=True):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                if manageable_only:
                    # ADMINISTRATOR perm check (0x8 = 8) ÉS bot jelenlét
                    cursor.execute("""
                        SELECT ug.guild_id, ug.guild_name, ug.guild_icon, ug.owner, ug.permissions,
                               CASE WHEN bg.guild_id IS NOT NULL THEN TRUE ELSE FALSE END as bot_present
                        FROM user_guilds ug
                        LEFT JOIN bot_guilds bg ON ug.guild_id = bg.guild_id
                        WHERE ug.user_id = %s 
                        AND (ug.owner = TRUE OR (ug.permissions & 8) = 8)
                        AND bg.guild_id IS NOT NULL
                        ORDER BY ug.guild_name
                    """, (user_id,))
                else:
                    cursor.execute("""
                        SELECT ug.guild_id, ug.guild_name, ug.guild_icon, ug.owner, ug.permissions,
                               CASE WHEN bg.guild_id IS NOT NULL THEN TRUE ELSE FALSE END as bot_present
                        FROM user_guilds ug
                        LEFT JOIN bot_guilds bg ON ug.guild_id = bg.guild_id
                        WHERE ug.user_id = %s
                        ORDER BY ug.guild_name
                    """, (user_id,))
                
                results = cursor.fetchall()
                return [{
                    'guild_id': r[0],
                    'guild_name': r[1],
                    'guild_icon': r[2],
                    'owner': r[3],
                    'permissions': r[4],
                    'bot_present': r[5]
                } for r in results]
        except Exception as e:
            print(f"❌ Hiba a guilds lekérése során: {e}")
            return []
        finally:
            self._return_connection(conn)
    
    def check_user_guild_permission(self, user_id, guild_id):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT owner, permissions
                    FROM user_guilds 
                    WHERE user_id = %s AND guild_id = %s
                """, (user_id, guild_id))
                
                result = cursor.fetchone()
                if result:
                    owner, permissions = result
                    # ADMINISTRATOR perm check (0x8 = 8)
                    return owner or (permissions & 8) == 8
                return False
        except Exception as e:
            print(f"❌ Hiba a permission ellenőrzés során: {e}")
            return False
        finally:
            self._return_connection(conn)
    
    def log_action(self, user_id, guild_id, action, details=None, ip_address=None):
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO audit_logs (user_id, guild_id, action, details, ip_address)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, guild_id, action, details, ip_address))
                conn.commit()
                return True
        except Exception as e:
            print(f"❌ Hiba az audit log írása során: {e}")
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