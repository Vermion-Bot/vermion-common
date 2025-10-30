import aiohttp
from typing import Any, Optional, Dict
import logging

logger = logging.getLogger(__name__)

class ConfigManager:    
    def __init__(self, api_url: str = "http://localhost:8000/api/config"):
        self.api_url = api_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def init_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _request(self, guild_id: int, method: str = "GET", **kwargs) -> Optional[Dict]:
        await self.init_session()
        
        url = f"{self.api_url}/{guild_id}"
        
        try:
            async with self.session.request(method, url, **kwargs) as resp:
                logger.debug(f"📡 {method} {url} -> {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    logger.debug(f"✅ Config betöltve: {guild_id}")
                    return data
                elif resp.status == 404:
                    logger.warning(f"⚠️ Config nem található: {guild_id}")
                    return None
                else:
                    logger.error(f"❌ API error: {resp.status}")
                    return None
        
        except aiohttp.ClientError as e:
            return None
        except Exception as e:
            return None
    
    async def get_config(self, guild_id: int) -> Optional[Dict]:
        return await self._request(guild_id)
    
    async def get_value(self, guild_id: int, key: str, default: Any = None) -> Any:
        config = await self.get_config(guild_id)
        
        if config is None:
            return default
        
        keys = key.split(".")
        value = config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        
        return value if value is not None else default
    
    async def get_string(self, guild_id: int, key: str, default: str = "") -> str:
        value = await self.get_value(guild_id, key, default)
        return str(value) if value is not None else default
    
    async def get_int(self, guild_id: int, key: str, default: int = 0) -> int:
        value = await self.get_value(guild_id, key, default)
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    async def get_bool(self, guild_id: int, key: str, default: bool = False) -> bool:
        value = await self.get_value(guild_id, key, default)

        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in "true"
        
        return bool(value) if value is not None else default
    
    async def get_list(self, guild_id: int, key: str, default: list = None) -> list:
        if default is None:
            default = []
        value = await self.get_value(guild_id, key, default)
        return value if isinstance(value, list) else default


config = ConfigManager()