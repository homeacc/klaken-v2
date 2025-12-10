from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BINANCE_BASE_URL: str = "https://api.binance.com"

    HYBLOCK_BASE_URL: str = "https://api.hyblockcapital.com"
    HYBLOCK_API_KEY: str = ""
    HYBLOCK_CLIENT_ID: str = ""
    HYBLOCK_CLIENT_SECRET: str = ""
    HYBLOCK_EXCHANGE: str = "binance_perp_stable"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
