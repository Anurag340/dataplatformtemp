from propel_data_platform.connectors.base import BaseConnector


class BrazeBaseConnector(BaseConnector):

    def __init__(
        self,
        **kwargs,
    ):

        super().__init__(**kwargs)

        self.api_key = kwargs.get("config", {}).get("braze-config", {}).get("api-key")
        self.api_url = kwargs.get("config", {}).get("braze-config", {}).get("api-url")

        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

        self.throttler.configure(
            key=f"{self.client_name}-braze-{self.ENDPOINT}",
            limit=self.RATE_LIMIT["limit"],
            window=self.RATE_LIMIT["window"],
        )
