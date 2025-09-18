from propel_data_platform.connectors.base import BaseConnector


class MixpanelBaseConnector(BaseConnector):

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.api_url = (
            kwargs.get("config", {}).get("mixpanel-config", {}).get("api-url")
        )
        self.username = (
            kwargs.get("config", {}).get("mixpanel-config", {}).get("username")
        )
        self.password = (
            kwargs.get("config", {}).get("mixpanel-config", {}).get("password")
        )
        self.project_id = int(
            kwargs.get("config", {}).get("mixpanel-config", {}).get("project-id")
        )

        self.session.auth = (self.username, self.password)
        self.session.headers.update(
            {
                "accept": "text/plain",
                "Accept-Encoding": "gzip",
            }
        )

        self.throttler.configure(
            key=f"{self.client_name}-mixpanel-{self.ENDPOINT}",
            limit=self.RATE_LIMIT["limit"],
            window=self.RATE_LIMIT["window"],
        )
