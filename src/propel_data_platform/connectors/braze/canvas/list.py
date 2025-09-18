import time

from propel_data_platform.connectors.braze.base import BrazeBaseConnector


class BrazeCanvasListConnector(BrazeBaseConnector):
    """Connector for fetching Braze Canvas data.
    https://www.braze.com/docs/api/endpoints/export/canvas/get_canvases/
    """

    ENDPOINT = "canvas/list"
    RATE_LIMIT = {"window": 3600, "limit": 250000}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.last_edit_time = kwargs.get("config", {}).get("last-edit-time")
        self.include_archived = kwargs.get("config", {}).get("include-archived")

    def fetch(
        self, page=None, include_archived=None, sort_direction=None, last_edit_time=None
    ):
        params = {}
        if page is not None:
            params["page"] = page
        if include_archived is not None:
            params["include_archived"] = include_archived
        if sort_direction is not None:
            params["sort_direction"] = sort_direction
        if last_edit_time is not None:
            params["last_edit.time[gt]"] = self.last_edit_time

        metadata = {
            "timestamp": time.time(),
            "params": params,
        }

        try:
            response = self.session.get(self.url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            response_json = response.json()
            if not response_json.get("message") == "success":
                raise Exception(f"Failed to fetch canvas list")
            if "metadata" in response_json:
                raise Exception("Key conflict: metadata key already exists in response")
            response_json["metadata"] = metadata
            return True, response_json
        except:
            return False, params

    def fetch_all(self):
        page, idx, results, retry_list = 0, 0, [], []

        while True:
            success, response = self.throttled_fetch(
                page=page,
                include_archived=self.include_archived,
                sort_direction="asc",
                last_edit_time=self.last_edit_time,
            )
            page += 1
            if not success:
                retry_list.append(response)
                continue
            page_canvases = response.get("canvases", [])
            if len(page_canvases) == 0:
                break

            for canvas in page_canvases:
                canvas["metadata"] = {
                    "timestamp": response["metadata"]["timestamp"],
                    "params": response["metadata"]["params"],
                    "run_id": self.run_id,
                }

            results.extend(page_canvases)

            while len(results) >= self.chunk_size:
                chunk, results = results[: self.chunk_size], results[self.chunk_size :]
                self.write_to_blob(chunk, idx, idx + self.chunk_size)
                idx += self.chunk_size

        for _ in range(self.retries):
            params, retry_list = retry_list, []
            for param in params:
                success, response = self.throttled_fetch(
                    **param,
                )
                if not success:
                    retry_list.append(response)
                    continue
                page_canvases = response.get("canvases", [])
                for canvas in page_canvases:
                    canvas["metadata"] = {
                        "timestamp": response["metadata"]["timestamp"],
                        "params": response["metadata"]["params"],
                        "run_id": self.run_id,
                    }
                results.extend(page_canvases)

                while len(results) >= self.chunk_size:
                    chunk, results = (
                        results[: self.chunk_size],
                        results[self.chunk_size :],
                    )
                    self.write_to_blob(chunk, idx, idx + self.chunk_size)
                    idx += self.chunk_size

        if len(results) > 0:
            self.write_to_blob(results, idx, idx + len(results))

        if len(retry_list) > 0:
            raise Exception("Failed to fetch data for some pages")
