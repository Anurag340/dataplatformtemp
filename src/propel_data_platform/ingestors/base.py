from propel_data_platform.tasks.base import BaseTask


class BaseIngestor(BaseTask):

    @property
    def jsonl_path(self):
        path = f"{self.run_id}/raw_data/{self.ENDPOINT}/*.jsonl"
        return self.get_wasbs_path(path=path)
