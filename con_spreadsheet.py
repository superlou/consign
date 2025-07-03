from typing import Any
from googleapiclient.discovery import Resource
import pandas as pd


class ConSpreadsheet:
    def __init__(self, sheets_svc: Resource, file_id: str):
        self.sheets_svc = sheets_svc
        self.file_id = file_id
        self.sheet_ids = self.load_sheet_ids()
        self.config = self.load_config()

    def load_sheet_ids(self) -> dict[str, int]:
        result = (
            self.sheets_svc.spreadsheets()
            .get(spreadsheetId=self.file_id, fields="sheets(properties)")
            .execute()
        )

        sheets = {
            sheet["properties"]["title"]: sheet["properties"]["sheetId"]
            for sheet in result["sheets"]
        }

        return sheets

    def get_sheet_as_df(self, sheet_name: str) -> pd.DataFrame:
        result = (
            self.sheets_svc.spreadsheets()
            .values()
            .get(spreadsheetId=self.file_id, range="config")
            .execute()
        )

        columns = result["values"][0]
        data = result["values"][1:]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def load_config(self) -> dict[str, Any]:
        df = self.get_sheet_as_df("config")

        return {row.parameter: row.value for i, row in df.iterrows()}

    def get_config(self, param: str) -> Any:
        return self.config[param]

    def update_sessions(self):
        sessions_sheet_id = self.sheet_ids["sessions"]
        result = (
            self.sheets_svc.spreadsheets()
            .get(spreadsheetId=self.file_id, fields="sheets(tables)")
            .execute()
        )

        print(result)
