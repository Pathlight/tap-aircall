"""REST client handling, including aircallStream base class."""

import time
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, Callable, Generator
from urllib.parse import urlparse, parse_qs

import requests
import backoff

from datetime import datetime
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class aircallStream(RESTStream):
    """aircall stream class."""

    url_base = "https://api.aircall.io/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.meta.next_page_link"  # Or override `get_next_page_token`.

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("api_id"),
            password=self.config.get("api_token"),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_next_page_token(
            self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        json_response = response.json()
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, json_response
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if not context:
            context = {}

        params: dict = {"per_page": 50}
        if next_page_token:
            # format next_page_token: "https://api.aircall.io/v1/calls?page=2&per_page=20"
            # page & per_page require int params
            # extract query from next_page_token string
            next_page_token_query: Dict = parse_qs(urlparse(next_page_token).query)
            params["page"] = int(next_page_token_query.get('page', ['1'])[0])  # Default is 1
            params["per_page"] = int(next_page_token_query.get('per_page', ['50'])[0])  # Default is is 20, but use 50
        if self.replication_key:
            params["order"] = "asc"

        starting_time = self.get_starting_timestamp(context)
        starting_unix_time = starting_time.timestamp()

        if starting_unix_time:
            params["from"] = starting_unix_time
        else:
            params["from"] = time.time()  # Unix timestamp

        params.update(context)  # If there are partitions, update it with the context
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.

        self.logger.info(f"meta: {list(extract_jsonpath('$.meta.[*]', input=response.json()))}")

        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # convert these values from type timestamp to datetime.
        datetime_types = ["answered_at", "started_at", "ended_at"]
        for key in datetime_types:
            if key in row and row.get(key):
                row[key] = datetime.fromtimestamp(row.get(key))
        return row

    """
    # This code-block is not required when backoff_wait_generator code-block is active
    def validate_response(self, response: requests.Response) -> None:
        try:
            super().validate_response(response)
        except RetriableAPIError as e:
             #Capture other Retriable Errors
            if e.response.status_code == 429:
                self.logger.error(f"(accepted intermittent error) Rate limit error: {e}")
                sys.exit(1001)
            else:
                raise e
    """

    #https://sdk.meltano.com/en/latest/code_samples.html#custom-backoff
    #FUJ-4120, introducing custom backoff for Aircall taps

    #https://developer.aircall.io/tutorials/logging-calls/#:~:text=Aircall%20Public%20API%20is%20rate,will%20be%20blocked%20by%20Aircall.
    #Aircall allows 60 requests per minute, generating a wait time of 90 seconds to avoid error caused by RateLimit exception
    def backoff_wait_generator(max_time: int) -> Callable[..., Generator[int, Any, None]]:
        return backoff.constant(interval=90)
