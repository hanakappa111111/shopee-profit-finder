"""
Shopee listing manager module.

Handles Shopee API interactions for creating, updating, pausing, and managing
listings with proper authentication and error handling.
"""

import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

import requests

from src.config.settings import settings
from src.database.database import db
from src.database.models import ListingStatus, ShopeeListing
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error


class ListingManager:
    """
    Manages Shopee API interactions for listing operations.

    Handles authentication, API requests, and database synchronization
    for creating and updating product listings on Shopee.
    """

    def __init__(self) -> None:
        """Initialize API client with partner credentials and session."""
        self.session = requests.Session()
        self.partner_id = settings.SHOPEE_PARTNER_ID
        self.partner_key = settings.SHOPEE_PARTNER_KEY
        self.shop_id = settings.SHOPEE_SHOP_ID
        logger.info(
            f"ListingManager initialized for shop {self.shop_id} (partner {self.partner_id})"
        )

    def _sign(self, path: str, timestamp: int) -> str:
        """
        Generate HMAC-SHA256 signature for API request.

        Args:
            path: API endpoint path (e.g., '/api/v2/product/add_item').
            timestamp: Unix timestamp.

        Returns:
            Hex-encoded HMAC signature.
        """
        message = f"{path}{self.partner_id}{self.shop_id}{timestamp}"
        signature = hmac.new(
            self.partner_key.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def _build_params(self, path: str) -> Dict[str, Any]:
        """
        Build common query parameters with authentication.

        Args:
            path: API endpoint path.

        Returns:
            Dictionary of query parameters including timestamp and signature.
        """
        timestamp = int(time.time())
        signature = self._sign(path, timestamp)
        return {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": signature,
        }

    @retry_on_network_error()
    def _post(
        self, path: str, params: Dict[str, Any], payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute authenticated POST request to Shopee API.

        Args:
            path: API endpoint path.
            params: Query parameters.
            payload: Request body.

        Returns:
            JSON response from API.

        Raises:
            requests.RequestException: On network errors (retried automatically).
        """
        url = f"{settings.SHOPEE_API_BASE}{path}"
        logger.debug(f"POST {url} with payload: {json.dumps(payload, indent=2)}")

        response = self.session.post(
            url,
            params=params,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()

        result = response.json()
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        return result

    @retry_on_network_error()
    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute authenticated GET request to Shopee API.

        Args:
            path: API endpoint path.
            params: Query parameters.

        Returns:
            JSON response from API.

        Raises:
            requests.RequestException: On network errors (retried automatically).
        """
        url = f"{settings.SHOPEE_API_BASE}{path}"
        logger.debug(f"GET {url}")

        response = self.session.get(
            url,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        result = response.json()
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        return result

    def create_listing(self, listing: ShopeeListing) -> Dict[str, Any]:
        """
        Create a new listing on Shopee.

        Args:
            listing: ShopeeListing object with all required fields.

        Returns:
            API response with shopee_item_id.

        Raises:
            requests.RequestException: On API errors.
        """
        path = "/api/v2/product/add_item"
        params = self._build_params(path)

        payload = {
            "item": {
                "name": listing.title,
                "description": listing.description,
                "category_id": listing.category_id,
                "price": int(listing.price * 100),  # API expects cents
                "stock": listing.stock,
                "images": [{"url": img} for img in listing.images],
                "brand": listing.brand,
            }
        }

        result = self._post(path, params, payload)

        # Update DB with shopee_item_id and status
        if "item" in result and "item_id" in result["item"]:
            shopee_item_id = result["item"]["item_id"]
            db.update_listing(
                listing.id,
                shopee_item_id=shopee_item_id,
                status=ListingStatus.ACTIVE,
            )
            logger.info(f"Created listing {shopee_item_id} for '{listing.title}'")
        else:
            logger.error(f"Unexpected API response: {result}")

        return result

    def update_listing(
        self, listing_id: int, shopee_item_id: int, **fields
    ) -> Dict[str, Any]:
        """
        Update an existing listing on Shopee.

        Args:
            listing_id: Database listing ID.
            shopee_item_id: Shopee item ID.
            **fields: Fields to update (name, description, price, stock, images, etc.).

        Returns:
            API response.

        Raises:
            requests.RequestException: On API errors.
        """
        path = "/api/v2/product/update_item"
        params = self._build_params(path)

        payload = {
            "item": {
                "item_id": shopee_item_id,
            }
        }
        payload["item"].update(fields)

        result = self._post(path, params, payload)

        # Update DB with new values
        db.update_listing(listing_id, **fields)
        logger.info(f"Updated listing {shopee_item_id}: {list(fields.keys())}")

        return result

    def update_price(
        self, listing_id: int, shopee_item_id: int, new_price: float
    ) -> Dict[str, Any]:
        """
        Update the price of a listing.

        Args:
            listing_id: Database listing ID.
            shopee_item_id: Shopee item ID.
            new_price: New price in dollars.

        Returns:
            API response.

        Raises:
            requests.RequestException: On API errors.
        """
        logger.info(f"Updating price for item {shopee_item_id} to ${new_price:.2f}")
        return self.update_listing(
            listing_id,
            shopee_item_id,
            price=int(new_price * 100),
        )

    def update_stock(
        self, listing_id: int, shopee_item_id: int, new_stock: int
    ) -> Dict[str, Any]:
        """
        Update the stock quantity of a listing.

        Args:
            listing_id: Database listing ID.
            shopee_item_id: Shopee item ID.
            new_stock: New stock quantity.

        Returns:
            API response.

        Raises:
            requests.RequestException: On API errors.
        """
        logger.info(f"Updating stock for item {shopee_item_id} to {new_stock}")
        return self.update_listing(listing_id, shopee_item_id, stock=new_stock)

    def pause_listing(self, listing_id: int, shopee_item_id: int) -> Dict[str, Any]:
        """
        Pause a listing (disable it on Shopee).

        Args:
            listing_id: Database listing ID.
            shopee_item_id: Shopee item ID.

        Returns:
            API response.

        Raises:
            requests.RequestException: On API errors.
        """
        logger.info(f"Pausing listing {shopee_item_id}")
        result = self.update_listing(
            listing_id,
            shopee_item_id,
            status=ListingStatus.PAUSED.value,
        )
        db.update_listing(listing_id, status=ListingStatus.PAUSED)
        return result

    def unpause_listing(
        self, listing_id: int, shopee_item_id: int
    ) -> Dict[str, Any]:
        """
        Unpause a listing (enable it on Shopee).

        Args:
            listing_id: Database listing ID.
            shopee_item_id: Shopee item ID.

        Returns:
            API response.

        Raises:
            requests.RequestException: On API errors.
        """
        logger.info(f"Unpausing listing {shopee_item_id}")
        result = self.update_listing(
            listing_id,
            shopee_item_id,
            status=ListingStatus.NORMAL.value,
        )
        db.update_listing(listing_id, status=ListingStatus.ACTIVE)
        return result

    def get_listings_from_shopee(
        self, offset: int = 0, page_size: int = 50
    ) -> Dict[str, Any]:
        """
        Retrieve list of items from Shopee shop.

        Args:
            offset: Pagination offset.
            page_size: Number of items per page (max 50).

        Returns:
            API response with items list.

        Raises:
            requests.RequestException: On API errors.
        """
        path = "/api/v2/product/get_item_list"
        params = self._build_params(path)
        params.update({"offset": offset, "page_size": page_size})

        logger.info(f"Fetching listings from Shopee (offset={offset}, size={page_size})")
        return self._get(path, params)

    def dry_run_create(self, listing: ShopeeListing) -> Dict[str, Any]:
        """
        Simulate creating a listing without sending to API.

        Useful for testing and validation.

        Args:
            listing: ShopeeListing to simulate.

        Returns:
            Simulated payload that would be sent to API.
        """
        payload = {
            "item": {
                "name": listing.title,
                "description": listing.description,
                "category_id": listing.category_id,
                "price": int(listing.price * 100),
                "stock": listing.stock,
                "images": [{"url": img} for img in listing.images],
                "brand": listing.brand,
            }
        }
        logger.info(f"Dry run: would create listing '{listing.title}'")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        return payload
