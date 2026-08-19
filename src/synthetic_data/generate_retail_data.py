"""Generate and load a relational e-commerce source dataset."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

import numpy as np
import pandas as pd

from src.utils.database import (
    execute_sql_file,
    get_engine,
    load_dataframes_to_postgres,
)


LOGGER = logging.getLogger(__name__)
REFERENCE_DATE = pd.Timestamp("2026-01-01", tz="UTC")

DEFAULT_CUSTOMERS = 2_000
DEFAULT_PRODUCTS = 300
DEFAULT_ORDERS = 10_000
DEFAULT_SEED = 42

ORDER_STATUSES = ("completed", "cancelled", "refunded", "pending")
PAYMENT_STATUSES = ("paid", "failed", "refunded", "pending")
PAYMENT_METHODS = ("credit_card", "paypal", "bank_transfer", "gift_card")
SYNTHETIC_BEHAVIOR_SEGMENTS = (
    "high_value",
    "frequent",
    "occasional",
    "inactive",
    "new",
)

COUNTRY_CITIES = {
    "Spain": ("Madrid", "Barcelona", "Valencia", "Seville", "Bilbao", "Malaga"),
    "France": ("Paris", "Lyon", "Toulouse", "Bordeaux", "Lille", "Nantes"),
    "Germany": ("Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Leipzig"),
    "Italy": ("Rome", "Milan", "Turin", "Bologna", "Florence", "Naples"),
    "Portugal": ("Lisbon", "Porto", "Braga", "Coimbra", "Faro", "Aveiro"),
    "Netherlands": ("Amsterdam", "Rotterdam", "Utrecht", "Eindhoven", "Groningen"),
    "Belgium": ("Brussels", "Antwerp", "Ghent", "Bruges", "Leuven"),
    "Austria": ("Vienna", "Graz", "Linz", "Salzburg", "Innsbruck"),
    "Ireland": ("Dublin", "Cork", "Galway", "Limerick", "Waterford"),
}
FIRST_NAMES = (
    "Alex",
    "Sofia",
    "Lucas",
    "Emma",
    "Daniel",
    "Marta",
    "Hugo",
    "Laura",
    "Leo",
    "Clara",
    "David",
    "Elena",
    "Mateo",
    "Lucia",
    "Pablo",
    "Sara",
    "Adrian",
    "Paula",
    "Marco",
    "Julia",
    "Thomas",
    "Camille",
    "Louis",
    "Anna",
    "Felix",
    "Lena",
    "Bruno",
    "Ines",
    "Tiago",
    "Beatriz",
    "Victor",
    "Nora",
)
LAST_NAMES = (
    "Garcia",
    "Martin",
    "Lopez",
    "Bernard",
    "Schmidt",
    "Rossi",
    "Silva",
    "Costa",
    "Muller",
    "Moreau",
    "Dubois",
    "Laurent",
    "Petit",
    "Weber",
    "Fischer",
    "Romano",
    "Bianchi",
    "Santos",
    "Pereira",
    "Fernandez",
    "Ruiz",
    "Navarro",
    "Moreno",
    "Alonso",
    "Ribeiro",
    "Martins",
    "Lefevre",
    "Fontaine",
    "Schneider",
    "Wagner",
    "Bauer",
    "Conti",
)

# category: (category code, ((product code, base name, min price, max price), ...))
PRODUCT_CATALOG = {
    "Electronics": (
        "ELEC",
        (
            ("WH", "Wireless Headphones", 39.0, 249.0),
            ("SS", "Smart Speaker", 55.0, 179.0),
            ("UH", "USB-C Hub", 45.0, 109.0),
            ("KB", "Mechanical Keyboard", 75.0, 189.0),
            ("WM", "Wireless Mouse", 29.0, 119.0),
            ("PW", "Portable Charger", 35.0, 129.0),
            ("WC", "HD Webcam", 45.0, 159.0),
            ("ER", "E-Reader", 99.0, 249.0),
            ("BT", "Bluetooth Tracker", 19.0, 59.0),
            ("UM", "USB Microphone", 49.0, 199.0),
        ),
    ),
    "Home": (
        "HOME",
        (
            ("DL", "Desk Lamp", 25.0, 85.0),
            ("CM", "Coffee Maker", 49.0, 179.0),
            ("SB", "Storage Basket", 15.0, 49.0),
            ("BC", "Bedding Set", 55.0, 149.0),
            ("KT", "Kitchen Tools", 29.0, 89.0),
            ("AP", "Air Purifier", 89.0, 249.0),
            ("BL", "Countertop Blender", 45.0, 159.0),
            ("VC", "Vacuum Cleaner", 99.0, 299.0),
            ("WM", "Wall Mirror", 39.0, 129.0),
            ("TT", "Cotton Towel Set", 25.0, 79.0),
        ),
    ),
    "Sports": (
        "SPRT",
        (
            ("YM", "Yoga Mat", 22.0, 79.0),
            ("RB", "Running Bottle", 18.0, 59.0),
            ("RS", "Resistance Bands", 20.0, 69.0),
            ("FB", "Fitness Backpack", 45.0, 119.0),
            ("FR", "Foam Roller", 24.0, 69.0),
            ("DB", "Adjustable Dumbbells", 79.0, 299.0),
            ("JR", "Speed Jump Rope", 15.0, 49.0),
            ("GT", "Gym Towel", 12.0, 39.0),
            ("HB", "Hiking Backpack", 69.0, 189.0),
            ("PB", "Pilates Ball", 25.0, 79.0),
        ),
    ),
    "Books": (
        "BOOK",
        (
            ("DH", "Data Engineering Handbook", 32.0, 69.0),
            ("PG", "Python Programming Guide", 29.0, 64.0),
            ("DG", "Data Governance Playbook", 35.0, 75.0),
            ("TG", "European Travel Guide", 22.0, 55.0),
            ("RC", "Mediterranean Recipe Collection", 24.0, 59.0),
            ("SD", "SQL for Data Analysis", 30.0, 68.0),
            ("CS", "Cloud Systems Handbook", 34.0, 72.0),
            ("BM", "Business Metrics Guide", 28.0, 62.0),
            ("MA", "Modern Analytics Guide", 31.0, 70.0),
            ("DS", "Distributed Systems Primer", 36.0, 79.0),
        ),
    ),
    "Fashion": (
        "FASH",
        (
            ("TS", "Cotton T-Shirt", 18.0, 49.0),
            ("BP", "Canvas Backpack", 42.0, 109.0),
            ("SN", "Everyday Sneakers", 55.0, 149.0),
            ("JK", "Lightweight Jacket", 69.0, 179.0),
            ("SC", "Wool Scarf", 25.0, 69.0),
            ("DJ", "Denim Jeans", 49.0, 129.0),
            ("LS", "Linen Shirt", 39.0, 99.0),
            ("CH", "Casual Chinos", 45.0, 109.0),
            ("CP", "Classic Cap", 19.0, 49.0),
            ("SW", "Everyday Sweatshirt", 39.0, 99.0),
        ),
    ),
}

CATEGORY_VARIANTS = {
    "Electronics": ("Basic", "Plus", "Pro", "Mini", "Max", "Premium"),
    "Home": ("Basic", "Plus", "Modern", "Compact", "Premium", "Essential"),
    "Sports": ("Basic", "Plus", "Pro", "Mini", "Max", "Premium"),
    "Books": ("Essential", "Practical", "Advanced", "Pocket", "Illustrated"),
    "Fashion": ("Basic", "Plus", "Classic", "Urban", "Premium", "Essential"),
}

SEGMENT_ORDER_WEIGHTS = {
    "high_value": 0.4,
    "frequent": 4.5,
    "occasional": 0.8,
    "inactive": 0.3,
    "new": 1.2,
}


def _validate_count(value: int, name: str) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")


def _get_rng(rng: np.random.Generator | None) -> np.random.Generator:
    """Return the supplied generator or one based on the public default seed."""
    return rng if rng is not None else np.random.default_rng(DEFAULT_SEED)


def _empty_dataframe(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _timestamp_days_ago(
    rng: np.random.Generator,
    minimum_days: int,
    maximum_days: int,
) -> pd.Timestamp:
    days = int(rng.integers(minimum_days, maximum_days + 1))
    seconds = int(rng.integers(0, 86_400))
    return REFERENCE_DATE - pd.Timedelta(days=days, seconds=seconds)


def _timestamp_between(
    rng: np.random.Generator,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.Timestamp:
    available_seconds = max(1, int((end - start).total_seconds()))
    return start + pd.Timedelta(
        seconds=int(rng.integers(0, available_seconds))
    )


def generate_customers(
    n_customers: int,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate customers with unique IDs, emails and behavioural segments."""
    _validate_count(n_customers, "n_customers")
    generator = _get_rng(rng)
    countries = generator.choice(tuple(COUNTRY_CITIES), size=n_customers)
    first_names = generator.choice(FIRST_NAMES, size=n_customers)
    last_names = generator.choice(LAST_NAMES, size=n_customers)
    customer_ids = np.arange(1, n_customers + 1)
    segments = generator.choice(
        SYNTHETIC_BEHAVIOR_SEGMENTS,
        size=n_customers,
        p=(0.10, 0.20, 0.35, 0.20, 0.15),
    )

    created_at = []
    for segment in segments:
        if segment == "new":
            created_at.append(_timestamp_days_ago(generator, 1, 90))
        elif segment == "inactive":
            created_at.append(_timestamp_days_ago(generator, 540, 900))
        else:
            created_at.append(_timestamp_days_ago(generator, 90, 730))

    return pd.DataFrame(
        {
            "customer_id": customer_ids,
            "first_name": first_names,
            "last_name": last_names,
            "email": [
                f"{first}.{last}.{customer_id}@example.com".lower()
                for first, last, customer_id in zip(
                    first_names, last_names, customer_ids, strict=True
                )
            ],
            "country": countries,
            "city": [
                generator.choice(COUNTRY_CITIES[country])
                for country in countries
            ],
            "synthetic_behavior_segment": segments,
            "created_at": created_at,
        }
    )


def generate_products(
    n_products: int,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate products with technical IDs and unique commercial SKUs."""
    _validate_count(n_products, "n_products")
    generator = _get_rng(rng)
    categories = generator.choice(tuple(PRODUCT_CATALOG), size=n_products)
    product_ids = np.arange(1, n_products + 1)
    records = []

    for product_id, category in zip(product_ids, categories, strict=True):
        category_code, catalogue_entries = PRODUCT_CATALOG[category]
        product_code, base_name, minimum_price, maximum_price = (
            catalogue_entries[int(generator.integers(0, len(catalogue_entries)))]
        )
        variant = generator.choice(CATEGORY_VARIANTS[category])
        records.append(
            {
                "product_id": int(product_id),
                "sku": f"{category_code}-{product_code}-{product_id:04d}",
                "product_name": f"{base_name} {variant}",
                "category": category,
                "unit_price": round(
                    float(generator.uniform(minimum_price, maximum_price)), 2
                ),
                "created_at": _timestamp_days_ago(generator, 1, 540),
            }
        )

    columns = (
        "product_id",
        "sku",
        "product_name",
        "category",
        "unit_price",
        "created_at",
    )
    return pd.DataFrame.from_records(records, columns=columns)


def generate_inventory(
    products_df: pd.DataFrame,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate one non-negative inventory record per product ID."""
    generator = _get_rng(rng)
    return pd.DataFrame(
        {
            "product_id": products_df["product_id"].to_numpy(copy=True),
            "stock_quantity": generator.integers(0, 501, size=len(products_df)),
            "reorder_level": generator.integers(10, 51, size=len(products_df)),
            "updated_at": [REFERENCE_DATE] * len(products_df),
        }
    )


def _order_window(
    customer_created_at: pd.Timestamp,
    segment: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    if segment == "inactive":
        return (
            max(customer_created_at, REFERENCE_DATE - pd.Timedelta(days=540)),
            REFERENCE_DATE - pd.Timedelta(days=365),
        )
    if segment == "new":
        return (
            max(customer_created_at, REFERENCE_DATE - pd.Timedelta(days=60)),
            REFERENCE_DATE,
        )
    return (
        max(customer_created_at, REFERENCE_DATE - pd.Timedelta(days=365)),
        REFERENCE_DATE,
    )


def generate_orders(
    customers_df: pd.DataFrame,
    n_orders: int,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate orders whose frequency and recency depend on customer segment."""
    _validate_count(n_orders, "n_orders")
    columns = ("order_id", "customer_id", "order_date", "order_status", "country")
    if n_orders == 0:
        return _empty_dataframe(columns)
    if customers_df.empty:
        raise ValueError("customers_df cannot be empty when generating orders.")

    generator = _get_rng(rng)
    customer_weights = customers_df["synthetic_behavior_segment"].map(
        SEGMENT_ORDER_WEIGHTS
    )
    customer_probabilities = customer_weights / customer_weights.sum()
    customer_ids = generator.choice(
        customers_df["customer_id"].to_numpy(),
        size=n_orders,
        p=customer_probabilities.to_numpy(),
    )
    customers_by_id = customers_df.set_index("customer_id")
    order_dates = []

    for customer_id in customer_ids:
        customer = customers_by_id.loc[customer_id]
        start, end = _order_window(
            pd.Timestamp(customer["created_at"]),
            str(customer["synthetic_behavior_segment"]),
        )
        order_dates.append(_timestamp_between(generator, start, end))

    return pd.DataFrame(
        {
            "order_id": np.arange(1, n_orders + 1),
            "customer_id": customer_ids,
            "order_date": order_dates,
            "order_status": generator.choice(
                ORDER_STATUSES,
                size=n_orders,
                p=(0.72, 0.08, 0.05, 0.15),
            ),
            "country": [
                customers_by_id.loc[customer_id, "country"]
                for customer_id in customer_ids
            ],
        }
    )


def _product_pool_for_segment(
    products_df: pd.DataFrame,
    segment: str,
) -> pd.DataFrame:
    if segment == "high_value":
        minimum_price = products_df["unit_price"].quantile(0.60)
        pool = products_df[products_df["unit_price"] >= minimum_price]
    elif segment == "frequent":
        lower_price = products_df["unit_price"].quantile(0.20)
        upper_price = products_df["unit_price"].quantile(0.80)
        pool = products_df[
            products_df["unit_price"].between(lower_price, upper_price)
        ]
    else:
        pool = products_df

    return pool if not pool.empty else products_df


def _line_profile(segment: str) -> tuple[int, int]:
    """Return maximum lines and exclusive maximum quantity for a segment."""
    if segment == "high_value":
        return 3, 6
    if segment == "frequent":
        return 3, 4
    if segment == "new":
        return 3, 3
    return 2, 3


def generate_order_items(
    orders_df: pd.DataFrame,
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame | None = None,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate valid product lines with segment-dependent ticket patterns."""
    columns = (
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
    )
    if orders_df.empty:
        return _empty_dataframe(columns)
    if products_df.empty:
        raise ValueError("products_df cannot be empty when generating order items.")

    generator = _get_rng(rng)
    behavior_segments = (
        customers_df.set_index("customer_id")[
            "synthetic_behavior_segment"
        ].to_dict()
        if customers_df is not None
        else {}
    )
    records: list[dict[str, int | float]] = []
    next_order_item_id = 1

    for order in orders_df.itertuples(index=False):
        segment = str(behavior_segments.get(order.customer_id, "occasional"))
        product_pool = _product_pool_for_segment(products_df, segment)
        maximum_lines, maximum_quantity = _line_profile(segment)
        line_count = int(
            generator.integers(1, min(maximum_lines, len(product_pool)) + 1)
        )
        product_indexes = generator.choice(
            len(product_pool), size=line_count, replace=False
        )

        for product_index in product_indexes:
            product = product_pool.iloc[int(product_index)]
            minimum_quantity = 2 if segment == "high_value" else 1
            quantity = int(
                generator.integers(minimum_quantity, maximum_quantity)
            )
            unit_price = float(product["unit_price"])
            records.append(
                {
                    "order_item_id": next_order_item_id,
                    "order_id": int(order.order_id),
                    "product_id": int(product["product_id"]),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": round(quantity * unit_price, 2),
                }
            )
            next_order_item_id += 1

    return pd.DataFrame.from_records(records, columns=columns)


def generate_payments(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Generate one payment whose amount equals each order total."""
    columns = (
        "payment_id",
        "order_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "payment_date",
    )
    if orders_df.empty:
        return _empty_dataframe(columns)

    totals = order_items_df.groupby("order_id")["line_total"].sum().round(2)
    missing_totals = set(orders_df["order_id"]) - set(totals.index)
    if missing_totals:
        raise ValueError("Every order must have at least one order item.")

    generator = _get_rng(rng)
    payment_status_by_order = {
        "completed": "paid",
        "cancelled": "failed",
        "refunded": "refunded",
        "pending": "pending",
    }
    records = []

    for payment_id, order in enumerate(orders_df.itertuples(index=False), start=1):
        records.append(
            {
                "payment_id": payment_id,
                "order_id": int(order.order_id),
                "payment_method": generator.choice(PAYMENT_METHODS),
                "payment_status": payment_status_by_order[order.order_status],
                "payment_amount": float(totals.loc[order.order_id]),
                "payment_date": pd.Timestamp(order.order_date)
                + pd.Timedelta(hours=int(generator.integers(0, 49))),
            }
        )

    return pd.DataFrame.from_records(records, columns=columns)


def generate_all_data(
    n_customers: int = DEFAULT_CUSTOMERS,
    n_products: int = DEFAULT_PRODUCTS,
    n_orders: int = DEFAULT_ORDERS,
    seed: int = DEFAULT_SEED,
) -> dict[str, pd.DataFrame]:
    """Generate the source dataset using child RNGs derived from one seed."""
    _validate_count(n_customers, "n_customers")
    _validate_count(n_products, "n_products")
    _validate_count(n_orders, "n_orders")
    if n_orders > 0 and (n_customers == 0 or n_products == 0):
        raise ValueError("Orders require at least one customer and one product.")

    child_sequences = np.random.SeedSequence(seed).spawn(6)
    generators = [np.random.default_rng(sequence) for sequence in child_sequences]

    customers = generate_customers(n_customers, generators[0])
    products = generate_products(n_products, generators[1])
    inventory = generate_inventory(products, generators[2])
    orders = generate_orders(customers, n_orders, generators[3])
    order_items = generate_order_items(
        orders,
        products,
        customers,
        generators[4],
    )
    payments = generate_payments(orders, order_items, generators[5])

    return {
        "customers": customers,
        "products": products,
        "inventory": inventory,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
    }


def load_synthetic_data_to_postgres(
    n_customers: int = DEFAULT_CUSTOMERS,
    n_products: int = DEFAULT_PRODUCTS,
    n_orders: int = DEFAULT_ORDERS,
    seed: int = DEFAULT_SEED,
) -> dict[str, int]:
    """Create the schema, generate parameterized data and load PostgreSQL."""
    dataframes = generate_all_data(n_customers, n_products, n_orders, seed)
    engine = get_engine()
    execute_sql_file(engine=engine)
    return load_dataframes_to_postgres(dataframes, engine=engine)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line dataset sizes and the master random seed."""
    parser = argparse.ArgumentParser(
        description="Generate and load RetailPulse synthetic source data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=DEFAULT_CUSTOMERS,
        help="number of customers",
    )
    parser.add_argument(
        "--products",
        type=int,
        default=DEFAULT_PRODUCTS,
        help="number of products",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=DEFAULT_ORDERS,
        help="number of orders",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="master random seed",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for synthetic source-data generation and loading."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    loaded_counts = load_synthetic_data_to_postgres(
        n_customers=args.customers,
        n_products=args.products,
        n_orders=args.orders,
        seed=args.seed,
    )
    LOGGER.info("Synthetic data load completed with seed %s.", args.seed)
    for table, count in loaded_counts.items():
        LOGGER.info("%-12s %s rows", table, count)


if __name__ == "__main__":
    main()
