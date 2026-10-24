"""
Generate deterministic synthetic LOBSTER fixtures and hand-calculated feature truth tables.
All data is strictly synthetic authored data with known mathematical properties.
"""

import json
from pathlib import Path


def generate_fixtures():
    fixtures_dir = Path("data/fixtures")
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    # 1. Clean synthetic dataset (30 rows, structured intervals)
    # Timestamps in seconds after midnight (e.g. 34200 = 09:30:00)
    messages = []
    books = []

    # Initial levels before event 0
    # Level 1: Ask=10050, ASize=100, Bid=10000, BSize=100
    # Level 2: Ask=10060, ASize=200, Bid=9990, BSize=200
    # Level 3: Ask=10070, ASize=300, Bid=9980, BSize=300
    # Level 4: Ask=10080, ASize=400, Bid=9970, BSize=400
    # Level 5: Ask=10090, ASize=500, Bid=9960, BSize=500

    # Hand-crafted event sequence:
    # Row 0: Submission of 50 shares buy at 10000. Book: Bid1 size 100->150.
    # OFI: null (first row). Mid: 10025.
    messages.append("34200.100000000,1,101,50,10000,1")
    books.append("10050,100,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500")

    # Row 1: Submission of 50 shares sell at 10050. Book: Ask1 size 100->150.
    # OFI: Bid=0, Ask=-50 -> Total OFI = -50. TVI = 0. Mid = 10025.
    messages.append("34200.200000000,1,102,50,10050,-1")
    books.append("10050,150,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500")

    # Row 2: Submission of 80 shares buy at 10010 (new best bid!). Book: Bid1 price 10000->10010, size=80.
    # Bid price increased: OFI bid component = +80. Ask component = 0 -> Total OFI = +80. TVI = 0. Mid = 10030.
    messages.append("34200.300000000,1,103,80,10010,1")
    books.append("10050,150,10010,80,10060,200,10000,150,10070,300,9990,200,10080,400,9980,300,10090,500,9970,400")

    # Row 3: Visible execution of 80 shares at 10010. Resting order 103 filled.
    # Direction in LOBSTER = 1 (Buy resting) -> Aggressor = -1 (Sell initiated). Signed TVI = -80.
    # Bid price drops back to 10000, size=150. OFI bid component = -80. Total OFI = -80. Mid = 10025.
    messages.append("34200.400000000,4,103,80,10010,1")
    books.append("10050,150,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500")

    # Row 4: New second 34201.0 (Bucket 1). Submission of 60 shares sell at 10040 (new best ask!).
    # Ask price drops: OFI ask component = -60. Total OFI = -60. Mid = 10020. TVI = 0.
    messages.append("34201.100000000,1,104,60,10040,-1")
    books.append("10040,60,10000,150,10050,150,9990,200,10060,200,9980,300,10070,300,9970,400,10080,400,9960,500")

    # Row 5: Cancellation of 20 shares from order 104 at 10040. Ask1 size 60->40.
    # Ask size decreased at same price: OFI ask component = -(-20) = +20. Total OFI = +20. Mid = 10020.
    messages.append("34201.200000000,2,104,20,10040,-1")
    books.append("10040,40,10000,150,10050,150,9990,200,10060,200,9980,300,10070,300,9970,400,10080,400,9960,500")

    # Row 6: Execution of 40 shares at 10040 against resting sell order (Direction -1 -> Aggressor Buy +1).
    # Signed TVI = +40. Order 104 filled. Ask1 returns to 10050, size 150.
    # Ask price increased: OFI ask component = +40. Total OFI = +40. Mid = 10025.
    messages.append("34202.100000000,4,104,40,10040,-1")
    books.append("10050,150,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500")

    # Rows 7 to 20: Continued progression across timestamps 34203 to 34265 (to test 1s, 5s, 10s, 30s, 60s horizons)
    # Let's add deterministic events at 34205, 34210, 34220, 34230, 34260, 34265
    extended_events = [
        # (time, type, id, size, price, dir, ask1_p, ask1_s, bid1_p, bid1_s)
        (34205.0, 1, 105, 100, 10020, 1, 10050, 150, 10020, 100), # Bid jumps to 10020, Mid = 10035
        (34210.0, 1, 106, 50, 10040, -1, 10040, 50, 10020, 100),  # Ask drops to 10040, Mid = 10030
        (34215.0, 4, 106, 50, 10040, -1, 10050, 150, 10020, 100), # Exec buy, Mid = 10035
        (34220.0, 1, 107, 100, 10030, 1, 10050, 150, 10030, 100), # Bid jumps to 10030, Mid = 10040
        (34230.0, 1, 108, 100, 10040, 1, 10050, 150, 10040, 100), # Bid jumps to 10040, Mid = 10045
        (34260.0, 4, 108, 100, 10040, 1, 10050, 150, 10030, 100), # Exec sell, Bid drops to 10030, Mid = 10040
        (34261.0, 1, 109, 50, 10060, -1, 10050, 150, 10030, 100), # Add ask at level 2
        (34262.0, 2, 109, 50, 10060, -1, 10050, 150, 10030, 100), # Cancel
        (34265.0, 1, 110, 100, 10035, 1, 10050, 150, 10035, 100), # Bid jumps to 10035, Mid = 10042.5
    ]

    for t, et, oid, sz, pr, d, a1p, a1s, b1p, b1s in extended_events:
        messages.append(f"{t:.9f},{et},{oid},{sz},{pr},{d}")
        # Build 5 levels
        b_str = f"{a1p},{a1s},{b1p},{b1s},{a1p+10},200,{b1p-10},200,{a1p+20},300,{b1p-20},300,{a1p+30},400,{b1p-30},400,{a1p+40},500,{b1p-40},500"
        books.append(b_str)

    # Write clean synthetic files
    clean_msg_path = fixtures_dir / "synthetic_message.csv"
    clean_book_path = fixtures_dir / "synthetic_orderbook.csv"
    clean_msg_path.write_text("\n".join(messages) + "\n", encoding="utf-8")
    clean_book_path.write_text("\n".join(books) + "\n", encoding="utf-8")

    # Hand-calculated feature values for verification tests
    expected_hand_calculations = {
        "rows": [
            {"row": 0, "ofi_l1": None, "tvi": 0, "mid_price": 10025.0, "reason": "First row has no prior book"},
            {"row": 1, "ofi_l1": -50.0, "tvi": 0, "mid_price": 10025.0, "reason": "Ask size increased by 50 at same price"},
            {"row": 2, "ofi_l1": 80.0, "tvi": 0, "mid_price": 10030.0, "reason": "Bid price increased, new bid size 80 added"},
            {"row": 3, "ofi_l1": -80.0, "tvi": -80.0, "mid_price": 10025.0, "reason": "Bid price decreased, prev bid size 80 subtracted; Visible execution against resting buy -> seller initiated"},
            {"row": 4, "ofi_l1": -60.0, "tvi": 0, "mid_price": 10020.0, "reason": "Ask price decreased, new ask size 60 subtracted"},
            {"row": 5, "ofi_l1": 20.0, "tvi": 0, "mid_price": 10020.0, "reason": "Ask size decreased by 20 at same price -> +20"},
            {"row": 6, "ofi_l1": 40.0, "tvi": 40.0, "mid_price": 10025.0, "reason": "Ask price increased, prev ask size 40 added; Execution against resting sell -> buyer initiated"}
        ],
        "time_buckets_1s": {
            "34200": {"ofi_sum": -50.0, "tvi_sum": -80.0, "last_mid": 10025.0}, # events at 34200.1 (null), 34200.2 (-50), 34200.3 (+80), 34200.4 (-80) -> sum = -50
            "34201": {"ofi_sum": -40.0, "tvi_sum": 0.0, "last_mid": 10020.0},  # events at 34201.1 (-60), 34201.2 (+20) -> sum = -40
            "34202": {"ofi_sum": 40.0, "tvi_sum": 40.0, "last_mid": 10025.0}   # events at 34202.1 (+40) -> sum = 40
        }
    }
    (fixtures_dir / "expected_features.json").write_text(json.dumps(expected_hand_calculations, indent=2), encoding="utf-8")

    # 2. Malformed fixtures for strict edge-case validation tests

    # Malformed 1: message file with 5 columns instead of 6
    (fixtures_dir / "malformed_msg_5cols.csv").write_text(
        "34200.100000000,1,101,50,10000\n", encoding="utf-8"
    )

    # Malformed 2: unaligned row counts (message 3 rows, book 2 rows)
    (fixtures_dir / "malformed_unaligned_msg.csv").write_text(
        "34200.100000000,1,101,50,10000,1\n34200.200000000,1,102,50,10050,-1\n34200.300000000,1,103,80,10010,1\n",
        encoding="utf-8"
    )
    (fixtures_dir / "malformed_unaligned_book.csv").write_text(
        "10050,100,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n"
        "10050,150,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n",
        encoding="utf-8"
    )

    # Single-row message for 1-row book validation tests
    (fixtures_dir / "malformed_single_msg.csv").write_text(
        "34200.100000000,1,101,50,10000,1\n", encoding="utf-8"
    )

    # Malformed 3: crossed order book (bid price 10050 >= ask price 10040)
    (fixtures_dir / "malformed_crossed_book.csv").write_text(
        "10040,100,10050,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n",
        encoding="utf-8"
    )

    # Malformed 4: negative or zero book size at level 1
    (fixtures_dir / "malformed_zero_size_book.csv").write_text(
        "10050,0,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n",
        encoding="utf-8"
    )

    # Malformed 5: equal timestamps (two events at identical nanosecond, ensuring row order is preserved)
    (fixtures_dir / "equal_timestamp_msg.csv").write_text(
        "34200.100000000,1,101,50,10000,1\n"
        "34200.100000000,1,102,25,10000,1\n",
        encoding="utf-8"
    )
    (fixtures_dir / "equal_timestamp_book.csv").write_text(
        "10050,100,10000,150,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n"
        "10050,100,10000,175,10060,200,9990,200,10070,300,9980,300,10080,400,9970,400,10090,500,9960,500\n",
        encoding="utf-8"
    )

    print("Synthetic fixtures successfully generated in data/fixtures/")

if __name__ == "__main__":
    generate_fixtures()
