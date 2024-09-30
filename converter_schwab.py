import pandas as pd
import json
import argparse

from pyfifotax.utils import create_report_sheet
from pyfifotax.data_structures_dataframe import (
    BuyOrderRow,
    SellOrderRow,
    RSURow,
    ESPPRow,
    DividendRow,
    TaxWithholdingRow,
    CurrencyConversionRow,
    MoneyTransferRow,
    TaxReversalRow,
    StockSplitRow
)

from datetime import datetime


parser = argparse.ArgumentParser(
    description="Convert Schwab JSON output to XLSX for later processing. Please review converted transactions before creating Tax/AWV reports!"
)
parser.add_argument(
    "-i",
    "--json",
    dest="json_filename",
    type=str,
    required=True,
    help="Schwab JSON History",
)
parser.add_argument(
    "-o",
    "--xlsx",
    dest="xlsx_filename",
    type=str,
    required=True,
    help="Output XLSX file",
)
parser.add_argument(
    "--forex_transfer_as_exchange",
    action="store_true",
    help=(
        "If set, treats outgoing wire transfers as currency exchange to EUR."
        " This can be helpful to simplify the reporting of currency conversions"
        " if this is the only style of transfer. Please check the actual date"
        " of conversion and for correctness in general!"
    ),
)
parser.add_argument(
    "--stock_splits_fp",
    dest="stock_splits_fp",
    type=str,
    required=True,
    help="Filepath to the stock_splits.xlsx file",
)


def process_schwab_json(json_file_name, xlsx_file_name, forex_transfer_as_exchange, stock_splits_fp: str):
    schwab_rsu_events = []
    schwab_rsu_deposit_events: dict[tuple[int, int, str], RSURow] = {}
    schwab_rsu_lapse_events = {}
    schwab_espp_events = []
    schwab_espp_deposit_events: dict[datetime.date, ESPPRow] = {}
    schwab_dividend_events = []
    schwab_buy_events = [BuyOrderRow.empty_dict()]
    schwab_sell_events = []
    schwab_wire_events = []
    schwab_money_transfer_events = []

    with open(json_file_name) as f:
        d = json.load(f)
        for e in d["Transactions"]:
            if e["Action"] == "Deposit" and e["Description"] == "ESPP":
                espp = ESPPRow.from_schwab_json(e)
                if espp.date in schwab_espp_deposit_events:
                    raise RuntimeError(f"Found duplicated ESPP Deposit event: {espp}")
                schwab_espp_deposit_events[espp.date] = espp

            # assumption behind RSU: each grant has its own vest/deposit event
            # assumption behind RSU: award-id, year, and month are unique to each
            # deposit/lapse event (day of deposit and lapse might differ)
            elif (
                e["Action"] == "Lapse" and e["Description"] == "Restricted Stock Lapse"
            ):
                tmp, award_id = RSURow.from_schwab_lapse_json(e)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_lapse_events:
                    raise RuntimeError("Found duplicated RSU Lapse event: {tmp}")
                schwab_rsu_lapse_events[key] = tmp

            elif e["Action"] == "Deposit" and e["Description"] == "RS":
                tmp, award_id = RSURow.from_schwab_deposit_json(e)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_deposit_events:
                    raise RuntimeError(f"Found duplicated RSU deposit event: {tmp}")
                schwab_rsu_deposit_events[key] = tmp

            elif e["Action"] == "Dividend" and e["Description"] == "Credit":
                tmp = DividendRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp)

            elif e["Action"] == "Sale" and e["Description"] == "Share Sale":
                schwab_sell_events.append(SellOrderRow.from_schwab_json(e).to_dict())

                # update all the RSU deposits with sold count
                for td in e["TransactionDetails"]:
                    details = td["Details"]
                    if details["Type"] == "RS":
                        date = datetime.strptime(details["VestDate"], "%m/%d/%Y").date()
                        key = (date.year, date.month, details["GrantId"])
                        if key not in schwab_rsu_deposit_events:
                            raise RuntimeError(f"Ran into a Sale event with sold RSUs {key} that do not exist")
                        schwab_rsu_deposit_events[key].sold += int(details["Shares"])
                    elif details["Type"] == "ESPP":
                        date = datetime.strptime(details["PurchaseDate"], "%m/%d/%Y").date()
                        if date not in schwab_espp_deposit_events:
                            raise RuntimeError(f"Ran into a Sale event with sold ESPPs {key} that do not exist")
                        schwab_espp_deposit_events[date].sold += int(details["Shares"])
            elif (
                e["Action"] == "Wire Transfer"
                and e["Description"] == "Cash Disbursement"
            ):
                if forex_transfer_as_exchange:
                    schwab_wire_events.append(
                        CurrencyConversionRow.from_schwab_json(e).to_dict()
                    )

                else:
                    schwab_money_transfer_events.append(
                        MoneyTransferRow.from_schwab_json(e).to_dict()
                    )

            elif e["Action"] == "Tax Withholding" and e["Description"] == "Debit":
                tmp = TaxWithholdingRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp.to_dividend_row())

            elif e["Action"] == "Tax Reversal" and e["Description"] == "Credit":
                tmp = TaxReversalRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp.to_dividend_row())

            else:
                # do nothing on unused fields
                pass

    if len(schwab_rsu_lapse_events) != len(schwab_rsu_deposit_events):
        raise RuntimeError(
            f"Number of RSU Lapses {len(schwab_rsu_lapse_events)} does not match number of RSU deposits {len(schwab_rsu_deposit_events)}"
        )

    with pd.ExcelFile(stock_splits_fp) as xls:
        dtypes = StockSplitRow.type_dict()
        dtypes["date"] = None
        df_stock_splits = pd.read_excel(xls, parse_dates=["date"], dtype=dtypes)

    if len(schwab_rsu_lapse_events) > 0:
        for key, rsu in schwab_rsu_deposit_events.items():
            if key not in schwab_rsu_lapse_events:
                raise ValueError(
                    f"RSU Deposit {key} does not have a matching Lapse Event"
                )

            rsu_lapse = schwab_rsu_lapse_events[key]
            # schwab applies splits on historical lapse data but not on deposits
            # thus, use this difference to determine split factor on-the-fly
            # based on the split factor, we then can rely on the gross quantity
            # in the lapse event while the prices for the deposit event are already
            # correct
            split_factor = 1
            for _, row in df_stock_splits.iterrows():
                if rsu.date >= row["date"].date():
                    break
                split_factor *= float(row["shares_after_split"])

            if rsu.sold < rsu.net_quantity:
                print(f"Not all shares were sold for {key}. Undoing adjustments. Split factor is {split_factor}")
                rsu.fair_market_value *= split_factor
                rsu.net_quantity /= split_factor

            rsu.gross_quantity = rsu_lapse.gross_quantity / split_factor
            schwab_rsu_events.append(rsu)

    # adjust ESPP events based on split
    for key, espp in schwab_espp_deposit_events.items():
        split_factor = 1
        for _, row in df_stock_splits.iterrows():
            if espp.date >= row["date"].date():
                break
            split_factor *= float(row["shares_after_split"])

        if espp.sold < espp.quantity:
            print(f"Not all shares were sold for {key}. Undoing adjustments. Split factor is {split_factor}")
            espp.fair_market_value *= split_factor
            espp.buy_price *= split_factor
            espp.quantity /= split_factor

        schwab_espp_events.append(espp)

    if len(schwab_espp_events) == 0:
        schwab_espp_events.append(ESPPRow.empty_dict())
    if len(schwab_dividend_events) == 0:
        schwab_dividend_events.append(DividendRow.empty_dict())
    if len(schwab_sell_events) == 0:
        schwab_sell_events.append(SellOrderRow.empty_dict())
    if len(schwab_wire_events) == 0:
        schwab_wire_events.append(CurrencyConversionRow.empty_dict())
    if len(schwab_money_transfer_events) == 0:
        schwab_money_transfer_events.append(MoneyTransferRow.empty_dict())

    dfs = {
        "rsu": pd.DataFrame(schwab_rsu_events),
        "espp": pd.DataFrame(schwab_espp_events),
        "dividends": pd.DataFrame(schwab_dividend_events),
        "buy_orders": pd.DataFrame(schwab_buy_events),
        "sell_orders": pd.DataFrame(schwab_sell_events),
        "currency_conversions": pd.DataFrame(schwab_wire_events),
        "money_transfers": pd.DataFrame(schwab_money_transfer_events),
    }

    with pd.ExcelWriter(
        xlsx_file_name, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
    ) as writer:
        for k, v in dfs.items():
            v.sort_values("date", inplace=True)
            create_report_sheet(k, v, writer)
            # overwrite column width somewhat inline with manual examples
            writer.sheets[k].set_column(1, 20, 16)


def main(args):
    process_schwab_json(
        args.json_filename, args.xlsx_filename, args.forex_transfer_as_exchange, args.stock_splits_fp
    )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
