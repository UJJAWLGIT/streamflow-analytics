"""
generator.py -- StreamFlow Synthetic Data Generator
Generates realistic cancel-flow data for all pipeline inputs.
Usage: python data/synthetic/generator.py --start-date 2024-01-01 --end-date 2024-12-31 --companies 100000
"""
import argparse, os, random, uuid, hashlib
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

random.seed(42); np.random.seed(42)

PRODUCTS = ["SAAS_CORE","SAAS_PLUS","SAAS_ADVANCED","SAAS_PAYROLL","SAAS_LIVE"]
PRODUCT_WGTS = [0.40, 0.25, 0.15, 0.12, 0.08]
SKUS = {"SAAS_CORE":["CORE_MONTHLY","CORE_ANNUAL"],"SAAS_PLUS":["PLUS_MONTHLY","PLUS_ANNUAL"],
        "SAAS_ADVANCED":["ADV_MONTHLY","ADV_ANNUAL"],"SAAS_PAYROLL":["PAYROLL_CORE_MONTHLY","PAYROLL_CORE_ANNUAL"],
        "SAAS_LIVE":["LIVE_MONTHLY"]}
ACCESS_PTS = ["CancelFlowBillingCancel","CancelFlowTalkToExpert","AccountSettingsCancel","MobileAppBillingCancel"]
CANCEL_SUCCESS_DATE = datetime(2024, 5, 7)
IPD_CATALOG = [
    {"offer_id":"OFF-CS-001","ipd_type":"CS IPD","offer_name":"Talk to a Specialist",
     "cta_text":"Talk to a specialist","cta_action":"contact-us-widget","obill_offer_id":None,"show_prob":0.35,"ctr":0.28},
    {"offer_id":"OFF-DISC-50","ipd_type":"Discount IPD","offer_name":"50 pct off 3 months",
     "cta_text":"Claim my discount","cta_action":"external","obill_offer_id":"OBILL-DISC-50PCT-3M","show_prob":0.30,"ctr":0.72},
    {"offer_id":"OFF-DISC-30","ipd_type":"Discount IPD","offer_name":"30 pct off 6 months",
     "cta_text":"Claim my discount","cta_action":"external","obill_offer_id":"OBILL-DISC-30PCT-6M","show_prob":0.20,"ctr":0.65},
    {"offer_id":"OFF-UPGR-001","ipd_type":"Upgrade IPD","offer_name":"Upgrade to Plus",
     "cta_text":"Upgrade and save","cta_action":"external","obill_offer_id":None,"show_prob":0.15,"ctr":0.22},
    {"offer_id":"OFF-DNGR-001","ipd_type":"Downgrade IPD","offer_name":"Switch to Core",
     "cta_text":"Switch to a lower plan","cta_action":"external","obill_offer_id":None,"show_prob":0.10,"ctr":0.31},
    {"offer_id":"OFF-KEEP-001","ipd_type":"Keep my Plan IPD","offer_name":"Keep your plan",
     "cta_text":"Keep my plan","cta_action":"callbackOnly","obill_offer_id":None,"show_prob":0.12,"ctr":0.58},
]

def rts(s, e):
    d = int((e-s).total_seconds())
    return s + timedelta(seconds=random.randint(0, max(d,1)))

def generate_companies(n, start, end):
    prods = np.random.choice(PRODUCTS, size=n, p=PRODUCT_WGTS)
    rows = []
    for i, prod in enumerate(prods):
        sku  = random.choice(SKUS[prod])
        bill = "annual" if "ANNUAL" in sku else "monthly"
        subt = np.random.choice(["direct","accountant_billed","trial"], p=[0.72,0.20,0.08])
        days = min(int(np.random.exponential(500)), 1825)
        rows.append({"company_id": 100_000_000+i, "realm_id": str(100_000_000+i),
                     "product": prod, "sku": sku, "billing_frequency": bill,
                     "subscription_type": subt, "country": "United States", "is_suspicious": 0,
                     "signup_date": (start - timedelta(days=days)).strftime("%Y-%m-%d"),
                     "accountant_realm_id": str(random.randint(9000000,9999999)) if subt=="accountant_billed" else None})
    return pd.DataFrame(rows)

def generate_raw_events(companies, start, end, cancel_rate=0.12):
    evs = []
    mask = np.random.random(len(companies)) < cancel_rate
    cc = companies[mask].reset_index(drop=True)
    print(f"   {len(cc):,} companies with cancel events")
    for _, co in cc.iterrows():
        cid = int(co["company_id"])
        dev = np.random.choice(["desktop","mobile","tablet"], p=[0.62,0.30,0.08])
        n_i = np.random.choice([1,2,3], p=[0.75,0.20,0.05])
        for rank in range(1, n_i+1):
            it = rts(start, end)
            we = it + timedelta(hours=1)
            base = {"company_id": str(cid), "product": co["product"], "sku": co["sku"],
                    "billing_frequency": co["billing_frequency"], "subscription_type": co["subscription_type"],
                    "ua_parser_device_type": dev, "properties_url_host_name": "app.saas.com",
                    "context_page_path": "/app/billing/cancel", "accountant_realm_id": co.get("accountant_realm_id"),
                    "properties_ui_access_point": None, "properties_custom_fp_offer_id": None}
            evs.append({**base, "event": random.choice(["workflow: started","workflow:started"]),
                       "properties_object_detail": random.choice(["cancel","cancellation_workflow"]),
                       "properties_ui_object_detail": random.choice(["cancel_subscription","cancel"]),
                       "event_timestamp": it.isoformat(), "event_date": it.strftime("%Y-%m-%d")})
            cr = 0.28 if it >= CANCEL_SUCCESS_DATE else 0.38
            if random.random() < cr:
                ct = it + timedelta(minutes=random.randint(2,20))
                if it >= CANCEL_SUCCESS_DATE:
                    evs.append({**base, "event": "workflow: completed","properties_object_detail":"cancel",
                               "properties_ui_object_detail":"cancel_success",
                               "event_timestamp": ct.isoformat(),"event_date": ct.strftime("%Y-%m-%d")})
                elif random.random() < 0.6:
                    evs.append({**base,"event":"workflow: engaged","properties_object_detail":"cancel",
                               "properties_ui_object_detail":"yes_cancel",
                               "event_timestamp": ct.isoformat(),"event_date": ct.strftime("%Y-%m-%d")})
                else:
                    evs.append({**base,"event":"cancelation flow: viewed","properties_object_detail":"canceled",
                               "properties_ui_object_detail":None,
                               "properties_ui_access_point":"cancel success",
                               "event_timestamp": ct.isoformat(),"event_date": ct.strftime("%Y-%m-%d")})
            for offer in IPD_CATALOG:
                if random.random() < offer["show_prob"]:
                    ot = it + timedelta(seconds=random.randint(15,300))
                    if ot >= we: continue
                    evs.append({**base,"event":"offer: viewed","properties_object_detail":None,
                               "properties_ui_object_detail":None,
                               "properties_ui_access_point":random.choice(ACCESS_PTS),
                               "properties_custom_fp_offer_id":offer["offer_id"],
                               "event_timestamp":ot.isoformat(),"event_date":ot.strftime("%Y-%m-%d")})
                    if random.random() < offer["ctr"]:
                        ct2 = ot + timedelta(seconds=random.randint(2,30))
                        evs.append({**base,"event":"offer: clicked","properties_object_detail":None,
                                   "properties_ui_object_detail":None,
                                   "properties_ui_access_point":random.choice(ACCESS_PTS),
                                   "properties_custom_fp_offer_id":offer["offer_id"],
                                   "event_timestamp":ct2.isoformat(),"event_date":ct2.strftime("%Y-%m-%d")})
            if random.random() < 0.45:
                dt = it + timedelta(seconds=random.randint(5,60))
                n_pts = random.randint(1,5)
                evs.append({**base,"event":"content: viewed",
                           "properties_object_detail":"usage-highlights-widget",
                           "properties_ui_object_detail": f'{{"data_object_display_count":{n_pts}}}',
                           "event_timestamp":dt.isoformat(),"event_date":dt.strftime("%Y-%m-%d")})
            if random.random() < 0.06:
                ut = it + timedelta(hours=random.randint(1,18))
                act = random.choice(["upgrade","downgrade"])
                evs.append({**base,"event":"workflow: completed","properties_object_detail":act,
                           "properties_ui_object_detail":"get_started",
                           "event_timestamp":ut.isoformat(),"event_date":ut.strftime("%Y-%m-%d")})
    return pd.DataFrame(evs)

def generate_subscriber_status(companies, start, end):
    rows = []
    sample = companies.sample(min(5000, len(companies)), random_state=42)
    for _, co in sample.iterrows():
        d = start
        while d <= end + timedelta(days=95):
            rows.append({"company_id": co["company_id"],"date_of":d.strftime("%Y-%m-%d"),"open_subscriber":1})
            if random.random() < 0.001: break
            d += timedelta(days=1)
    return pd.DataFrame(rows)

def main(args):
    start = datetime.strptime(args.start_date,"%Y-%m-%d")
    end   = datetime.strptime(args.end_date,  "%Y-%m-%d")
    os.makedirs(args.output_path, exist_ok=True)
    print(f"StreamFlow Data Generator | {args.start_date} -> {args.end_date} | {args.companies:,} companies\n")

    print("[1/6] Companies..."); co = generate_companies(args.companies, start, end)
    co.to_parquet(f"{args.output_path}/companies.parquet", index=False); print(f"   {len(co):,} rows")

    print("[2/6] Raw events..."); ev = generate_raw_events(co, start, end)
    ev.to_parquet(f"{args.output_path}/raw_events.parquet", index=False); print(f"   {len(ev):,} rows")

    print("[3/6] Offer catalog..."); oc = pd.DataFrame(IPD_CATALOG)
    oc.to_parquet(f"{args.output_path}/offer_catalog.parquet", index=False); print(f"   {len(oc):,} rows")

    print("[4/6] IXP assignments...")
    ixp_rows = [{"company_id":cid,"experiment_id":304561,
                 "treatment_name":random.choice(["control","variant_a","variant_b"]),
                 "first_assignment_date":rts(start,end-timedelta(days=30)).strftime("%Y-%m-%d"),"version":1}
                for cid in co.sample(frac=0.65,random_state=42)["company_id"]]
    ixp = pd.DataFrame(ixp_rows)
    ixp.to_parquet(f"{args.output_path}/ixp_assignments.parquet", index=False); print(f"   {len(ixp):,} rows")

    print("[5/6] Subscriber status..."); ss = generate_subscriber_status(co, start, end)
    ss.to_parquet(f"{args.output_path}/subscriber_status.parquet", index=False); print(f"   {len(ss):,} rows")

    print("[6/6] Offer history...")
    disc = [o for o in IPD_CATALOG if o.get("obill_offer_id")]
    oh = pd.DataFrame([{"company_id":cid,"offer_id":random.choice(disc)["obill_offer_id"],
                        "purchase_date":rts(start,end).strftime("%Y-%m-%d %H:%M:%S")}
                       for cid in co.sample(frac=0.07,random_state=77)["company_id"]])
    oh.to_parquet(f"{args.output_path}/offer_history.parquet", index=False); print(f"   {len(oh):,} rows")
    print("\nDone!")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start-date",  required=True)
    p.add_argument("--end-date",    required=True)
    p.add_argument("--companies",   type=int, default=50_000)
    p.add_argument("--output-path", default="./data/raw")
    main(p.parse_args())
