"""Whitelisted enums for transaction classification.

Kept separate from extractor.py so the web layer can import the lists
without pulling in provider deps."""

# Transaction categories for cashflow analysis. The list is deliberately
# short and German-leaning since most users running this on a Synology VM
# have German bank statements; localisation happens in the i18n layer
# rather than by translating these canonical keys.
TX_CATEGORIES = (
    "miete",            # rent / housing-related fixed costs
    "nebenkosten",      # utilities (Strom, Gas, Wasser, GEZ)
    "lebensmittel",     # supermarkt purchases
    "essen-ausser-haus",# restaurants, takeaway, cafés
    "mobilitaet",       # public transport, fuel, parking, car
    "versicherung",     # any insurance premium
    "abonnement",       # streaming, gym, software subs, mobile contract
    "gesundheit",       # pharmacy, doctor, copays
    "freizeit",         # leisure: cinema, concerts, hobbies
    "bekleidung",       # clothes, shoes
    "elektronik",       # gadgets, electronics
    "haushalt",         # home goods, drugstore
    "bildung",          # books, courses
    "spende",           # donations
    "gehalt",           # salary in
    "rente-zuschuss",   # pension / state benefits in
    "erstattung",       # refunds, reimbursements
    "zins-dividende",   # interest, dividends
    "uebertrag",        # transfer between own accounts
    "bargeld",          # ATM withdrawals
    "gebuehr",          # bank fees
    "steuer",           # tax payments / refunds
    "sonstiges",        # fallback
)

# Transaction types — closer to what the bank actually prints next to
# each line. Useful for filtering ("show all Lastschriften") and for
# detecting recurring debits.
TX_TYPES = (
    "ueberweisung",     # incoming or outgoing wire
    "lastschrift",      # SEPA direct debit
    "dauerauftrag",     # standing order
    "kartenzahlung",    # debit/credit card POS
    "bargeld",          # ATM
    "gehalt",           # explicit salary booking
    "gebuehr",          # bank fee
    "zinsen",           # interest credit/debit
    "uebertrag",        # internal transfer
    "sonstiges",        # fallback
)

# Common German banks — whitelisted so the extractor settles on one
# canonical name even when the OCR header is messy. Anything else falls
# through and gets stored verbatim.
BANK_NAMES = (
    "Sparkasse", "Volksbank", "DKB", "ING", "Comdirect", "Commerzbank",
    "Deutsche Bank", "Postbank", "HypoVereinsbank", "Targobank",
    "Santander", "N26", "Revolut", "Wise", "C24", "Norisbank",
    "1822direkt", "PSD Bank", "Sparda-Bank", "Consorsbank", "PayPal",
)
