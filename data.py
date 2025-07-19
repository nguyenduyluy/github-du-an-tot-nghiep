import pandas as pd
import re
import numpy as np

class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def clean_text(self, text: str) -> str:
        if pd.isnull(text):
            return ""
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text

    def standardize_mainbranch(self, col='MainBranch', new_col=None):
        new_col = new_col or col
        self.df[new_col] = self.df[col].apply(self.clean_text)
        return self

    def label_mainbranch(self, clean_col='MainBranch', label_col='BranchGroup'):
        def label(row):
            txt = row.lower()
            if "not primarily" in txt or "not a developer" in txt:
                return "Semi-technical"
            elif "developer" in txt:
                return "Developer"
            else:
                return "Other"
        self.df[label_col] = self.df[clean_col].apply(label)
        return self

    def clean_age(self, col='Age', new_col=None):
        new_col = new_col or col
        def extract_agegroup(age_text):
            if pd.isnull(age_text):
                return None
            match = re.search(r'\d{2}-\d{2}', str(age_text))
            return match.group(0) if match else None
        self.df[new_col] = self.df[col].apply(extract_agegroup)
        return self

    def clean_edlevel(self, col='EdLevel', new_col=None):
        new_col = new_col or col
        def map_education(value):
            if pd.isnull(value):
                return "Unknown"
            val = value.lower()
            if "bachelor" in val:
                return "Bachelor"
            elif "master" in val:
                return "Master"
            elif "professional degree" in val:
                return "Professional"
            elif "associate" in val:
                return "Associate"
            elif "secondary" in val or "high school" in val:
                return "HighSchool"
            elif "some college" in val:
                return "No Degree"
            elif "doctoral" in val or "ph.d" in val:
                return "Doctorate"
            else:
                return "Other"
        self.df[new_col] = self.df[col].apply(map_education)
        return self

    def clean_country(self, col='Country', new_col=None):
        new_col = new_col or col
        self.df[new_col] = self.df[col].apply(lambda x: self.clean_text(str(x)).title())
        return self
    
    def clean_currency(self, col='Currency', new_col=None):
        new_col = new_col or col
        self.df[new_col] = self.df[col].astype(str).str.strip().str[:3].str.upper()
        return self

    def convert_currency_to_usd(self, amount_col='CompTotal', currency_col='Currency', new_col='CompTotalUSD'):
        rates = {
        "USD": 1.00,
        "EUR": 1.10,
        "GBP": 1.30,
        "INR": 0.012,
        "VND": 0.000042,
        "JPY": 0.0068,
        "CNY": 0.14,
        "CAD": 0.73,
        "AUD": 0.66,
        "BRL": 0.19,
        "NOK": 0.093,
        "SEK": 0.094,
        "DKK": 0.15,
        "MXN": 0.058,
        "PEN": 0.26,
        "LKR": 0.0031,
        "UAH": 0.025,
        "ILS": 0.27,
        "PLN": 0.26,
        "ZAR": 0.0558,
        "CHF": 1.12,
        "SGD": 0.74,
        "HKD": 0.128,
        "MYR": 0.21,
        "THB": 0.027,
        "IDR": 0.000063,
        "KRW": 0.00072,
        "EGP": 0.021,
        "NGN": 0.00067,
        "PKR": 0.0036,
        "BDT": 0.0086,
        "CZK": 0.043,
        "HUF": 0.0027,
        "RON": 0.22,
        "SAR": 0.27,
        "AED": 0.27,
        "KWD": 3.25,
        "BHD": 2.65,
        "QAR": 0.27,
        "OMR": 2.60,
        "CLP": 0.0011,
        "COP": 0.00026,
        "ARS": 0.0009,
        "UYU": 0.025,
        "BOB": 0.14,
        "PYG": 0.00014,
        "DOP": 0.017,
        "GTQ": 0.13,
        "HNL": 0.041,
        "NIO": 0.027,
        "CRC": 0.0019,
        "JMD": 0.0064,
        "XOF": 0.0017,
        "XAF": 0.0017,
        "CDF": 0.00037,
        "GHS": 0.084,
        "TZS": 0.00039,
        "KES": 0.0078,
        "UGX": 0.00027,
        "ETB": 0.017,
        "MWK": 0.00059,
        "ZMW": 0.046,
        "MZN": 0.016,
        "BWP": 0.073,
        "MUR": 0.022,
        "MAD": 0.10,
        "TND": 0.32,
        "DZD": 0.0074,
        "LBP": 0.000009,  # biến động mạnh
        "SYP": 0.000079,
        "IRR": 0.000024,
        "IQD": 0.00076,
        "AFN": 0.012,
        "NPR": 0.0075,
        "MMK": 0.00047,
        "MNT": 0.00029,
        "KZT": 0.0022,
        "UZS": 0.000082,
        "AZN": 0.59,
        "GEL": 0.36,
        "AMD": 0.0025,
        "ALL": 0.011,
        "MKD": 0.017,
        "ISK": 0.0073,
        "BAM": 0.55,
        "RSD": 0.0093,
        "BYN": 0.31,
        "RUB": 0.011,
        "VES": 0.028,
        "BGN": 0.56,
        "NZD": 0.62,
        "GBP": 1.30,
        "BRL": 0.19,
        "IRR": 0.000024,
        "SEK": 0.095,
        "PLN": 0.25,
        "CHF": 1.12,
        "AUD": 0.67,
        "HKD": 0.13,
        "TWD": 0.031,
        "PHP": 0.018,
        "AED": 0.27,
        "TRY": 0.032,
        "NZD": 0.62,
        "IDR": 0.000065,
        "MGA": 0.00022,
        "BGN": 0.56,
        "RWF": 0.00081,
        "TTD": 0.15,
        "JOD": 1.41,
        "AOA": 0.0012,
        "KGS": 0.011,
        "MOP": 0.12,
        "TJS": 0.091,
        "TMT": 0.29,
        "CUP": 0.041,
        "BTN": 0.012,
        "MVR": 0.065


    }
        self.df[currency_col] = self.df[currency_col].astype(str).str.strip().str.upper()
        self.df[amount_col] = pd.to_numeric(self.df[amount_col], errors='coerce')

        def convert(row):
            currency = row[currency_col]
            amount = row[amount_col]
            rate = rates.get(currency)
            if rate is None or pd.isnull(amount):
                return None
            return amount * rate

        self.df[new_col] = self.df.apply(convert, axis=1)
        return self


    # Dùng Z-Score để xử lý ngoại lai
    
    def clean_comptotal(
        self,
        col: str = "CompTotalUSD",
        new_col: str | None = None,
        remove_outliers: bool = True,
        replace_with_mean: bool = True,
        min_value: float | None = None  # loại các giá trị quá nhỏ nếu muốn
    ) -> "DataCleaner":
        new_col = new_col or col

        def to_float(x):
            if pd.isnull(x):
                return np.nan
            cleaned = re.sub(r"[^0-9.\-]", "", str(x))
            try:
                return float(cleaned) if cleaned else np.nan
            except ValueError:
                return np.nan

        # Áp dụng hàm chuyển đổi
        self.df[new_col] = self.df[col].apply(to_float)

        s = self.df[new_col]

        # Loại bỏ giá trị nhỏ hơn ngưỡng (nếu min_value được cung cấp)
        if min_value is not None:
            self.df.loc[s < min_value, new_col] = np.nan

        # Nếu cần loại bỏ outliers bằng IQR
        if remove_outliers:
            s = self.df[new_col]
            q1, q3 = s.quantile([0.25, 0.75])
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            mask = (s < lower_bound) | (s > upper_bound)

            if replace_with_mean:
                mean_value = s[~mask].mean()
                self.df.loc[mask, new_col] = mean_value
            else:
                self.df.loc[mask, new_col] = np.nan

        return self



    def clean_remotework(self, col='RemoteWork', new_col=None):
        new_col = new_col or col
        def map_remote(value):
            if pd.isnull(value):
                return "Unknown"
            val = value.lower()
            if "remote" in val and "in-person" in val:
                return "Hybrid"
            elif "remote" in val:
                return "Remote"
            elif "in-person" in val:
                return "In-person"
            else:
                return "Other"
        self.df[new_col] = self.df[col].apply(map_remote)
        return self

    def clean_employment(self, col='Employment', new_col=None):
        new_col = new_col or col
        def map_employment(value):
            if pd.isnull(value):
                return "Unknown"
            value = value.lower()
            labels = []
            if "employed" in value:
                labels.append("Employ")
            if "student" in value:
                labels.append("Student")
            if "independent contractor" in value or "freelancer" in value or "self-employed" in value:
                labels.append("Freelance")
            return "-".join(sorted(set(labels))) if labels else "Other"
        self.df[new_col] = self.df[col].apply(map_employment)
        return self

    def clean_orgsize(self, col='OrgSize', new_col=None):
        new_col = new_col or col
        def map_orgsize(value):
            if pd.isnull(value):
                return "Unknown"
            val = value.lower()
            if "10 to 19" in val or "20 to 99" in val:
                return "Small"
            elif "100 to 499" in val or "500 to 999" in val:
                return "Medium"
            elif "1,000 to 4,999" in val or "5,000 to 9,999" in val:
                return "Large"
            elif "10,000" in val:
                return "Enterprise"
            elif "fewer than 10" in val:
                return "Micro"
            else:
                return "Other"
        self.df[new_col] = self.df[col].apply(map_orgsize)
        return self

    def clean_time_columns(self, cols=['TimeSearching', 'TimeAnswering']):
        def convert_to_minutes(value):
            if pd.isnull(value):
                return None
            value = value.lower()
            if "less than 15" in value:
                return 10
            elif "15-30" in value:
                return 22
            elif "30-60" in value:
                return 45
            elif "60-120" in value:
                return 90
            elif "over 120" in value:
                return 150
            else:
                return None

        for col in cols:
            self.df[col] = self.df[col].apply(convert_to_minutes)
        return self

    def clean_survey_experience(self, length_col='SurveyLength', ease_col='SurveyEase'):
        length_map = {
            "Too short": -1,
            "Appropriate in length": 0,
            "Too long": 1
        }
        ease_map = {
            "Difficult": -1,
            "Neither easy nor difficult": 0,
            "Easy": 1
        }
        self.df[length_col] = self.df[length_col].map(length_map)
        self.df[ease_col] = self.df[ease_col].map(ease_map)
        return self

    def clean_years_code_pro(self, col='YearsCodePro', new_col=None):
        new_col = new_col or col
        def parse(val):
            if pd.isnull(val):
                return None
            val = str(val).lower()
            if "less than" in val:
                return 0.5
            elif "more than" in val:
                return 51
            try:
                return float(val)
            except:
                return None
        self.df[new_col] = self.df[col].apply(parse)
        return self

    def clean_dev_type(self, col='DevType', new_col=None):
        new_col = new_col or col
        def map_type(text):
            if pd.isnull(text):
                return "Unknown"
            text = text.lower()
            if "full-stack" in text:
                return "Full-Stack Developer"
            elif "back-end" in text:
                return "Back-End Developer"
            elif "front-end" in text:
                return "Front-End Developer"
            elif "mobile" in text:
                return "Mobile Developer"
            elif "embedded" in text:
                return "Embedded Developer"
            elif "game" in text:
                return "Game Developer"
            elif "engineering manager" in text:
                return "Manager"
            elif "research" in text:
                return "Researcher"
            else:
                return "Other"
        self.df[new_col] = self.df[col].apply(map_type)
        return self

    def clean_buildvsbuy(self, col='BuildvsBuy', new_col=None):
        new_col = new_col or col
        def map_text(x):
            if pd.isnull(x):
                return "Unknown"
            x = x.strip().lower()
            if x.startswith("out-of-the-box"):
                return "Ready-to-go"
            elif "ready-to-go but also customizable" in x:
                return "Ready+Customizable"
            elif "customized and needs to be engineered" in x:
                return "Requires Customization"
            else:
                return "Other"
        self.df[new_col] = self.df[col].apply(map_text)
        return self

    def clean_semicolon_columns(self, cols):
        for col in cols:
            self.df[col] = self.df[col].apply(
                lambda x: '-'.join([item.strip() for item in str(x).split(';') if item.strip()])
                if pd.notnull(x) else "")
        return self

    def clean_SOComm_text(self, col='SOComm'):
        mapping = {
            'Yes, definitely': 'Yes',
            'Yes, somewhat': 'Somewhat',
            'No, not really': 'No',
            'No, not at all': 'Strong No',
            'Neutral': 'Neutral'
        }
        self.df[col] = self.df[col].map(mapping)
        return self

    def get_result(self):
        return self.df

    def summary(self, col):
        return self.df[col].value_counts(dropna=False)


# Đọc dữ liệu
df = pd.read_excel("H:\\Dự án tốt nghiệp\\github_project\\du_lieu.xlsx")

# Các cột cần xử lý chuỗi phân cách
semicolon_cols = [
    'TechEndorse',
    'NEWCollabToolsHaveWorkedWith',
    'NEWCollabToolsWantToWorkWith',
    'OpSysProfessional use',
    'OfficeStackAsyncHaveWorkedWith',
    'OfficeStackSyncHaveWorkedWith',
    'AIToolCurrently Using',
    'AIBen'
]

# Xử lý
df_clean = (DataCleaner(df)
    .standardize_mainbranch()
    .label_mainbranch()
    .clean_age()
    .clean_country()
    .clean_currency()
    .convert_currency_to_usd() 
    .clean_comptotal(remove_outliers=True, replace_with_mean=True, min_value=100)
    .clean_edlevel()
    .clean_remotework()
    .clean_employment()
    .clean_orgsize()
    .clean_time_columns()
    .clean_survey_experience()
    .clean_years_code_pro()
    .clean_dev_type()
    .clean_buildvsbuy()
    .clean_semicolon_columns(semicolon_cols)
    .clean_SOComm_text()
    .get_result()
)

# Xuất kết quả
df_clean.to_excel("H:\\Dự án tốt nghiệp\\du_lieu_sach_tong_hop.xlsx", index=False)
print("Hoàn tất xử lý dữ liệu.")
