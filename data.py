import pandas as pd
import re

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

    def clean_comptotal(
        self,
        col: str = "CompTotal",
        new_col: str | None = None,
        *,
        remove_outliers: bool = False,
        z_thresh: float = 3.0,
    ):
        """Convert compensation strings to float and optionally drop outliers via Z‑score.

        Parameters
        ----------
        col / new_col : str
            Source and destination columns (default keeps same name).
        remove_outliers : bool, default False
            If *True*, values with |Z| > ``z_thresh`` are set to ``NaN``.
        z_thresh : float, default 3.0
            Z‑score threshold to flag outliers.
        """

        new_col = new_col or col

        # Convert to float ------------------------------------------------
        def to_float(x):
            if pd.isnull(x):
                return np.nan
            cleaned = re.sub(r"[^0-9.\-]", "", str(x))
            if cleaned == "":
                return np.nan
            try:
                return float(cleaned)
            except ValueError:
                return np.nan

        self.df[new_col] = self.df[col].apply(to_float)

        # Remove outliers via Z‑score ------------------------------------
        if remove_outliers:
            s = self.df[new_col]
            std = s.std(ddof=0)
            if std == 0 or pd.isna(std):
                # All values identical or not enough data — nothing to mask
                return self
            z = (s - s.mean()) / std
            mask = z.abs() > z_thresh
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
    .clean_comptotal(remove_outliers=True, z_thresh=3)
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
