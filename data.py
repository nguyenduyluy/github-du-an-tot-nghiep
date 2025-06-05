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
# Chuẩn hóa cột mainbranch
    def standardize_mainbranch(self, col='MainBranch', new_col='MainBranch_Clean'):
        self.df[new_col] = self.df[col].apply(self.clean_text)
        return self

    def label_mainbranch(self, clean_col='MainBranch_Clean', label_col='BranchGroup'):
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
# Chuẩn hóa cột age
    def clean_age(self, col='Age', new_col='AgeGroup'):
        def extract_agegroup(age_text):
            if pd.isnull(age_text):
                return None
            match = re.search(r'\d{2}-\d{2}', str(age_text))
            return match.group(0) if match else None
        self.df[new_col] = self.df[col].apply(extract_agegroup)
        return self
# Chuẩn hóa cột edlevel   
    def clean_edlevel(self, col='EdLevel', new_col='EdLevel_Clean'):
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
# Chuẩn hóa cột country
    def clean_country(self, col='Country', new_col='Country_Clean'):
        self.df[new_col] = self.df[col].apply(lambda x: self.clean_text(str(x)).title())
        return self
# Chuẩn hóa cột comptotal
    def clean_comptotal(self, col='CompTotal', new_col='CompTotal_Clean'):
        self.df[new_col] = pd.to_numeric(self.df[col], errors='coerce')
        return self
# Chuẩn hóa cột remotework
    def clean_remotework(self, col='RemoteWork', new_col='RemoteWork_Clean'):
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
# Chuẩn hóa cột employment
    def clean_employment(self, col='Employment', new_col='EmploymentGroup'):
        def map_employment(value):
            if pd.isnull(value):
                return "Unknown"
            value = value.lower()
            labels = []
            if "employed" in value:
                labels.append("Employed")
            if "student" in value:
                labels.append("Student")
            if "independent contractor" in value or "freelancer" in value or "self-employed" in value:
                labels.append("Freelance")
            return " + ".join(sorted(set(labels))) if labels else "Other"
        self.df[new_col] = self.df[col].apply(map_employment)
        return self

# Chuẩn hóa cột orgsize
    def clean_orgsize(self, col='OrgSize', new_col='OrgSizeGroup'):
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

# Chuẩn hóa cột time
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
            new_col = col + '_Minutes'
            self.df[new_col] = self.df[col].apply(convert_to_minutes)
        return self
    
# Chuẩn hóa cột survey
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

        self.df["SurveyLengthScore"] = self.df[length_col].map(length_map)
        self.df["SurveyEaseScore"] = self.df[ease_col].map(ease_map)

        return self


    def get_result(self):
        return self.df

    def summary(self, col):
        return self.df[col].value_counts(dropna=False)


# Đọc file gốc
df = pd.read_excel("H:\\Dự án tốt nghiệp\\du_lieu.xlsx")

# Khởi tạo và chạy chuỗi xử lý
cleaner = DataCleaner(df)
df_clean = (cleaner
            .standardize_mainbranch()
            .label_mainbranch()
            .clean_age()
            .clean_country()
            .clean_comptotal()
            .clean_edlevel()
            .clean_remotework()
            .clean_employment()
            .clean_orgsize()
            .clean_time_columns()
            .clean_survey_experience()
            .get_result() )

# Xem kết quả
print(df_clean.head())

# Xuất file sạch
df_clean.to_excel("H:\\Dự án tốt nghiệp\\du_lieu_sach_tong_hop.xlsx", index=False)
