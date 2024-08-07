import scrapy
import pandas as pd
import re
import os


class FpsDetailsSpider(scrapy.Spider):
    name = "fps_details"
    start_urls = ["https://epos.kerala.gov.in/dfso_fps_details"]

    def parse(self, response):
        table = response.xpath("//table").get("")
        df = pd.read_html(table)[0]
        df.columns = range(df.shape[1])
        df = df.rename(
            columns={
                0: "Sl.No",
                1: "District Name",
                2: "Total Shops",
                3: "Mapped Shops",
                4: "Not Mapped Shops",
            }
        )
        delete_row = df[df["Sl.No"] == "Total"].index
        df = df.drop(delete_row)
        df.to_csv("fps_details.csv", index=False)
        url = "https://epos.kerala.gov.in/afso_fps_details.action"
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        dist_codes = response.xpath("//td/a/@onclick").getall()
        dist_names = response.xpath("//td/a/text()").getall()
        for dist_code, dist_name in zip(dist_codes, dist_names):
            code = re.findall(r"\d+", dist_code)[0]
            payload = f"dist_code={code}"
            yield response.follow(
                url,
                headers=headers,
                body=payload,
                method="POST",
                callback=self.parse_district,
                cb_kwargs={"district": dist_name},
            )

    def parse_district(self, response, district=None):
        table = response.xpath("//table").get("")
        df1 = pd.read_html(table)[0]
        df1.columns = range(df1.shape[1])
        df1 = df1.rename(
            columns={
                0: "Sl.No",
                1: "AFSO Name",
                2: "Total Shops",
                3: "Mapped Shops",
                4: "Not Mapped Shops",
            }
        )
        df1["District Name"] = district
        # df1.loc[df1["AFSO Name"] == "Total", "District Name"] = ""
        delete_row = df1[df1["Sl.No"] == "Total"].index
        df1 = df1.drop(delete_row)

        if not os.path.isfile("fps_details_district.csv"):
            df1.to_csv("fps_details_district.csv", mode="a", index=None, header="column_names")
        else:
            df1.to_csv("fps_details_district.csv", mode="a", index=None, header=False)
        url = "https://epos.kerala.gov.in/fps_aso_details.action"
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        dist_codes = response.xpath('//input[@id="dist_code"]/@value').getall()
        office_codes = response.xpath('//input[@name="office_code"]/@value').getall()
        afso_names = response.xpath("//td/a/text()").getall()
        for dist_code, office_code, afso_name in zip(
            dist_codes, office_codes, afso_names
        ):
            payload = f"dist_code={dist_code}&office_code={office_code}"
            yield response.follow(
                url,
                headers=headers,
                body=payload,
                method="POST",
                callback=self.parse_ard_details,
                cb_kwargs={"district": district, "afso_name": afso_name},
            )


    def parse_ard_details(self, response, district=None, afso_name=None):
        table = response.xpath("//table").get("")
        df2 = pd.read_html(table)[0]
        df2.columns = range(df2.shape[1])
        df2 = df2.rename(
            columns={
                0: "Sl.No",
                1: "ARD",
                2: "Total Cards",
                3: "License",
                4: "Owner Name",
                5: "Mobile",
                6: "Latitude",
                7: "Longitude",
                8: "Status",
            }
        )
        df2["AFSO Name"] = afso_name
        df2["District Name"] = district
        if not os.path.isfile("ard_details.csv"):
            df2.to_csv("ard_details.csv", mode="a", index=None, header="column_names")
        else:
            df2.to_csv("ard_details.csv", mode="a", index=None, header=False)
