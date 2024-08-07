import scrapy
import pandas as pd
import re


def data_clean(data):
    cleaned_data = ""
    if data:
        cleaned_data = re.sub(r"\s+", " ", data)
    else:
        cleaned_data = data
    return cleaned_data


class PdsSpider(scrapy.Spider):
    name = 'pds_2_kerala'
    handle_httpstatus_list = [500]

    def __init__(self, month, year, **kwargs):
        self.month = str(month)
        self.year = str(year)
        super().__init__(**kwargs)

    def start_requests(self):
        df = pd.read_csv("merged_data_kerala.csv")
        df_data = df.to_dict("records")
        for row in df_data:
            district_id = row.get('district_id')
            ard_name = row.get('ard_name')
            ard_id = row.get('ard_id')
            district_name = row.get('district_name')
            tso_name = row.get('tso_name')
            tso_id = row.get('tso_id')
            latitude = row.get('Latitude')
            longitude = row.get('Longitude')
            url = "https://epos.kerala.gov.in/FPS_Trans_Details.jsp"
            payload = f"dist_code={district_id}&fps_id={ard_id}&month={str(self.month)}&year={str(self.year)}"
            headers = {
                'Accept': '*/*',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
                'sec-ch-ua-platform': '"Linux"',
                'Origin': 'https://epos.kerala.gov.in',
                'Referer': 'https://epos.kerala.gov.in/FPS_Trans_Abstract.jsp',
                'Accept-Language': 'en-US,en;q=0.9',

            }
            yield scrapy.Request(url=url, method="POST",
                                 body=payload, headers=headers,  callback=self.parse,
                                 cb_kwargs={"year": self.year, "month": self.month, "ard_id": ard_id,
                                            "district_id": district_id, "district_name": district_name,
                                            "ard_name": ard_name, "tso_name": tso_name, "tso_id": tso_id,
                                            "latitude": latitude, "longitude": longitude})

    def parse(self, response, year, month, ard_id, ard_name, district_id, district_name, tso_name, tso_id, latitude, longitude):
        info = str(year) + "--" + str(month) + "--" + str(ard_id) + "--" + str(ard_name) + "--" + str(district_id) + \
            "--" + str(district_name) + "--" + str(tso_id) + "--" + \
            str(tso_name) + "--" + str(latitude) + "--" + str(longitude)

        if response.status == 500:
            with open('500_response_data.txt', 'a') as f:
                f.write("%s\n" % info)
        elif "No Transaction data found" in response.text:
            with open('no_transaction_data.txt', 'a') as f:
                f.write("%s\n" % info)
        elif len(response.xpath('//th[contains(text(),"Sl No")]/ancestor::thead/following-sibling::tbody/tr')) < 1:
            with open('no_table_tr_data.txt', 'a') as f:
                f.write("%s\n" % info)
        elif response.status == 200:
            table_headers = []
            for head in response.xpath('//thead/tr[@class="tableheader"]/th[not(@colspan)]'):
                head_name = head.xpath('.//text()').get('').strip()
                table_headers.append(head_name)
            row_td_length = len(response.xpath(
                '//th[contains(text(),"Sl No")]/ancestor::thead/following-sibling::tbody/tr[1]/td'))
            td_key = {}
            for index, value in enumerate(table_headers):
                td_key[value] = index+1
            for row in response.xpath('//th[contains(text(),"Sl No")]/ancestor::thead/following-sibling::tbody/tr'):
                items = {}
                items["year"] = year
                items["month"] = month
                items["district_id"] = district_id
                items["district_name"] = district_name
                items["tso_name"] = tso_name
                items["tso_id"] = tso_id
                items["ard_id"] = ard_id
                items["ard_name"] = data_clean(ard_name)
                items["latitude"] = latitude.replace("-NA-", "") if "-NA-" in latitude else latitude
                items["longitude"] = longitude.replace("-NA-", "") if "-NA-" in longitude else longitude
                items["rc_no"] = row.xpath('.//td[2]/text()').get('').strip()
                items["scheme"] = row.xpath('.//td[3]/text()').get('').strip()
                items["avail_type"] = row.xpath('.//td[4]/text()').get('').strip()
                receipt_no = row.xpath('.//td[5]/text()').get('').strip()
                items["receipt_no"] = str(receipt_no)
                items["date"] = row.xpath('.//td[6]/text()').get('').strip()
                items["rr_kgs"] = row.xpath(
                    f'.//td[{str(td_key.get("RR")-3)}]/text()').get('').strip() if "RR" in td_key.keys() else ""
                items["br_kgs"] = row.xpath(f'.//td[{str(td_key.get("BR")-3)}]/text()').get(
                    '').strip() if "BR" in td_key.keys() else ""
                items["atta_pkts"] = row.xpath(
                    f'.//td[{str(td_key.get("Atta")-3)}]/text()').get('').strip() if "Atta" in td_key.keys() else ""
                items["wheat_kgs"] = row.xpath(
                    f'.//td[{str(td_key.get("Wheat")-3)}]/text()').get('').strip() if "Wheat" in td_key.keys() else ""
                items['sugar_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("Sugar")-3)}]/text()').get('').strip() if "Sugar" in td_key.keys() else ""
                items['koil_ltrs'] = row.xpath(
                    f'.//td[{str(td_key.get("Koil")-3)}]/text()').get('').strip() if "Koil" in td_key.keys() else ""
                items['pmgkaydal_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("PMGKAYDAL")-3)}]/text()').get('').strip() if "PMGKAYDAL" in td_key.keys() else ""
                items['pmgkaybr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("PMGKAYBR")-3)}]/text()').get('').strip() if "PMGKAYBR" in td_key.keys() else ""
                items['sbr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("SBR")-3)}]/text()').get('').strip() if "SBR" in td_key.keys() else ""
                items['pmgkaywheat'] = row.xpath(
                    f'.//td[{str(td_key.get("PMGKAYWheat")-3)}]/text()').get('').strip() if "PMGKAYWheat" in td_key.keys() else ""
                items['srr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("SRR")-3)}]/text()').get('').strip() if "SRR" in td_key.keys() else ""
                items['pmgkayrr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("PMGKAYRR")-3)}]/text()').get('').strip() if "PMGKAYRR" in td_key.keys() else ""
                items['koiln_ltrs'] = row.xpath(
                    f'.//td[{str(td_key.get("KoilN")-3)}]/text()').get('').strip() if "KoilN" in td_key.keys() else ""
                items['koilnsall_ltrs'] = row.xpath(
                    f'.//td[{str(td_key.get("KoilNSALL")-3)}]/text()').get('').strip() if "KoilNSALL" in td_key.keys() else ""
                items['cmr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("CMR")-3)}]/text()').get('').strip() if "CMR" in td_key.keys() else ""
                items['frr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("FRR")-3)}]/text()').get('').strip() if "FRR" in td_key.keys() else ""
                items['ffr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("FFR")-3)}]/text()').get('').strip() if "FFR" in td_key.keys() else ""
                items['fbr_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("FBR")-3)}]/text()').get('').strip() if "FBR" in td_key.keys() else ""
                items['fkoil'] = row.xpath(
                    f'.//td[{str(td_key.get("FKoil")-3)}]/text()').get('').strip() if "FKoil" in td_key.keys() else ""
                items['freekitspl'] = row.xpath(
                    f'.//td[{str(td_key.get("FreeKitSPL")-3)}]/text()').get('').strip() if "FreeKitSPL" in td_key.keys() else ""
                items['cffreekitspl'] = row.xpath(
                    f'.//td[{str(td_key.get("CFFreeKitSPL")-3)}]/text()').get('').strip() if "CFFreeKitSPL" in td_key.keys() else ""
                items['sub_kit_freekit'] = row.xpath(
                    f'.//td[{str(td_key.get("SUB KIT")-3)}]/text()').get('').strip() if "SUB KIT" in td_key.keys() else ""
                items['deckit_freekit'] = row.xpath(
                    f'.//td[{str(td_key.get("DECKIT")-3)}]/text()').get('').strip() if "DECKIT" in td_key.keys() else ""
                items['one_freekit'] = row.xpath(
                    f'.//td[{str(td_key.get("1")-3)}]/text()').get('').strip() if "1" in td_key.keys() else ""                
                items['mattaspl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("MattaSPL")-3)}]/text()').get('').strip() if "MattaSPL" in td_key.keys() else ""
                items['rrf_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("RRF")-3)}]/text()').get('').strip() if "RRF" in td_key.keys() else ""
                items['cmrf_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("CMRF")-3)}]/text()').get('').strip() if "CMRF" in td_key.keys() else ""
                items['cmrf_spl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("CMR F. SPL")-3)}]/text()').get('').strip() if "CMR F. SPL" in td_key.keys() else ""
                items['brf_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("BRF")-3)}]/text()').get('').strip() if "BRF" in td_key.keys() else ""
                items['brspl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("BRSPL")-3)}]/text()').get('').strip() if "BRSPL" in td_key.keys() else ""
                items['brfspl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("BR F. SPL")-3)}]/text()').get('').strip() if "BR F. SPL" in td_key.keys() else ""
                items['rrspl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("RRSPL")-3)}]/text()').get('').strip() if "RRSPL" in td_key.keys() else ""
                items['rrfspl_kgs'] = row.xpath(
                    f'.//td[{str(td_key.get("RR F. SPL")-3)}]/text()').get('').strip() if "RR F. SPL" in td_key.keys() else ""
                items['rgp_pkts'] = row.xpath(
                    f'.//td[{str(td_key.get("RGP")-3)}]/text()').get('').strip() if "RGP" in td_key.keys() else ""
                items["amount"] = row.xpath(
                    f'.//td[{str(row_td_length-2)}]/text()').get('').strip()
                items["portability"] = row.xpath(
                    f'.//td[{str(row_td_length-1)}]/text()').get('').strip()
                items["auth_trans_time"] = row.xpath(
                    f'.//td[{str(row_td_length)}]/text()').get('').strip()
                yield items
