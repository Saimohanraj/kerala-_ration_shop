import scrapy
import re

class keralaSpider(scrapy.Spider):
    name = "kerala"

    def start_requests(self):        
        start_url = "https://epos.kerala.gov.in/FPS_Trans_Abstract.jsp"
        yield scrapy.Request(start_url, callback=self.parse)


    def parse(self, response):   
        for district in response.xpath('//select[@name="dist_code"]/option[not(contains(text(),"Select"))]'):
            district_name = district.xpath('.//text()').get('').strip()
            district_id = district.xpath('.//@value').get('').strip()
            dict_url = f'https://epos.kerala.gov.in/AjaxExecution.jsp?select=true&type=tso&param={district_id}'
            yield scrapy.Request(url=dict_url, callback=self.parse_fps, cb_kwargs={"district_name": district_name, "district_id": district_id})
    

    def parse_fps(self, response, district_name, district_id):
        for fps_sub_district in response.xpath('//option[not(contains(text(),"select"))]'):
            tso = fps_sub_district.xpath('.//text()').get('').strip()
            tso_name = tso.replace("\n\t", " ").replace("\n", "").replace("\r", "").strip()
            tso_id = fps_sub_district.xpath('.//@value').get('').strip()
            tso_id_url = f"https://epos.kerala.gov.in/AjaxExecution.jsp?select=true&type=fps&param={tso_id}"
            yield scrapy.Request(tso_id_url,callback=self.parse_fps_id,cb_kwargs={"district_name": district_name,"district_id": district_id,"tso_name": tso_name,"tso_id": tso_id})

    
    def parse_fps_id(self, response, district_name, district_id, tso_name, tso_id ):
        for fps_ids in response.xpath('//option[not(contains(text(),"select"))]'):
            ard = fps_ids.xpath('.//text()').get('').strip()
            ard_name = ard.replace("\n\t", " ").replace("\n", "").replace("\r", "").strip()
            ard_id = fps_ids.xpath('.//@value').get('').strip()
            yield {"district_name": district_name,"district_id": district_id,"tso_name": tso_name,"tso_id": tso_id,"ard_name":ard_name,"ard_id":ard_id}
  