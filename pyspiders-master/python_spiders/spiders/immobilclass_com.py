# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'immobilclass_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobilclass_PySpider_italy"
    start_urls = ['http://immobilclass.com/ajax.html?azi=Archivio&lin=it&n=']  # LEVEL 1
    custom_settings = {
        "PROXY_ON" : True,
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [301,302,403,503],
        "LOG_LEVEL" : "DEBUG",
    }
    
    formdata = {
        "H_Url": "http://immobilclass.com/it/affitti/", 
        "Src_Li_Tip": "A",
        "Src_Li_Cat": "",
        "Src_Li_Cit": "",
        "Src_Li_Zon": "",
        "Src_T_Pr1": "", 
        "Src_T_Pr2": "",
        "Src_T_Mq1": "",
        "Src_T_Mq2": "",
        "Src_T_Cod": "",
        "Src_Li_Ord": "",
    }
    
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "ASP.NET_SessionId=naflzgb4radpappiqib3qbpl; coo_pref=-need-notneed-",
        "Origin": "http://immobilclass.com",
        "Pragma": "no-cache",
        "Proxy-Authorization": "Basic bWVobWV0a3VydGlwZWtAZ21haWwuY29tOmZCWlVMc3NaZXNGOUx5RERZdW1F",
        "Proxy-Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    }
    
    def start_requests(self):
        
        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            formdata=self.formdata,
            headers= self.headers
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            controller = response.xpath("//a[i[contains(@class,'right')]]/@href").get()
            if controller:
                seen = True
        
        if page == 2 or seen:
            url = f"http://immobilclass.com/ajax.html?azi=Archivio&lin=it&n={page}"
            
            yield FormRequest(
                url=url,
                callback=self.parse,
                formdata=self.formdata,
                headers= self.headers,
                meta={"page": page+1}
            )
            
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[contains(@class,' lab') and contains(.,'Categoria')]/following-sibling::div/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return
        
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        external_id = response.xpath("//div[contains(@class,' lab') and contains(.,'Codice')]/following-sibling::div/text()").get()
        item_loader.add_value("external_id", external_id)
        
        room_count = response.xpath("//div[contains(@class,' lab') and contains(.,'Local')]/following-sibling::div/text()").get()
        item_loader.add_value("room_count", room_count)
        
        energy_label = response.xpath("//div[contains(@class,' lab') and contains(.,'energetica')]/following-sibling::div//text()").get()
        if energy_label and len(energy_label)==1:
            item_loader.add_value("energy_label", energy_label)
        
        address = "".join(response.xpath("//div[contains(@class,' lab') and contains(.,'Città')]/following-sibling::div//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        
        city = response.xpath("//div[contains(@class,' lab') and contains(.,'Città')]/following-sibling::div//text()").get()
        item_loader.add_value("city", city)
        
        floor = response.xpath("//div[contains(@class,' lab') and contains(.,'Piano')]/following-sibling::div//text()").get()
        item_loader.add_value("floor", floor)
        
        rent = response.xpath("//i[contains(@class,'price')]/following-sibling::text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//i[contains(@class,'design')]/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        bathroom_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[contains(@class,'description')]/text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'map-canvas')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("map-canvas',")[1].split(",")[0]
            lng = lat_lng.split("map-canvas',")[1].split(",")[1].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        item_loader.add_value("landlord_name", "IMMOBILCLASS")
        item_loader.add_value("landlord_phone", "031 241099")
        item_loader.add_value("landlord_email", "info@immobilclass.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower() or " local" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "attico" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "casa indipendente" in p_type_string.lower():
        return "house"
    else:
        return None