# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'immobilier_rambier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings={
        "PROXY_ON":"True"
    }

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.immobilier-rambier.com/?s=&type=5&l=&pmin=&pmax=&ref=", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.immobilier-rambier.com/?s=&type=6&l=&pmin=&pmax=&ref=", 
                "property_type": "studio"
            },
	        {
                "url": "https://www.immobilier-rambier.com/?s=&type=4&l=&pmin=&pmax=&ref=", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'fadeIn')]/div[@class='row'][2]/div//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"com/page/{page-1}/", "com/").replace("com/", f"com/page/{page}/")
            yield Request(url, callback=self.parse, meta={"page": page+1,'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Immobilier_Rambier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Ref')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@style,'text-align: left')]//h1//strong//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h4[contains(.,'Présentation')]//following-sibling::p//text()", input_type="M_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@style,'text-align: left')]//h1//strong//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@style,'text-align: left')]//h1//strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//h4[contains(.,'Présentation')]//following-sibling::p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface')]//text()", input_type="F_XPATH", get_num=True, split_list={":":1,"m":0,".":0})
        
        room_count = response.xpath("//li[contains(.,'Chambre')]//text()").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath("//li[contains(.,'Pièces')]//text()").get()
                if room_count:
                    room_count = room_count.split(":")[1].strip()
                    item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'Pièces')]//text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip()
                item_loader.add_value("room_count", room_count)

        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat ")[1].split("=")[1].split(";")[0].strip()
            longitude = latitude_longitude.split("lon ")[1].split("=")[1].split(";")[0].strip()   
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
                
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@style,'text-align: right')]//p/text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'carousel slide')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'DPE')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="RAMBIER IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 67 60 55 33", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="rambier@rambier.com", input_type="VALUE")

        yield item_loader.load_item()