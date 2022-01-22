# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'quintainliving_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Quintainliving_PySpider_united_kingdom"
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
    }
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.quintainliving.com/search-units",
                "property_type" : "house"
            },

        ] #LEVEL-1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,headers=self.headers,
                                 meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response): 
        data=str(response.body).split('"qlSearch":{"units":"')[-1].split('"},"tipi"')[0].replace("\\u0022","").replace("\\","")
        data=data.split("title")

        for item in data:
            images=item.split("images")[-1].split(",floorplan")[0]
            link=item.split("link:")[-1].split(",t_c")[0]
            follow_url = response.urljoin(link)
            yield Request(follow_url, callback=self.populate_item,meta={"images":images,"item":item})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        summary = " ".join(response.xpath("//div[@class='a-copy']//text()").getall()).strip() 
        if get_p_type_string(summary):
            item_loader.add_value("property_type", get_p_type_string(summary))
        item_loader.add_value("external_source", self.external_source)
        external_id = response.url.split('/')[-1].split('-')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        address = " ".join(response.xpath("//h1[@class='a-title']//text()").getall()).strip()
        if address:
            address = re.sub('\s{2,}', ' ', address)
            item_loader.add_value("address", address.strip())
        item_loader.add_value("city", "Wembley")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='a-copy']//text()").getall()).strip()   
        if description:
            description = re.sub('\s{2,}', ' ', description)
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[@class='m-feature col-md-6 col-xl-3']//text()[contains(.,'sqm')]").get()
        if square_meters:
            square_meters = square_meters.split('sqm')[0].split('|')[-1].strip()
            item_loader.add_value("square_meters", str(int(float(square_meters))))

        room_count = str(response.meta.get("item")).split("bedrooms:")[1].split(",")[0]
        if room_count and "0"!=room_count:
            item_loader.add_value("room_count", room_count)
        rent = response.xpath("//span[@class='rent-value']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",",""))
            item_loader.add_value("currency", 'GBP')
        
        images = [x for x in response.meta.get("images").split(",")]
        if images:
            item_loader.add_value("images", images)

        balcony=str(response.meta.get("item")).split("has_balcony:")[1].split(",")[0]
        if balcony and "true"==balcony:
            item_loader.add_value("balcony",True)
        terrace=str(response.meta.get("item")).split("has_terrace:")[1].split(",")[0]
        if terrace:
            if "0"==terrace:
                item_loader.add_value("terrace",False)
            else:
                item_loader.add_value("terrace",True)
        parking=str(response.meta.get("item")).split("has_parking:")[1].split(",")[0]
        if parking:
            if "0"==parking:
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)
             
         
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//a[contains(.,'View full floorplan')]/..//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        furnished = response.xpath("//div[@class='m-feature col-md-6 col-xl-3']//text()[contains(.,'furnished') or contains(.,'Furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        available_date=str(response.meta.get("item")).split("move_in_date:")[1].split(",")[0]
        if available_date:
            item_loader.add_value("available_date",available_date.strip())
        item_loader.add_value("landlord_name", "Quintain Living")
        item_loader.add_value("landlord_phone", "020 3151 1927")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"    
    else:
        return None
