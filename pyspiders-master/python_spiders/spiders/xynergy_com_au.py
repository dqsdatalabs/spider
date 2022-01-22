# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'xynergy_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.xynergy.com.au/for-lease/properties-for-lease",
                ]
            }

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            dont_filter=True)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)       
        seen = False
        for item in response.xpath("//a[@class='embed-responsive-item']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page==2 or seen:
            url = f"https://www.xynergy.com.au/for-lease/properties-for-lease/page/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})
        


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", "Xynergy_PySpider_australia")
        propertype =response.xpath("//li//label[contains(.,'Property Type')]/following-sibling::div/text()").get()
        if get_p_type_string(propertype):
            item_loader.add_value("property_type", get_p_type_string(propertype))
        else:
            return

        item_loader.add_value("external_id", response.url.split("/")[-1])
        title =response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        sale=response.xpath("//div[@class='listing-page-description row no-gutters my-5']/div/h5[@class='sub-title']/text()[contains(.,'sale')]").get()
        if sale:
            return 
        desc = "".join(response.xpath("//div[@class='detail-description mb-3']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        address =response.xpath("//h2[@class='property-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(" ")[-2].strip()
            item_loader.add_value("city", city.strip())
            zipcode=address.split(" ")[0] 
            item_loader.add_value("zipcode", zipcode.strip())
        rent =response.xpath("//li//label[contains(.,'Price')]/following-sibling::div/text()").get()
        if rent and "$" in rent:
            rent = rent.split("$")[1].split("p")[0].replace(",","").replace(" ","").strip()
            rent=re.findall("\d+",rent)
            if rent:
                item_loader.add_value("rent", int(rent[0])*4)
        item_loader.add_value("currency", "AUD")
        room_count =response.xpath("//li//i[@class='las la-bed']/following-sibling::span/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)
        bathroom_count =response.xpath("//li//i[@class='las la-bath']/following-sibling::span/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        images = [x for x in response.xpath("//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        parking =response.xpath("//li//i[@class='las la-car']/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        features =response.xpath("//h5[@class='sub-title']/following-sibling::div/ul//li//text()").getall()
        if features:
            for i in features:
                if "garage" in i.lower() or "parking" in i.lower():
                    item_loader.add_value("parking", True)  
                if "terrace" in i.lower() or "garden" in i.lower():
                    item_loader.add_value("terrace", True) 
                if "balcony" in i.lower():
                    item_loader.add_value("balcony", True)
                if "furnished" in i.lower() or "meublé" in i.lower():
                    item_loader.add_value("furnished", True)
                if "lift" in i.lower():
                    item_loader.add_value("elevator", True)
                if "dishwasher" in i.lower():
                    item_loader.add_value("elevator", True)
                    
        landlord_name = response.xpath("//p[contains(@class,'name')]/strong/text()").get()
        item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//p[contains(@class,'phone')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[-1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//p[contains(@class,'email')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[-1].strip()
            item_loader.add_value("landlord_email", landlord_email)


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "townhouse" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None