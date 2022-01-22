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
from datetime import datetime

class MySpider(Spider):
    name = 'northview_london'
    execution_type = 'testing' 
    country = 'united_kingdom'
    locale = 'en'
    custom_settings={"HTTPCACHE_ENABLED":False}
    start_urls = ["https://www.northview.london/Search?listingType=6&statusids=1&obc=Added&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice="]
    
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='fdLink']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Northview_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_id", response.url.split('/')[-1])
        
        prop_type = ""
        f_text = " ".join(response.xpath("//h2[contains(.,'Summary ')]/..//text()").getall())
        property_type = response.xpath("//li[contains(.,'Studio')]/text()").get()
        if property_type:
            prop_type = "studio"
        elif get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//h2[contains(.,'Description')]/..//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
            else:
                if "floor" in f_text.lower():
                    prop_type = "apartment"
        
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        
        externalid=item_loader.get_output_value("external_id")
        if externalid=="42347806":
            item_loader.add_value("property_type","apartment")
            
       
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
                
        rent = "".join(response.xpath("//h2/div[contains(.,'Fees')]//text()").extract())
        if rent:
            if "PW" in rent:
                price = rent.split("PW")[0].split("£")[1].strip().replace(",","")
                item_loader.add_value("rent", int(price)*4)
            else:
                price = rent.split("PCM")[0].split("£")[1].strip().replace(",","")
                item_loader.add_value("rent", price)
            
            item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//div/i[contains(@class,'bed')]/parent::div/text()").extract())
        if room_count:
            if room_count.strip() and room_count.strip() != "0":
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = "".join(response.xpath("//div/i[contains(@class,'reception')]/parent::div/text()").extract())
                if room_count.strip():
                    item_loader.add_value("room_count", room_count.strip())
                elif prop_type == 'studio':
                    item_loader.add_value("room_count", "1")
        
        bathroom_count = "".join(response.xpath("//div/i[contains(@class,'bath')]/parent::div/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
              
        desc = " ".join(response.xpath("//div/h2[contains(.,'Full')]/parent::div//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='detailsRoyalSlider']//a/@href[not(contains(.,'#'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "available now" in desc.lower() or "available immediately" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        energy_label = response.xpath("//li[contains(.,'Energy Rating')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
            
        furnished = response.xpath("//li[contains(.,'Furnished')]/text()").get()
        unfurnished = response.xpath("//li[contains(.,'Unfurnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elif unfurnished:
            item_loader.add_value("furnished", False)
        
        washing_machine = response.xpath("//li[contains(.,'Washing Machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        
        item_loader.add_value("landlord_name", "NORTHVIEW")
        item_loader.add_value("landlord_phone", "0203 8748 888")
        item_loader.add_value("landlord_email", "info@northview.london")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None