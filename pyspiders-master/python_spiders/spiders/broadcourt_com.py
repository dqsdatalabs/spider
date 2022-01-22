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
    name = 'broadcourt_com'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    start_urls = ["https://www.broadcourt.com/search.ljson?channel=lettings&fragment=status-available"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        for item in data["properties"]:
            follow_url = response.urljoin(item["property_url"])
            yield Request(follow_url, callback=self.populate_item ,meta={"item":item})
        
        if data["pagination"]["has_next_page"]:
            p_url = f"https://www.broadcourt.com/search.ljson?channel=lettings&fragment=status-available/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@id='description-content']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        item_loader.add_value("external_source", "Broadcourt_PySpider_united_kingdom")
        item = response.meta.get('item')
        if item["bathrooms"].strip() != "0":
            item_loader.add_value("bathroom_count",item["bathrooms"])
        if item["bedrooms"].strip() != "0":
            item_loader.add_value("room_count",item["bedrooms"])
        item_loader.add_value("latitude",str(item["lat"]))
        item_loader.add_value("longitude",str(item["lng"]))
           
        address = item["display_address"]
        if address:
            item_loader.add_value("address",address)
            zipcode = address.split(",")[-1].strip()
            if  len(zipcode.split(" "))<3:
                if not zipcode.replace(" ","").isalpha() and ".." not in zipcode:
                    item_loader.add_value("zipcode",zipcode.split("(")[0])
        item_loader.add_value("external_id",str(item["property_id"]))
    
        item_loader.add_xpath("title", "//div[@id='property-show']/h3/text()")        

        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery-slideshow-container']/div//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan-slideshow-container']/div//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        desc = " ".join(response.xpath("//div[@id='description-content']/div/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        rent = response.xpath("//div[@class='price']//text()").extract_first()
        if rent:
            if "pw" in rent:
                rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)*4))) 
            else:
                rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent) 
        item_loader.add_value("currency", "GBP") 
 
        item_loader.add_value("landlord_name", "Broad Court")
        item_loader.add_value("landlord_phone", "0121 414 1617")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None