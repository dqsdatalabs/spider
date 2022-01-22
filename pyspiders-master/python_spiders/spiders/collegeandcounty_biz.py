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
class MySpider(Spider):
    name = 'collegeandcounty_biz'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.collegeandcounty.biz/search.ljson?channel=lettings&fragment="]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["properties"]:
            if item["status"] == "To let":
                features = " ".join(item["features"])
                desc = remove_tags(item["html_description"])
                follow_url = "https://www.collegeandcounty.biz" + item["property_url"]
                yield Request(follow_url, callback=self.populate_item, meta={"features":features, "desc":desc})
        
        if data["pagination"]["has_next_page"]:
            page = data["pagination"]["current_page"] + 1
            p_url = f"https://www.collegeandcounty.biz/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(p_url, callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        features = response.meta.get("features")
        desc = response.meta.get("desc")

        if get_p_type_string(features):
            item_loader.add_value("property_type", get_p_type_string(features))
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        item_loader.add_value("external_source", "Collegeandcounty_PySpider_united_kingdom")
         
        item_loader.add_xpath("title", "//div/h2/text()")
        
        rent = response.xpath("substring-after(//div/p[contains(@class,'price')]//text(),'(')").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent) 
        
        room_count = response.xpath("//div[span[contains(@class,'fa-bed')]]//text()[normalize-space()]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//div[span[contains(@class,'fa-bath')]]//text()[normalize-space()]").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = response.xpath("//div/h2/text()").extract_first()       
        if address:
            item_loader.add_value("address", address)   
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            if zipcode.isalpha():
                zipcode = ""
                city = address.split(",")[-1].strip()
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
            if city:
                item_loader.add_value("city",city)
       
        desc = " ".join(response.xpath("//div[@id='propDescription']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
     
        lat = response.xpath("//div[@id='propertyStreetview']/@data-lat").get()
        lng = response.xpath("//div[@id='propertyStreetview']/@data-lng").get()
        if lng and lng:
            item_loader.add_value("latitude",lat.strip())
            item_loader.add_value("longitude",lng.strip())
       
        images = []
        img = response.xpath("//div[@id='propertyShowCarousel']//div/@data-bgset").extract()
        if img:
            for j in img:            
                image = j.split(",")[0]
                images.append(response.urljoin(image.replace("\n                ","")))
            if images:
                item_loader.add_value("images", images)

        floor_plan_images  = [response.urljoin(x) for x in response.xpath("//div[@class='floorplan']/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images )    

        item_loader.add_value("landlord_phone", "01865 722722")
        item_loader.add_value("landlord_name", "College and County")          

        # yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
