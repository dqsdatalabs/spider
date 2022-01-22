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
import dateparser

class MySpider(Spider):
    name = 'futurelet_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.rightmove.co.uk/property-to-rent/find/Future-Let/Old-Harlow.html?locationIdentifier=BRANCH%5E24587&propertyStatus=all&includeLetAgreed=true&_includeLetAgreed=on']  # LEVEL 1
    
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 24)
        seen = False
        for item in response.xpath("//a[@data-test='property-details']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)

        if page == 24 or seen:
            p_url = response.url + f"&index={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+24})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[@class='OD0O7FWw1TjbTD4sdRi1_']/div//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Futurelet_Co_PySpider_united_kingdom")

        rent_string = response.xpath("//div[@class='_1gfnqJ3Vtd1z40MlC0MzXu']/span/text()").get()
        if rent_string:
            item_loader.add_value("rent_string", rent_string.replace(" ", "").replace(',', '').strip().strip('pcm'))
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        script_data = response.xpath("//script[contains(.,'PAGE_MODEL')]/text()").get()
        if script_data:
            data = json.loads(script_data.split("PAGE_MODEL = ")[1].strip())["propertyData"]

            if data and "id" in data:
                item_loader.add_value("external_id", data["id"])
            
            if data and "text" in data and "pageTitle" in data["text"]:
                item_loader.add_value("title", data["text"]["pageTitle"])
            else:
                item_loader.add_value("title", response.meta.get("title"))
            
            if data and "address" in data and "displayAddress" in data["address"]:
                item_loader.add_value("address", data["address"]["displayAddress"])
            else:
                address = response.xpath("//h1[@class='_2uQQ3SV0eMHL1P6t5ZDo2q']/text()").get()
                if address:
                    item_loader.add_value("address", address)
            
            city = " ".join(response.xpath("//h1[@itemprop='streetAddress']/text()").extract())
            if city:
                item_loader.add_value("city", city.split(",")[-1].strip())

            if data and "location" in data:
                if "latitude" in data["location"] and "longitude" in data["location"]:
                    item_loader.add_value("latitude", str(data["location"]["latitude"]))
                    item_loader.add_value("longitude", str(data["location"]["longitude"]))
            
            if data and "bedrooms" in data:
                item_loader.add_value("room_count", str(data["bedrooms"]))
            else:
                room_count = response.xpath("//div[.='BEDROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if room_count:
                    item_loader.add_value("room_count", room_count.replace("x", "").strip())
            
            if data and "bathrooms" in data:
                item_loader.add_value("bathroom_count", str(data["bathrooms"]))
            else:
                bathroom_count = response.xpath("//div[.='BATHROOMS']/..//div[@class='_1fcftXUEbWfJOJzIUeIHKt']/text()").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.replace("x", "").strip())
            
            if data and "images" in data and len(data["images"]) > 0:
                images = [x["url"] for x in data["images"]]
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
            
            if data and "floorplans" in data and len(data["floorplans"]) > 0:
                floor_images = [x["url"] for x in data["floorplans"]]
                item_loader.add_value("floor_plan_images", floor_images)
            
            if data and "lettings" in data:
                if "letAvailableDate" in data["lettings"]:
                    available_date = data["lettings"]["letAvailableDate"]
                else:
                    available_date = response.xpath("//span[contains(.,'Let available date')]/following-sibling::*/text()").get()
                if available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

                terrace = "".join(response.xpath("//div[@class='_2Pr4092dZUG6t1_MyGPRoL']/div[.='Terraced']/text()").extract())
                if terrace:
                    item_loader.add_value("terrace", True)

                if "deposit" in data["lettings"]:
                    item_loader.add_value("deposit", str(data["lettings"]["deposit"]))
                
                if "furnishType" in data["lettings"] and data["lettings"]["furnishType"] == "Furnished":
                    item_loader.add_value("furnished", True)
                elif "furnishType" in data["lettings"]:
                    item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_name", "Future Let")
        item_loader.add_value("landlord_phone", "01279 949063")
        
        status = response.xpath("//span[contains(@class,'berry')]/text()").get()
        if not status:
            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None