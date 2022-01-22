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
    name = 'churchillsestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("https://www.churchillsestateagents.co.uk/search?page=1&national=false&p_department=RL&p_division=&location=&auto-lat=&auto-lng=&keywords=&minimumPrice=&minimumRent=0&maximumPrice=&maximumRent=10000&rentFrequency=&minimumBedrooms=0&maximumBedrooms=&searchRadius=&recentlyAdded=&propertyIDs=&propertyType=&rentType=&orderBy=price%2Bdesc&networkID=&clientID=&officeID=&availability=&propertyAge=&prestigeProperties=&includeDisplayAddress=No&videoettesOnly=0&360TourOnly=0&virtualTourOnly=0&country=&addressNumber=&equestrian=0&tag=&golfGroup=&coordinates=&priceAltered=&sfonly=0&openHouse=0&student=&limit=20", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='search-results']/div/div/div[not(contains(@class,'right'))]"):
            follow_url = response.urljoin(item.xpath("./div/a[1]/@href").get())
            property_type = item.xpath(".//p[@class='bedrooms']/text()").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//ul[@class='pagination']//a[contains(.,'›')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Churchillsestateagents_Co_PySpider_united_kingdom")
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
            
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split("£")[1].split("p")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        room_count = response.xpath("//span[@class='type']/text()").get()
        if room_count and "Bedroom" in room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        external_id = response.xpath("//small/span/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        description = " ".join(response.xpath("//div[@class='full_description_large']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        if "sq ft" in description:
            square_meters = description.split("sq ft")[0].strip().split(" ")[-1]
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)

        lat_long = response.xpath("//section//div[@class='google-map-embed']/@data-location").extract_first()
        if lat_long:
            item_loader.add_value("latitude", lat_long.split(",")[0])
            item_loader.add_value("longitude", lat_long.split(",")[1])
            
        images = response.xpath("//script[contains(.,'property-details-slider')]/text()").get()
        if images:
            images = images.split("null,")[1].split(", [")[0].strip()
            image_json = json.loads(images)
            for item in image_json:
                item_loader.add_value("images", item["image"])
        
        item_loader.add_value("landlord_name", "Churchills Estate Agents")
        item_loader.add_value("landlord_phone", "01904 646611")
        item_loader.add_value("landlord_email", "info@churchillsyork.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None