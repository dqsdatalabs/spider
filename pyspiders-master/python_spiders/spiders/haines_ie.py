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
    name = 'haines_ie'
    execution_type='testing'
    country='ireland'
    locale='en'

    def start_requests(self):
        start_url = "https://haines.ie/property-listings/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-card property-card') and contains(@class,'status-to-let')]"):
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            property_type = item.xpath(".//div[@class='property-card--subtype']/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Haines_PySpider_ireland")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")


        address = " ".join(response.xpath("//div/h1/text()").getall())
        if address:
            address = re.sub("\s{2,}", " ", address)
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].replace("Co.","").strip())

        rent =  response.xpath("//div[span[.='Price']]/em/text()").extract_first()
        if rent:
            price = rent.split("/")[0].strip().replace(",","").strip()
            item_loader.add_value("rent_string", price)

        room = "".join(response.xpath("//div[span[.='BEDROOMS']]/em/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        
        label = response.xpath("//div[@class='ber-value']/span/text()").extract_first()
        if label:
            item_loader.add_value("energy_label", label.strip())
        script_coord = response.xpath("//script[@type='application/ld+json']/text()").extract_first()
        if script_coord:
            item = json.loads(script_coord)
            latitude = item["geo"]["latitude"]
            longitude = item["geo"]["longitude"]
            item_loader.add_value("latitude",str(latitude))
            item_loader.add_value("longitude", str(longitude))

        bathroom_count = "".join(response.xpath("//div[span[.='BATHROOMS']]/em/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        desc =  " ".join(response.xpath("//div[@id='property-description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//div[@class='carousel-cell']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)  

        item_loader.add_value("landlord_name", "Hanies Lettings Team")
        item_loader.add_value("landlord_phone", "012845677")
        item_loader.add_value("landlord_email", "info@haines.ie")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None