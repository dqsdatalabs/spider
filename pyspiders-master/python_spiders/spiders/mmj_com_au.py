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
    name = 'mmj_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Mmj_Com_PySpider_australia'

    def start_requests(self):
        start_url = "https://www.mmj.com.au/properties/getAllProperties"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data:
            is_residential = True if item["Type"] == "residential" else False
            is_available = False if item["Status"] == "leased" else True
            is_rent = True if item["Purpose"] == "rent" else False
            if is_residential and is_available and is_rent:
                follow_url = "https://www.mmj.com.au/properties/" + item["URLSegment"]
                property_type = item["Category"]
                if property_type:
                    if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = "".join(response.xpath("//div[@class='title']/span[@classname='size']/text()").extract())
        if "deposit" in rent.lower():
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source) 

        external_id = response.xpath("//div[contains(@class,'side-title')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[@class='property-top']//div[@class='title']//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        square_meters = response.xpath("//div[@class='ir-it']/div[div[.='Home Size']]/div[2]/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("s")[0]) 
        rent = "".join(response.xpath("//div[@class='title']/span[@classname='size']/text()").getall())
        if rent:
            price = rent.split("$")[1].strip().split(" ")[0].replace("p/w","").replace(",","").strip()
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "USD")

        item_loader.add_xpath("address", "//div[@class='title']/p/text()")
        address = response.xpath("//div[@class='title']/p/text()").extract_first()
        if address:
           item_loader.add_value("zipcode",address.split(",")[-1].strip()) 
           item_loader.add_value("city",address.split(",")[-2].strip()) 

        available_date=response.xpath("//div[@classname='price']/span[2]/text()").get()
        if available_date:
            date2 =  available_date.split("-")[0].split(" ")[1]
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        room = "".join(response.xpath("//div[@class='ir-it']/div[div[.='Bedrooms']]/div[2]/text()").extract())
        if room:
           item_loader.add_value("room_count",room.strip()) 

        bathroom_count = "".join(response.xpath("//div[@class='ir-it']/div[div[.='Bathrooms']]/div[2]/text()").extract())
        if bathroom_count:
           item_loader.add_value("bathroom_count",room.strip())

        description = " ".join(response.xpath("//div[@class='description']/div/p/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        item_loader.add_xpath("latitude", "//div[@class='responsive-map']/@lat")
        item_loader.add_xpath("longitude", "//div[@class='responsive-map']/@lng")

        images = [x for x in response.xpath("//section[@class='property-gallery']/div/img/@data-url").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[@class='details']/span[@class='parking']/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@class='details']/span[@class='parking']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "Tyler Filippi")
        item_loader.add_value("landlord_phone", "0410 268 699")
        item_loader.add_value("landlord_email", "rentals@mmj.com.au")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None