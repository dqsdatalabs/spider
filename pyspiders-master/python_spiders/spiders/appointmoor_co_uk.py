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
    name = 'appointmoor_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Apartment&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Flat&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Maisonette&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Bungalow&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=House&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Studio&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "https://www.appointmoor.co.uk/view-properties/rent.php?database_name=Lettings&search_location=&search_property_type=Room&search_minimum_price=0&search_maximum_price=999999999&search_bedrooms=&search_radius=&search_include_stc=off&search_include_stc=on&search_include_letby=off&search=",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'View details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[@rel='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Appointmoor_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
            
        address = response.xpath("//aside/h2//text()").get()
        city = response.xpath("//aside/address//text()").get()
        if address or city:
            item_loader.add_value("address", f"{address} {city}")
            item_loader.add_value("city", city.split(",")[-1].strip())
        
        rent = response.xpath("//strong[@class='section__price']/text()").get()
        if rent:
            price = rent.split(" ")[0].replace(",","").replace("£","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//dt[contains(.,'Bedroom')]/following-sibling::dd//text()").getall())
        if room_count:
            room_count = room_count.split("Bed")[0].strip()
            item_loader.add_value("room_count", room_count)
        
        furnished = response.xpath("//ul[@class='property-features']/li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        floor = response.xpath("//ul[@class='property-features']/li[contains(.,'Floor')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        parking = response.xpath("//ul[@class='property-features']/li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//img/@src[contains(.,'EE_')]").get()
        if energy_label:
            item_loader.add_value("energy_label", str(int(energy_label.split("_")[-2])))
        
        description = " ".join(response.xpath("//div[@class='property-description']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        images = [x for x in response.xpath("//div[@class='property-slider']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplans']//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        import dateparser
        available_date = response.xpath("//ul[@class='property-features']/li[contains(.,'Available')]//text()").get()
        if available_date:
            if "now" not in available_date.lower():
                available_date = available_date.split("Available")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "Appointmoor")
        item_loader.add_value("landlord_phone", "01702 719 966")
        item_loader.add_value("landlord_email", "info@appointmoor.co.uk")

        yield item_loader.load_item()