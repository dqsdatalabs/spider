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
    name = 'fnrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://fnrealty.com.au/lease"]

    # 1. FOLLOWING
    def parse(self, response):

        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "x-requested-with": "XMLHttpRequest",
        }

        token = response.xpath("//meta[@name='csrf-token']/@content").get()
        url = f"https://fnrealty.com.au/json/data/listings/?authenticityToken={token}&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=BlockOfUnits&listing_property_type=DuplexSemi-detached&listing_property_type=House&listing_property_type=Townhouse&listing_property_type=Unit&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=1200"

        yield Request(
            url,
            callback=self.jump,
            headers=headers,
            meta={
                "property_type":"house"
            }
        )
        
        url = f"https://fnrealty.com.au/json/data/listings/?authenticityToken={token}&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=Apartment&listing_property_type=Flat&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=1200"

        yield Request(
            url,
            callback=self.jump,
            headers=headers,
            meta={
                "property_type":"apartment"
            }
        )
    def jump(self, response):
        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            item_loader = ListingLoader(response=response)
            follow_url = f"https://fnrealty.com.au{item['listing_url']}"
            item_loader.add_value("external_source", "Fnrealty_Com_PySpider_australia")
            item_loader.add_value("external_link", follow_url)
            item_loader.add_value("property_type", response.meta["property_type"])
            item_loader.add_value("title", str(item["listing_heading"]))
            item_loader.add_value("images", item["listing_gallery"])
            item_loader.add_value("latitude", str(item["latitude"]))
            item_loader.add_value("longitude", str(item["longitude"]))
            item_loader.add_value("address", item["listing_full_address"])
            item_loader.add_value("city", item["suburb_name"])
            item_loader.add_value("zipcode", str(item["listing_suburb_postcode"]))
            item_loader.add_value("external_id", str(item["id"]))
            item_loader.add_value("room_count", str(item["listing_bedrooms"]))

            rent = item["listing_display_price"]
            if rent:
                if "week" in rent.lower():
                    rent = str(float(rent.split(" ")[0].replace("$", "").split(".")[0]) * 4).split(".")[0].strip() + "$"
                elif "month" in rent.lower():
                    rent = rent.split(" ")[0].split(".")[0].strip()
            
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={
                    "item":item_loader,
                    "rent": rent
                }
            )
        
    


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = response.meta["item"]
        
        rent = response.meta["rent"]
        if rent:
            if "." in rent:
                item_loader.add_value("rent_string", rent.split(".")[0])
            else:
                item_loader.add_value("rent_string", rent)

        description = response.xpath("//meta[contains(@property,'description')]/@content").get()
        item_loader.add_value("description", description)
        
        deposit = response.xpath("//div[@class='text-feat']//p[contains(.,'Bond')]/text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)
        
        bathroom_count = response.xpath("//p[contains(@class,'in__bedbathcar')]//i[contains(@class,'icon-bath')]//parent::span//em//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//p[contains(@class,'in__bedbathcar')]//i[contains(@class,'icon-car')]//parent::span//em//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//p[contains(.,'Available')]//text()").get()
        if available_date:
            available_date = available_date.split("-")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        balcony = response.xpath("//p[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        square_meters = response.xpath("//p[contains(.,'Land Area')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("-")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
            
        floor_plan_images = response.xpath("//div[contains(@id,'floorplans')]//@src").get()
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "FIRST NATIONAL REAL ESTATE")
        item_loader.add_value("landlord_phone", "07 4942 4118")
        item_loader.add_value("landlord_email", "admin@mackayrealty.com.au")
        
        yield item_loader.load_item()
