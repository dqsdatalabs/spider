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
    name = 'lakinandco_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=76&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=75&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=147&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=156&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=150&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=155&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=80&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=142&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=149&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=148&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=141&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=157&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=79&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=140&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=78&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=144&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=154&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=139&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://lakinandco.com/properties/?department=residential-lettings&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&property_type=146&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&view=&pgp=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[contains(@class,'type-property')]"):  
            follow_url = response.urljoin(item.xpath(".//h3/a/@href").get()) 
            let_agreed = item.xpath(".//div[@class='flag' and contains(.,'Let Agreed')]").get()
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lakinandco_PySpider_united_kingdom")

        external_id = response.xpath("//li[contains(@class,'ref')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.replace(", W3","").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'summary sfull')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if property_type == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//li[contains(@class,'bed')]//text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip()
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(@class,'available') and not(contains(.,'Now'))]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        else:
            available_date = "".join(response.xpath("//div[contains(@class,'features')]//li[contains(.,'Available') and not(contains(.,'Now'))]//text()").getall())
            if available_date:
                available_date = available_date.split("Available")[1].strip()
                if not ("immediately" in available_date.lower() or "now" in available_date.lower()):
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'pakring') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(@class,'furnished')]//text()").get()
        if furnished:
            furnished = furnished.split(":")[1].strip()
            if not "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Furnished')]//text()").get()
            if furnished:
                item_loader.add_value("furnished", True)

        floor = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip()
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@class,'features')]//li[contains(.,'EPC')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "LAKIN & CO")
        item_loader.add_value("landlord_phone", "01895 544 555")
        item_loader.add_value("landlord_email", "enquiries@lakinandco.com")
        
        yield item_loader.load_item()