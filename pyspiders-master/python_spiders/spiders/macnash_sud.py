# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = "macnash_sud"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    external_source='Macnashsud_PySpider_belgium_en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.macnash.com/ui/propertyitems.aspx?Page=0&Sort=0&ZPTID=1&TT=1&Agency=1749", "property_type": "apartment", "type":"1"},
            {"url": "https://www.macnash.com/ui/propertyitems.aspx?Page=0&Sort=0&ZPTID=3&TT=1&Agency=1749", "property_type": "house", "type":"3"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            'type':url.get("type")})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        
        seen = False
        for item in response.xpath("//div[@id='container']/div"):
            address = item.xpath(".//h3/a/text()").extract_first()
            follow_url = response.urljoin(
                item.xpath(".//div[@class='image']/a/@href").extract_first()
            )
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get("property_type"), "address": address},
            )
            seen = True

        if page == 1 or seen:
            url = f"https://www.macnash.com/ui/propertyitems.aspx?Page={page}&Sort=0&ZPTID={response.meta.get('type')}&TT=1&Agency=1749"
            yield Request(url, 
                            callback=self.parse, 
                            meta={"page": page + 1, 
                                'type':response.meta.get("type"),
                                "property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", "Macnashsud_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        rented = response.xpath("//div[@id='contentHolder_sticker']/text()").get()
        if rented and "rented" in rented.lower():
            return
        rent = response.xpath(
            "//div[@class='contact-content']/h3[@class='lead']//text()"
        ).get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(",", ""))
            item_loader.add_value("currency", "EUR")
        item_loader.add_value("external_link", response.url)

        ref = response.xpath(
            "//div[@class='contact-content']/p[@class='reference']/text()"
        ).get()
        if ref:
            item_loader.add_value("external_id", ref.split(" ")[1])
            
        square = response.xpath("//tr[./td[.='Surface']]/td[2]//text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0].strip())
        elif not square:
            square = response.xpath("//tr[./td[contains(.,'surface')]]/td[2]//text()").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0].strip())
        desc = response.xpath("//div[@class='content']/p//text()").get()
        if desc:
            item_loader.add_value("description", desc.strip())

        room = response.xpath("//tr[./td[.='Number of bedrooms']]/td[2]//text()[.!='0']").get()
        if room:
            item_loader.add_value("room_count", room)
        elif "studio" in item_loader.get_collected_values("property_type") and "0" in room:
            item_loader.add_value("room_count", "1")
        elif desc and "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        item_loader.add_xpath("bathroom_count", "//tr[./td[.='Number of bathrooms']]/td[2]//text()[.!='0']")
        item_loader.add_xpath("utilities", "//tr[./td[.='Charges (€)']]/td[2]")
        item_loader.add_xpath("floor", "//tr[./td[.='Property floor']]/td[2]/text()")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='lightgallery']/a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        furnished = response.xpath(
            "//tr[td[.='Well furnished']]/td[2]/text()"
        ).get()
        if furnished:
            if "yes" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "no" in furnished.lower():
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//tr[td[.='Elevator']]/td[2]/text()").get()
        if elevator:
            if "yes" in elevator.lower():
                item_loader.add_value("elevator", True)
            elif "no" in elevator.lower():
                item_loader.add_value("elevator", False)

        terrace = response.xpath(
            "//tr[@id='contentHolder_terraceZone']/td[2]/text() | //tr[td[contains(.,'Terrace')]]/td[2]/text()"
        ).get()
        if terrace:
            if "no" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        parking = response.xpath("//tr[td[contains(.,'Parking') or contains(.,'parking')]]/td[2]/text()").get()
        if parking:            
            if "no" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        pet = response.xpath("//tr[td[contains(.,'Pets allowed')]]/td[2]/text()").get()
        if pet:            
            if "no" in pet.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        address = response.xpath("//h1/text()").extract_first()
        if address:
            address = address.split(" -")[0].replace("  "," ").strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        item_loader.add_xpath(
            "latitude", "//tr[./td[.='Xy coordinates']][1]/td[2]/text()"
        )
        item_loader.add_xpath(
            "longitude", "//tr[./td[.='Xy coordinates']][2]/td[2]/text()"
        )

        energy = response.xpath(
            "//tr[./td[.='Energy certificate']][last()]/td[2]/text()"
        ).extract_first()
        if energy:
            if not energy.isdigit():
                item_loader.add_value("energy_label", energy)

        item_loader.add_value("landlord_phone", "+32 (0)2 347 11 47")
        item_loader.add_value("landlord_name", "Macnash")
        yield item_loader.load_item()


def split_address(address, get):
    temp = address.split(" ")[-2]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city
