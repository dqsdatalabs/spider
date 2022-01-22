# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser

class MySpider(Spider):
    name = "living_stone_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
   
    def start_requests(self):
        start_urls = [
            {"url": "https://www.living-stone.be/nl/te-huur/appartementen", "property_type": "apartment"},
            {"url": "https://www.living-stone.be/nl/te-huur/woningen", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type'),"base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='property-contents ']/@href").extract():
            follow_url = response.urljoin(item)
            if "searchTerm=" not in follow_url:
                yield Request(follow_url, 
                                callback=self.populate_item, 
                                meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            
            base_url = response.meta.get("base_url")
            url = base_url+f"/pagina-{page}"
            yield Request(url, callback=self.parse, meta={"page": page + 1, 'property_type': response.meta.get('property_type'),"base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        item_loader.add_value("external_source", "Livingstone_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@id='property__detail']//h1/text()")
        address = response.xpath("//div[@class='address']/text()").extract_first()
        item_loader.add_value("address", address.rstrip(" -").strip())
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city")) 
        # item_loader.add_xpath("room_count", "//div[dt[.='slaapkamers']]/dd/text()")
        item_loader.add_xpath("bathroom_count", "//section[@id='property__title']//li[@class='bathrooms']/text()")
        room_count=response.xpath("//li[@class='rooms']/text()").get()
        if room_count:
            room=room_count.strip()
            item_loader.add_value("room_count",room)
        
        external_id = response.url
        item_loader.add_value("external_id", external_id.split("/")[-1])

        square = response.xpath("//section[@id='property__title']//li[@class='area']/text()").extract_first()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
   
        desc = " ".join(response.xpath("//div[@class='property__description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        price = response.xpath("//div[@class='price']/text()").extract_first()
        if price:
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        utilities = response.xpath("//div[dt[.='kosten']]/dd/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)

        terrace = response.xpath("//div[dt[.='terras']]/dd/text()").get()
        if terrace:
            if "nee" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        parking = response.xpath("//div[dt[contains(.,'garage')]]/dd/text()").get()
        if parking:
            if "nee" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//div[dt[.='gemeubeld']]/dd/text()").get()
        if furnished:
            if "nee" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//div[dt[.='lift']]/dd/text()").get()
        if elevator:
            if "nee" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
            
        available_date = response.xpath("//div[dt[.='beschikbaarheid']]/dd/text()").get()
        if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [
            response.urljoin(x)
            for x in response.xpath("//ul[contains(@id,'photo__small')]/li//a/@href").extract()
        ]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone","016 60 76 09")
        item_loader.add_value("landlord_name", "Living Stone")

        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1].split("-")[0].strip()

        if get == "zip":
            return zip_code
        else:
            return city
