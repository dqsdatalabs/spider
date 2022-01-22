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
from word2number import w2n

class MySpider(Spider):
    name = 'keijstefels_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.keij-stefels.nl/nl/realtime-listings/consumer"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["status"] == "Beschikbaar" and item["isRentals"]:
                follow_url = response.urljoin(item["url"])
                property_type = item["mainType"]
                if property_type == "apartment":
                    property_type = 'apartment'
                elif property_type == "house":
                    property_type = 'house'
                else:
                    property_type = 'pass'
                if property_type != 'pass':
                    yield Request(follow_url, callback=self.populate_item,meta={"item": item,"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
      
        item_loader.add_value("external_source", "Keijstefels_PySpider_" + self.country + "_" + self.locale)
        item = response.meta.get("item")
        item_loader.add_value("property_type", response.meta.get("property_type"))

        lat = item["lat"]
        lng = item["lng"]
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_id", str(item["_id"]))
        item_loader.add_value("room_count", str(item["bedrooms"]))
        item_loader.add_value("square_meters", str(item["livingSurface"]))
        item_loader.add_value("zipcode", item["zipcode"])
        item_loader.add_value("city", item["city"])
        item_loader.add_value("address", "{}, {}".format(item["address"], item["city"]))
        item_loader.add_value("rent", item["rentalsPrice"])
        item_loader.add_value("currency", "EUR")
        if item["isFurnished"]:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)
        if item["balcony"]:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("bathroom_count", "//dt[.='Aantal badkamers']/following-sibling::dd[1]/text()")
        item_loader.add_xpath("floor", "//dt[.='Aantal verdiepingen']/following-sibling::dd[1]/text()")

        desc = "".join(response.xpath("//div[@id='tab-omschrijving']/div//text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())


        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='swiper-wrapper']//div/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_phone", "020-5775333")
        item_loader.add_value("landlord_name", "Keij & Stefels")
        item_loader.add_value("landlord_email", "info@keij-stefels.nl")

        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng))

        yield item_loader.load_item()