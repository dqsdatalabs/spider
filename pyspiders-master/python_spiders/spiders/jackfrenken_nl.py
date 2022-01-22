# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'jackfrenken_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.jackfrenken.nl/aanbod/huurwoningen"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='content-wrapper']/div[@class='item-wrapper']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Jackfrenken_PySpider_" + self.country + "_" + self.locale)
        
        title = " ".join(response.xpath("//h1/text()").extract())
        item_loader.add_value("title", title.replace("\t", "").replace("\n",""))

        location=response.xpath("//script[contains(.,'address')]/text()").get()
        if location:
            address=location.split("address('")[1].split("'")[0].replace("+"," ")
            city=location.split("address('")[1].strip().split("+")[-1].split("'")[0]
            zipcode=location.split("address('")[1].split(",")[1].split("+")[1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[@class='content-wrapper' and @data-tab='1']/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "badkamer" in desc.lower():
            bathroom=desc.split("badkamer")[0].strip().split(" ")[-1]
            if "twee" in bathroom:
                item_loader.add_value("bathroom_count","2")
        
        property_type = response.xpath("//tr[./th[contains(.,'Soort')]]/th/text()").get()
        if property_type:
            property_type = property_type.split("Soort")[1].strip()
            if "woonhuis" in property_type:
                item_loader.add_value("property_type", "house")
            elif "appartement" in property_type:
                item_loader.add_value("property_type", "apartment")
        
        square_meters = response.xpath("//tr[./th[contains(.,'Woonoppervlakte')]]/td/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
        item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("//tr[./th[contains(.,'Slaapkamers')]]/td/text()").get()
        item_loader.add_value("room_count", room_count)
        

        images = [response.urljoin(x) for x in response.xpath("//div[@id='royal-slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        

        price = response.xpath("//p[@class='title-price']/text()").get()
        if price:
            price = price.split(",")[0].strip("€").strip()

        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        

        floor = response.xpath("//tr[./th[contains(.,'Verdiepingen')]]/td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        item_loader.add_value("landlord_name","JACK FRENKEN")
        item_loader.add_value("landlord_phone","0475 335225")
        item_loader.add_value("landlord_email","info@jackfrenken.nl")


        yield item_loader.load_item()