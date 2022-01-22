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
    name = '051immobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "051immobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://051immobiliare.it/affitti/bologna-e-provincia/appartamento.html",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//dt[@class='title']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//h2[contains(.,'Rif')]/strong/text()").get()
        item_loader.add_value("external_id", external_id)
        
        address = "".join(response.xpath("//address//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        
        rent = "".join(response.xpath("//tr[th[contains(.,'Canone')]]/td/text()").getall())
        if rent:
            rent = rent.replace(".","").split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        utilities = "".join(response.xpath("//tr[th[contains(.,'Spese')]]/td/text()").getall())
        if utilities:
            utilities = utilities.replace(".","").split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        square_meters = "".join(response.xpath("//tr[th[contains(.,'Superfice')]]/td/text()").getall())
        if square_meters:
            square_meters = square_meters.replace(".","").split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = "".join(response.xpath("//tr[th[contains(.,'Camere')]]/td/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = "".join(response.xpath("//tr[th[contains(.,'Bagni')]]/td/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        description = response.xpath("//div[contains(@class,'description')]//p//text()").get()
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//img[contains(@alt,'Planal')]/@src").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//div[@class='jea_dpe']//@src").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude')[1].split(';')[0].replace("=","").strip()
            longitude = latitude_longitude.split('longitude')[1].split(';')[0].replace("=","").strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        elevator = response.xpath("//li[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//li[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrazza')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='nivoSlider']//@src").getall()]
        item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "051immobiliare")
        item_loader.add_value("landlord_phone", "03098661204")

        city = response.xpath("//address/text()[2]").get()
        if city:
            item_loader.add_value("city",city.strip())
        
        yield item_loader.load_item()