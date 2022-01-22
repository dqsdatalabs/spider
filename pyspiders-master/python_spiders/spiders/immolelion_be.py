# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from python_spiders.loaders import ListingLoader
from w3lib.html import remove_tags
import json
import re


class MySpider(Spider):
    name = "immolelion_be"
    start_urls = ["https://immo-lelion.be/sitemap.xml"]
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    
    def parse(self, response):
        for item in response.xpath("//*"):
            url = item.xpath("./text()").extract_first()
            if "/location/" in url:
                yield Request(url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Immolelion_be_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h3/text()").extract_first()
        item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        item_loader.add_value("external_link", response.url)

        rent = "".join(response.xpath("//li[contains(.,'Prix')]/text()").extract())
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        description = "".join(response.xpath("//div[@id='text']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            if "salles de bain" in description:
                bathroom = description.split("salles de bain")[0].strip().split(" ")[-1]
                if bathroom.isdigit(): item_loader.add_value("bathroom_count", bathroom)

        if 'duplex' in response.url or \
            'villa' in response.url or \
            'residence' in response.url or \
            'maison' in response.url:
            property_type = 'house'
        elif "appartement" in response.url:
            property_type = 'apartment'
        else:
            property_type = None

        if property_type:
            item_loader.add_value(
                "property_type", property_type
            )

            ref = "".join(response.xpath("//li[contains(.,'Reference')]/text()").extract())
            item_loader.add_value("external_id", ref.strip())

            s_meter = "".join(
                response.xpath("//li[contains(.,'Superficie')]/text()").extract()
            )
            item_loader.add_value("square_meters", s_meter.strip())

            energy_label = "".join(
                response.xpath("//li[contains(.,'PEB:')]/text()").extract()
            )
            item_loader.add_value("energy_label", energy_label.strip())

            latlong = response.xpath("//div[@class='maps']/iframe/@src").extract_first()
            if latlong:
                item_loader.add_value(
                    "latitude", latlong.split("ll=")[1].split("&sp")[0].split(",")[0]
                )
                item_loader.add_value(
                    "longitude", latlong.split("ll=")[1].split("&sp")[0].split(",")[1]
                )

            r_count = "".join(
                response.xpath("//li[contains(.,'Chambre')]/text()").extract()
            )
            if r_count:
                item_loader.add_value("room_count", r_count.strip())

            parking = response.xpath("//li/label[contains(.,'Garage')]/following-sibling::text()").get()
            if parking:
                if int(parking.strip()) > 0:
                    item_loader.add_value("parking", True)

            terrace = response.xpath("//li[contains(.,'Terrasse')]").get()
            if terrace:
                item_loader.add_value("terrace", True)
            address = response.xpath("//ul[@class='breadcrumb']/li[3]/a/text()").get()
            if address:
                item_loader.add_value("address", address)
                if " - " in address:
                    item_loader.add_value("city", address.split(" - ")[0].strip())
                else:
                    item_loader.add_value("city", address)
            else:
                address = response.xpath("//h3/text()").get()
                if address:
                    address = address.split("-")[1].strip()
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address)
            
            images = [
                response.urljoin(x)
                for x in response.xpath(
                    "//div[@class='carousel-inner']//img/@src"
                ).extract()
            ]
            if images:
                item_loader.add_value("images", list(set(images)))
            phone = response.xpath('//b[@class="phone"]/a/@href').get()
            if phone:
                item_loader.add_value("landlord_phone", phone.replace("tel:", ""))
            item_loader.add_value("landlord_name", "Immobilière Le Lion")
            yield item_loader.load_item()
