# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'nienaber_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.nienaber.nl/wonen/zoeken/heel-nederland/huur/appartement/", "property_type": "apartment"},
            {"url": "https://www.nienaber.nl/wonen/zoeken/heel-nederland/huur/woonhuis/", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='houses__holder']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//span[contains(@class,'status')]//text()[contains(.,'Verhuurd')]").get()
        if status:
            return

        item_loader.add_value("external_source", "Nienaber_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath("normalize-space(//div[@class='detail__head-title']/h3/text())").extract_first()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@class='descriptions']/article//text()[not(parent::h4)]").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        desc = desc.replace("\n","").replace("\r","").replace("\t","").replace("\xa0","").replace("\u2013",",").strip()
        if desc:
            item_loader.add_value("description", desc )
        
        if "terras" in desc.lower():
            item_loader.add_value("terrace", True)
        
        price = response.xpath("normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Huurprijs']]/span/text())").get()
        if price:
            price = price.split("€")[1].split(",")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        
        square = response.xpath(
            "normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Woonoppervlakte']]/span/text())"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )
        room_count = response.xpath(
            "normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Aantal kamers']]/span/text())"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])

        address = response.xpath("normalize-space(//div[@class='detail__head-title']/p/text())").get()
        
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))
            
        available_date = response.xpath(
            "//div[@class='characteristics']/article/ul/li[./strong[.='Aanvaarding']]/span/text()[not(contains(.,'In overleg')) and not(contains(.,'Per datum '))]").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "beschikbaar per" in desc.lower():
            available_d = desc.lower().split("beschikbaar per")[1].split(",")[0].strip().split(" ")
            if "direct" not in str(available_d):
                available_date = available_d[0]+" "+available_d[1]+" "+available_d[2]
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%m-%d-%Y"]
                )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if "waarborgsom" in desc.lower():
            deposit = desc.lower().split("waarborgsom")[1].split("maanden")[0].replace(":","").strip()
            if deposit.isdigit():
                deposit = str(int(price.replace(".",""))*int(deposit))
                item_loader.add_value("deposit", deposit)
            
        floor = response.xpath("//div[@class='characteristics']/article/ul/li[./strong[.='Aantal woonlagen']]/span/text()").get()
        if floor:
            item_loader.add_value(
                "floor", floor.split(" ")[0]
            )

        washing = response.xpath(
            "//div[@class='characteristics']/article/ul/li[./strong[.='Badkamervoorzieningen']]/span/text()[contains(.,'wasmachineaansluiting')]"
        ).get()
        if washing:
            item_loader.add_value("washing_machine", True)

        
        parking = response.xpath(
            "normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Garage']]/span/text())").get()
        if parking:
            if "Geen" in parking:
                item_loader.add_value("parking", True)

        pets = response.xpath(
            "//div[@class='descriptions']/article//text()[contains(.,'Huisdieren alleen')]").get()
        if pets:
            item_loader.add_value("pets_allowed", True)

        energy = response.xpath("normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Energieklasse']]/span/text())").get()
        if energy:
            item_loader.add_value("energy_label", energy.split(" ")[0])

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='media__gallery']//a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        floor_images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='media__plans']//a/@href"
            ).extract()
        ]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)

        bathroom_count = response.xpath("normalize-space(//div[@class='characteristics']/article/ul/li[./strong[.='Aantal badkamers']]/span/text())").get()
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        
        item_loader.add_value("landlord_phone", "035-694 56 74")
        item_loader.add_value("landlord_name", "Nienaber Makelaars")
        item_loader.add_value("landlord_email", "info@nienaber.nl")

        yield item_loader.load_item()
        
def split_address(address, get):
    city = address.split(" ")[-1]
    zip_code = "".join(filter(lambda i: i.isdigit(), address.split(" ")[0] + " " + address.split(" ")[1]))
    
    if get == "zip":
        return zip_code
    else:
        return city