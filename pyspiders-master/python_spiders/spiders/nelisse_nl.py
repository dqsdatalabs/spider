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
    name = 'nelisse_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.nelisse.nl/nl/realtime-listings/consumer"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"]:
                property_type = item["mainType"]
                if "apartment" in property_type:
                    property_type = "apartment"
                elif "house" in property_type or "buildLot" in property_type:
                    property_type = "house"
                url = response.urljoin(item["url"])
                yield Request(url, callback=self.populate_item, meta={"property_type" : property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("normalize-space(//dl[@class='full-details']/dt[contains(.,'Status')]/following-sibling::dd[1]/text())").get()
        if "Verhuurd" in rented or "Verkocht" in rented :
            return
        item_loader.add_value("external_source", "Nelisse_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath("//h1/text()").extract_first()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = "".join(response.xpath("//div[@id='description']//p[@class='object-description']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc.strip())
            if "balkon" in desc.lower():
                item_loader.add_value("balcony", True)
            if "parkeergarage" in desc:
                item_loader.add_value("parking", True)
        utilities = response.xpath("normalize-space(//dl[@class='full-details']/dt[.='Servicekosten']/following-sibling::dd[1]/text())").extract_first()
        utilities_desc = response.xpath("//div[@id='description']//p[@class='object-description']/text()[contains(.,'Servicekosten') and contains(.,'€')]").extract_first()
        
        if utilities:
            util = utilities.split("€")[1]
            item_loader.add_value("utilities", util)
        elif not utilities and utilities_desc:
            util = utilities_desc.split("€")[1]
            item_loader.add_value("utilities", util)


        price = response.xpath("normalize-space(//dl[@class='full-details']/dt[.='Vraagprijs' or .='Huurprijs' or .='Prijs']/following-sibling::dd[1]/text())").get()
        if price:
            item_loader.add_value("rent", price.strip().split(" ")[-3])
        item_loader.add_value("currency", "EUR")
 
 
        square = response.xpath(
            "normalize-space(//dl[@class='full-details']/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text())").get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            ) 
        
        item_loader.add_xpath("room_count", "normalize-space(//dl[@class='full-details']/dt[.='Aantal slaapkamers']/following-sibling::dd[1]/text())")

        map_address = response.xpath("//div[@id='location']//div[@class='simple-map-markers']//text()").get()
        if map_address:
            json_l = json.loads(map_address)
            for data in json_l:
                map_city = data["city"]
                map_zipcode = data["zipCode"]
            if map_city:
                item_loader.add_value("city", map_city.strip())
            if map_zipcode:
                item_loader.add_value("zipcode", map_zipcode.strip())
                latitude = data["lat"]
                longitude = data["lng"]
                if latitude:
                    item_loader.add_value("latitude", str(latitude))
                if longitude:
                    item_loader.add_value("longitude", str(longitude))

                

        street = response.xpath("//div[@class='container']//h1/text()").get()
        city = response.xpath("//div[@class='container']//h1/small/text()").get()
        if city:
            item_loader.add_value("address", street.strip() + " " + city.strip())
            # item_loader.add_value("city", city.split("(")[1].split(")")[0])
        else:
            item_loader.add_value("address", street.strip())
            
            

        item_loader.add_xpath("floor","normalize-space(//dl[@class='full-details']/dt[.='Verdiepingen']/following-sibling::dd[1]/text())",
        )

        parking = response.xpath(
            "//dt[.='Soort garage']/following-sibling::dd[1]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath(
            "//dd[contains(.,'Lift') or contains(.,'lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = response.xpath(
            "//dt[.='Balkon']/following-sibling::dd[1]/text()").get()
        if balcony:
            if "Ja" in balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='responsive-gallery-media']/img/@data-src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("energy_label", "//dt[.='Energielabel']/following-sibling::dd[1]/text()")
        
        item_loader.add_value("landlord_phone", "+31(0)703501400")
        item_loader.add_value("landlord_name", "Nelisse Makelaars")
        item_loader.add_value("landlord_email", "info@nelisse.nl")

        yield item_loader.load_item()