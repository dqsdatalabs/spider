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
    name = 'loctoit_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://loctoit.fr/locations-rennes.php?vlt=1&type_loft=1&type_app1=1&type_app2=1&type_app3=1&type_app4=1&situation=&submit_chercher=Chercher+%3E%3E",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://loctoit.fr/locations-rennes.php?vlt=1&type_maison=1&situation=&submit_chercher=Chercher+%3E%3E",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://loctoit.fr/locations-rennes.php?vlt=1&type_studio=1&situation=&submit_chercher=Chercher+%3E%3E",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "http://loctoit.fr/locations-rennes.php?vlt=1&type_chambre=1&situation=&submit_chercher=Chercher+%3E%3E",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//div[contains(@style,'width:94%; margin:0')]"):
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_source", "Loctoit_PySpider_france")
            item_loader.add_value("property_type", response.meta["property_type"])
            
            
            title = item.xpath(".//div/b[1]//text()").get()
            if title:
                item_loader.add_value("title", title)
                if "m2" in title:
                    square_meters = title.split("m2")[0].strip().split(" ")[-1]
                    item_loader.add_value("square_meters", square_meters)

                if "meubl" in title: item_loader.add_value("furnished", True)
            
            address = item.xpath(".//text()[contains(.,'Localit')]").get()
            if address:
                address = address.split(":")[1].strip()
                item_loader.add_value("address", address)
                zipcode = address.split(" ")[0]
                item_loader.add_value("city", address.split(zipcode)[1].split("-")[0].strip())
                item_loader.add_value("zipcode", zipcode)
            
            rent = item.xpath(".//text()[contains(.,'LOYER CHAR')]").get()
            if rent:
                price = rent.split(":")[1].split("euro")[0].strip()
                item_loader.add_value("rent", price)
                item_loader.add_value("currency", "EUR")
            
            deposit = item.xpath(".//text()[contains(.,'DEPOT')]").get()
            if deposit:
                deposit = deposit.split(":")[1].split("euro")[0].strip()
                item_loader.add_value("deposit", deposit)
            
            utilities = item.xpath(".//text()[contains(.,'CHARGES :')]").get()
            if utilities:
                utilities = utilities.split("CHARGES :")[1].split("euro")[0].strip()
                if utilities !='0':
                    item_loader.add_value("utilities", utilities)
            
            description = "".join(item.xpath(".//div//text()").getall())
            if description:
                description = re.sub('\s{2,}', ' ', description.strip())
                item_loader.add_value("description", description)
            
            if "studio" in title.lower():
                item_loader.add_value("room_count", "1")
            elif "chambre" in description:
                room_count = description.split("chambre")[0].strip().split(" ")[-1]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)

            import dateparser
            available_date = item.xpath(".//div[contains(.,'mise')]/text()").get()
            if available_date:
                available_date = available_date.split(" le ")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            if "étage" in title:
                floor = title.split("étage")[0].strip().split(" ")[-1]
                item_loader.add_value("floor", floor)
            
            elevator = item.xpath("./div/font[contains(.,'ascenseur')]").get()
            if elevator:
                item_loader.add_value("elevator", True)

            images = [x for x in item.xpath(".//a//@src").getall()]
            if images:
                item_loader.add_value("images", images)
            
            item_loader.add_value("landlord_name", "Loc'Toit")
            item_loader.add_value("landlord_phone", "02 99 840 840")


            external_id = response.xpath("//font[contains(text(),'Annonce')]/text()").get()
            if external_id:
                external_id = external_id.split()[-1].strip().split("°")[-1]
                item_loader.add_value("external_id",external_id)

            item_loader.add_value("external_link", response.url + "#"+ external_id)


            yield item_loader.load_item()