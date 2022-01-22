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
    name = 'gfi_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.gfi-immo.com/location/annonces?cat=location_appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.gfi-immo.com/location/annonces?cat=location_maison",
                    
                    ],
                "property_type": "house"
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
        for item in response.xpath("//div[@class='annonce']"):
            follow_url = response.urljoin(item.xpath("./@onclick").get().split("href='")[1].split("'")[0])
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Gfi_Immo_PySpider_france")
        item_loader.add_value("external_id", response.url.split("id=")[1])

        title = " ".join(response.xpath("//div[contains(@class,'annonce_page_title')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'annonce_page_ville')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//div[contains(@class,'annonce_page_ville')]//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        square_meters = response.xpath("//div[contains(@class,'annonce_page_superficie')]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(@class,'amount')]/text()").get()
        if rent:
            rent = rent.strip().split(",")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'annonce_page_description')]//p//text()[contains(.,'Dépôt de Garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//div[contains(@class,'annonce_page_description')]//p//text()[contains(.,'Loyer')]").get()
        if utilities and "+" in utilities:
            utilities = utilities.split("+")[1].split("=")[0].replace("€","").strip()          
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'annonce_page_description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'chambre')]//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'piece')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slide_container')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'parking')]//following-sibling::div//text()[contains(.,'oui')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'balcon')]//following-sibling::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'terrasse')]//following-sibling::div//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'étage')]//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        energy_label = response.xpath("//div[contains(@class,'annonce_page_car_elt_title')][contains(.,'Consommations énergétiques')]//following-sibling::div/text()").get()
        if energy_label:
            if "et" in energy_label:
                energy_label = energy_label.split("et")[1].split("kW")[0]
                item_loader.add_value("energy_label", energy_label)
            else: 
                energy_label = energy_label.split("kW")[0].strip().split(" ")[-1]
                item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "GFI IMMOBILIER")
        item_loader.add_value("landlord_phone", "01 48 59 84 84")
        item_loader.add_value("landlord_email", "contact@gfi-immo.com")

        yield item_loader.load_item()