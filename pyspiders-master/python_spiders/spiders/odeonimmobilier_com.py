# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'odeonimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Odeonimmobilier_PySpider_france_fr"
    

    def start_requests(self):
        start_urls = [
            {"url": "http://www.odeon-immobilier.com/location/maisons-villas/1", "property_type": "house"},
            {"url": "http://www.odeon-immobilier.com/location/appartements/1", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//article[contains(@class,'row bien')]"):
            follow_url = response.urljoin(item.xpath(".//p/a/@href").extract_first())

            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if response.meta.get('property_type') == "apartment":
            if page == 2 or seen:
                url = f"http://www.odeon-immobilier.com/location/appartements/{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})
        elif response.meta.get('property_type') == "house":
            if page == 2 or seen:
                url = f"http://www.odeon-immobilier.com/location/maisons-villas/{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//p[contains(.,'Référence')]/text()").get().split(": ")[1])

        description = response.xpath("//h3[contains(.,'Détails')]/following-sibling::p[1]/text()").get()
        if description and description.strip("\xa0") != "":
            item_loader.add_value("description", description.strip())
            

        latLng = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latLng:
            latitude = latLng.split("center: ")[1].split(",")[0].split("lat")[1].split(":")[1].strip()
            longitude = latLng.split("center: ")[1].split(",")[1].split("lng")[1].split(":")[1].strip("}").strip()
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

        utilities = response.xpath("//div//p/span[contains(.,'charge')]/following-sibling::span//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        bathroom = response.xpath("//div//p/span[contains(.,'bains')]/following-sibling::span//text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        zipcode = response.xpath("//span[contains(.,'postal')]/following-sibling::span[1]/text()").get()
        if zipcode:    
            item_loader.add_value("zipcode", zipcode)
        
        address = response.xpath("//h2[@class='prix']//text()").get()
        if address:
            address = address.split("(")[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
           

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("//span[contains(.,'habitable')]/following-sibling::span[1]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))
        
        room_count = response.xpath("//span[contains(.,'pièces')]/following-sibling::span[1]/text()").get()
        item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//ul[@class='clearing-thumbs clearing-feature text-center']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        price = response.xpath("//span[contains(.,'Loyer')]/following-sibling::span[1]/text()").get()
        if price:
            price = price.strip().strip("€").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span[1]/text()[.!='Non renseigné']").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip().strip("€").strip())
        
        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::span[1]/text()").get()
        if furnished:
            if furnished.lower() != "non":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span[1]/text()").get()
        if elevator:
            if elevator.lower() != "non":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span[1]/text()").get()
        if balcony:
            if balcony.lower() != "non":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        
        parking = response.xpath("//span[contains(.,'parking')]/following-sibling::span[1]/text()").get()
        if parking:
            if parking.lower() != "non" or parking.lower() != "0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span[1]/text()").get()
        if terrace:
            if terrace.lower() != "non":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        parking = response.xpath("//span[contains(.,'garage')]/following-sibling::span[1]/text()").get()
        if parking:
            if parking.lower() != "non":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        

        item_loader.add_value("landlord_phone", "04 48 49 01 00")
        item_loader.add_value("landlord_email", "contact@odeon-immobilier.com")
        item_loader.add_value("landlord_name", "ODEON IMMOBILIER")

        yield item_loader.load_item()