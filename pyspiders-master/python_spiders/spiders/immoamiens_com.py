# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'immoamiens_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source= "Immoamiens_PySpider_france_fr"
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.immo-amiens.com/a-louer/maisons/1", "property_type":"house",
                "url": "http://www.immo-amiens.com/a-louer/appartements/1", "property_type":"apartment"
             }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse, meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False

        for item in response.xpath("//ul[@class='listingUL']/li/article[@class='row panelBien']"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            if response.meta.get('property_type')=='house':
                url = f"http://www.immo-amiens.com/a-louer/maisons/{page}"
            elif response.meta.get('property_type')=='apartment':
                url = f"http://www.immo-amiens.com/a-louer/appartements/{page}"
            yield Request(
                    url=url,
                    callback=self.parse,
                    meta={"page":page+1, 'property_type': response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
         
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title))
            
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        latitude_longitude = response.xpath("//script[contains(.,'geocoder')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat : ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:  ')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)            

        square_meters = response.xpath(
            "//span[contains(.,'Surface habitable (m²)')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))        

        room_count = response.xpath(
            "//span[contains(.,'pièces')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//span[contains(.,'salle de bains') or contains(.,'Nb de salle d') ]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        zipcode = response.xpath("//span[contains(.,'Code postal')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
                         
        address = response.xpath("//span[contains(.,'Quartier')]/following-sibling::span/text()").get()
        if address:
            if city:
                address = address.strip() + ", " + city.strip()
            item_loader.add_value("address", address.strip())   
        elif not address:
            item_loader.add_value("address", city.strip()) 
        rent = response.xpath(
            "//div[@id='infosfi']/p/span[contains(.,'Prix') or contains(.,'Loyer')]/following-sibling::span/text()"
            ).get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent_string", rent.replace(" ",""))        

        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.strip().strip('Ref').strip()
            item_loader.add_value("external_id", external_id)

        description = " ".join(response.xpath("//p[@itemprop='description']/text()").getall())            
        if description:
            item_loader.add_value("description", description.strip())
            if "meublé" in description:
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//ul[@class='imageGallery imageHC  loading']/li/@data-thumb[not(contains(.,'images/no_bien'))]").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span/text()[not(contains(.,'Non'))]").get()
        if deposit:
            deposit = deposit.split('€')[0].strip()
            item_loader.add_value("deposit", deposit.replace(" ",""))

        utilities = response.xpath("//span[contains(.,'Charges locatives')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split('€')[0].strip()
            item_loader.add_value("utilities", utilities.replace(" ",""))

        furnished = response.xpath("//span[contains(.,'Meubl')]/following-sibling::span/text()[not(contains(.,'Non renseigné'))]").get()
        if furnished:
            if 'non' in furnished.strip().lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//span[contains(.,'Nombre de parking') or contains(.,'Nombre de garage')]/following-sibling::span/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if 'non' in elevator.strip().lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if 'non' in balcony.strip().lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if 'non' in terrace.strip().lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "immo-amiens")    
        item_loader.add_value("landlord_phone", "03 22 91 60 37")    
        item_loader.add_value("landlord_email", "contact@immo-amiens.com")
        
        yield item_loader.load_item()

