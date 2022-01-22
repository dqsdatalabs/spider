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
    name = 'groupeimmoannonces_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    scale_seperator='.'
    external_source="Groupeimmoannonces_PySpider_france_fr"
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
                "url" : "https://www.groupe-immo-annonces.com/a-louer/1"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='listing2']//div[contains(@class,'bien')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='overlay']/a/@href").get())
            prop_type = item.xpath(".//p[@class='card-text']/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.groupe-immo-annonces.com/a-louer/{page}"
            yield Request(
                url, 
                callback=self.parse, 
                meta={
                    "page" : page+1
                }
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1[@class='titleBien']/text()")
        item_loader.add_value("external_link", response.url)
        
        
        rent = response.xpath("//table/tbody/tr[contains(.,'mois')]/th[2]//text()").get()
        if rent:
            rent = str(int(float(rent.split('€')[0].replace(',', '.').replace(' ', ''))))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
        
        deposit = response.xpath("//table/tbody/tr[contains(.,'garantie')]/th[2]//text()").get()
        if deposit:
            deposit = deposit.split('€')[0].replace(',', '.').replace(' ', '')
            if deposit.isnumeric():
                item_loader.add_value("deposit", str(int(float(deposit))))
        
        square_meters=response.xpath("//table/tbody/tr[contains(.,'habitable')]/th[2]//text()").get()
        if square_meters:
            meters = square_meters.split('m²')[0].strip().replace(",",".")
            item_loader.add_value("square_meters",int(float(meters)))
        
        room_count=response.xpath("//table//tr[contains(.,'chambre')]/th[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count=response.xpath("//table/tbody/tr[contains(.,'pièce')]/th[2]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count=response.xpath("//table//tr[contains(.,'salle')]/th[2]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address = response.xpath("//tr/th[contains(.,'Ville')]/following-sibling::th/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
            
        zipcode = response.xpath("//tr/th[contains(.,'Code')]/following-sibling::th/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longitude:
            try:
                latitude = latitude_longitude.split("lat:")[1].split(',')[0].strip()
                longitude = latitude_longitude.split("lng:")[1].split('}')[0].strip()
            except IndexError:
                pass
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        title = response.xpath("//h1[@class='detail-title']/span/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            
        external_id= "".join(response.xpath("//h2[@class='detail-price'][contains(.,'Ref')]//text()").getall())
        if external_id:
            external_id = external_id.split("Ref")[1].strip()
            item_loader.add_value("external_id", external_id)

        desc="".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_xpath("landlord_name","//li[@class='nom']/text()")
        item_loader.add_xpath("landlord_phone","//li[@class='tel']/text()")
        item_loader.add_xpath("landlord_email","//li[@class='mail']/text()")

        utilities = response.xpath("//table//tr[contains(.,'Honoraires')]/th[2]//text()").get()
        if utilities:
            utilities = utilities.split('€')[0].replace(',', '.').replace(' ', '')
            if utilities.isnumeric():     
                item_loader.add_value("utilities", str(int(float(utilities))))

        floor = response.xpath("//table//tr[contains(.,'Etage')]/th[2]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        furnished=response.xpath("//table//tr[contains(.,'Meublé')]/th[2]//text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                furnished = True
            elif furnished.strip().lower() == 'non':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)
        
        terrace=response.xpath("//table//tr[contains(.,'Terrasse')]/th[2]//text()").get()
        if terrace:
            if terrace.strip().lower() == 'oui':
                terrace = True
            elif terrace.strip().lower() == 'non':
                terrace = False
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)

        elevator = response.xpath("//table//tr[contains(.,'Ascenseur')]/th[2]//text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//table//tr[contains(.,'Balcon')]/th[2]//text()").get()
        if balcony:
            if balcony.strip().lower() == 'oui':
                balcony = True
            elif balcony.strip().lower() == 'non':
                balcony = False
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        parking = response.xpath("//table//tr[contains(.,'parking') or contains(.,'garage')]/th[2]//text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
            else:
                parking = False
            item_loader.add_value("parking", parking)

        yield item_loader.load_item()
