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
    name = 'pibracimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Pibracimmobilier_PySpider_france_fr"
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
                "url" : "https://www.pibracimmobilier.com/a-louer/1"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//ul[@class='listingUL']/li"):
            follow_url = response.urljoin(item.xpath(".//div[@class='backBtn']/a/@href").get())
            prop_type = item.xpath(".//h2/text()").get()
            property_type = ""
            if "Appartement" in prop_type:
                property_type = "apartment"
            elif "Maison" in prop_type:
                property_type = "house"
            elif "Studio" in prop_type:
                property_type = "apartment"
            elif "Duplex" in prop_type:
                property_type = "apartment"
            elif "Villa" in prop_type:
                property_type = "house"
            elif "Immeuble" in prop_type:
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.pibracimmobilier.com/a-louer/{page}"
            yield Request(
                url, 
                callback=self.parse, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                    "page" : page+1
                }
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//div[@class='themTitle']/h1/text()")

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        latitude_longitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat : ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:  ')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        zipcode = response.xpath("//span[contains(.,'Code postal')]/following-sibling::span/text()").get()
        if zipcode:           
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:  
            item_loader.add_value("city", city.strip())   

        if city and zipcode:  
            item_loader.add_value("address", city.strip() + ' (' + zipcode.strip() + ')')  

        bathroom_count = response.xpath("//span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters = response.xpath("//span[contains(.,'Surface habitable (m²)')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'Nombre de chambre')]/following-sibling::span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        
        elif not room_count:
            room1=response.xpath("//span[contains(.,'Nombre de pièces')]/following-sibling::span/text()").get()
            if room1:
                room1= re.findall("\d+",room1)
                item_loader.add_value("room_count", room1)

        rent = response.xpath("//span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        utilities = response.xpath("//span[contains(.,'Charges')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip().replace('\xa0', '').replace(' ', ''))

        external_id = response.xpath("//li[@itemprop='productID']/text()").get()
        if external_id:
            external_id = external_id.strip().strip('Ref').strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
            
        images = [x for x in response.xpath("//ul[@class='imageGallery  loading']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                furnished = True
            elif furnished.strip().lower() == 'non':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        parking = response.xpath("//span[contains(.,'parking') or contains(.,'garage')]/following-sibling::span/text()").get()
        if parking:
            if int(parking.strip()) > 0: item_loader.add_value("parking", True)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if balcony.strip().lower() == 'oui':
                balcony = True
            elif balcony.strip().lower() == 'non':
                balcony = False
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if terrace.strip().lower() == 'oui':
                terrace = True
            elif terrace.strip().lower() == 'non':
                terrace = False
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)

        landlord_phone = response.xpath("//a[@class='tels']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[@class='mail']/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("landlord_name", "PIBRAC IMMO")
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data