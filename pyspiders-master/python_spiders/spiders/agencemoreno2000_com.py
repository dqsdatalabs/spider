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
import math
import re

class MySpider(Spider):
    name = 'agencemoreno2000_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencemoreno_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agence-moreno2000.com/location/appartements/1", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'selectionBien')]/article//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agencemoreno_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//h1[@class='titleBien']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        item_loader.add_value("external_link", response.url)

        latitude_longitude = response.xpath("//script[contains(., 'getMap')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('center: {')[1].split('}')[0]
            latitude = latitude_longitude.split(',')[0].strip().split(':')[1].strip()
            longitude = latitude_longitude.split(',')[1].strip().split(':')[1].strip()  
            
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address=response.xpath("//ul/li[contains(.,'Ville')]/text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
        
        zipcode=response.xpath("//ul/li[contains(.,'Code')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())
        
        bathroom_count=response.xpath("//ul/li[contains(.,'salle')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
            
        deposit=response.xpath("//ul/li[contains(.,'garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            if "Non" not in deposit:
                item_loader.add_value("deposit", deposit )
            
        utilities=response.xpath("//ul/li[contains(.,'charge')]/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            if "." in utilities:
                item_loader.add_value("utilities", utilities.split(".")[0])
            elif utilities:
                item_loader.add_value("utilities", utilities)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))

        square_meters = response.xpath("//li[contains(.,'urface')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[1].strip().split(' ')[0].replace(",", ".")
            item_loader.add_value("square_meters", str(math.ceil(float(square_meters))))

        room_count = response.xpath("//li[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            room_count = room_count.split(':')[1].strip()
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//ul[@id='infosfi']/li[contains(.,'mois')]/text()").get()
        if rent:
            price=rent.split(":")[1].replace(" ","")
            item_loader.add_value("rent_string", price)

        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h2[contains(.,'offre')]/parent::div/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if "ascenceur" in desc_html:
            item_loader.add_value("elevator", True)
        
        city = response.xpath("//div[@class='title themTitle elementDtTitle']/h1/text()").get()
        if city:
            city = city.strip('La ville de ').strip().split('(')[0].strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//ul[@class='slider_Mdl']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//ul[@id='infos']/li[contains(., 'eubl')]/text()").get()
        if furnished:
            if furnished.split(':')[1].strip().lower() == 'non':
                furnished = False
            elif furnished.split(':')[1].strip().upper() == 'OUI':
                furnished = True
        if furnished != True and furnished != False:
            furnished = None
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//ul[@id='infos']/li[contains(., 'tage')]/text()").get()
        if floor:
            floor = floor.split(':')[1].strip()
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//ul[@id='infos']/li[contains(., 'scenseur')]/text()").get()
        if elevator:
            if elevator.split(':')[1].strip().lower() == 'non':
                elevator = False
            elif elevator.split(':')[1].strip().upper() == 'OUI':
                elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//ul[@id='details']/li[contains(., 'alcon')]/text()").get()
        if balcony:
            if balcony.split(':')[1].strip().lower() == 'non':
                balcony = False
            elif balcony.split(':')[1].strip().upper() == 'OUI':
                balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//ul[@id='details']/li[contains(., 'erras')]/text()").get()
        if terrace:
            if terrace.split(':')[1].strip().lower() == 'non':
                terrace = False
            elif terrace.split(':')[1].strip().upper() == 'OUI':
                terrace = True
            item_loader.add_value("terrace", terrace)

        item_loader.add_value("landlord_name", "MORENO 2000")
        item_loader.add_value("landlord_email", "moreno2000@orange.fr")
        landlord_phone = response.xpath("//a[@class='dispPhoneAgency']//span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data