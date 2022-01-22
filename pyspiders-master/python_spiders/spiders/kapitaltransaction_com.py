# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
class MySpider(Spider):
    name = 'kapitaltransaction_com'
    execution_type='testing'
    country='france'
    locale='fr' # LEVEL 1
    external_source = "Kapitaltransaction_PySpider_france_fr"
    def start_requests(self):

        payload='data%5BSearch%5D%5Boffredem%5D=2&data%5BSearch%5D%5Bidtype%5D%5B%5D=2&data%5BSearch%5D%5Bprixmax%5D=&data%5BSearch%5D%5Bpiecesmin%5D=&data%5BSearch%5D%5Bsurfmin%5D=&data%5BSearch%5D%5Bdistance_idvillecode%5D=&data%5BSearch%5D%5Bprixmin%5D=&data%5BSearch%5D%5BNO_DOSSIER%5D='
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.kapitaltransaction.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            #'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.60 YaBrowser/20.12.0.963 Yowser/2.5 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://www.kapitaltransaction.com/',
            'Accept-Language': 'tr,en;q=0.9',
            #'Cookie': 'PHPSESSID=uc6gsfoi4fhvu3pf2g79nt8pvc; SRV=c86; _ga=GA1.2.1622741717.1610360602; _gid=GA1.2.1589154351.1610360602; _gat_gtag_UA_185491114_1=1'
        }


        start_urls = [
            {
                "url" : "https://www.kapitaltransaction.com/recherche/",
                "property_type" : "apartment",
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'), method="POST", headers=headers, body=payload, callback=self.parse, meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        #with open("debug", "wb") as f: f.write(response.body)

        for item in response.xpath("//article[@itemprop='itemListElement']//picture/img/@data-url").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        address = ""
        city = response.xpath("//li[contains(.,'Ville')]/text()").get()
        if city:
            address += city.split(':')[-1].strip() + " "
            item_loader.add_value("city", city.split(':')[-1].strip())

        zipcode = response.xpath("//li[contains(.,'Code postal')]/text()").get()
        if zipcode:
            address += zipcode.split(':')[-1].strip() + " "
            item_loader.add_value("zipcode", zipcode.split(':')[-1].strip())
    
        if address:
            item_loader.add_value("address", address.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        ext_id = response.xpath("//p[@class='ref']/text()").get()
        if ext_id:
            item_loader.add_value("external_id", ext_id.split(":")[-1].strip())
 
        rent = response.xpath("//p[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
       
        utilities = response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].split("€")[0].strip())

        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].split("€")[0].replace(' ', '').strip())

        desc = "".join(response.xpath("//h2[@class='titleDetail']/following-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(':')[-1].split("m")[0].strip())
        
        room_count = response.xpath("//li[contains(.,'Nombre de chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(':')[-1].strip())

        bathroom_count = response.xpath("//li[contains(.,'salle')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(':')[-1].strip())
        
        floor = response.xpath("//li[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(':')[-1].strip())
                      
        images = [x for x in response.xpath("//ul[@class='slider_Mdl']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        furnished = response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished:
            if furnished.split(':')[-1].strip().lower() == 'oui':
                item_loader.add_value("furnished", True)
            elif furnished.split(':')[-1].strip().lower() == 'non':
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            if elevator.split(':')[-1].strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.split(':')[-1].strip().lower() == 'non':
                item_loader.add_value("elevator", False)

        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "KAPITAL TRANSACTION")
        item_loader.add_value("landlord_phone", "01 85 09 27 09")
        item_loader.add_value("landlord_email", "contact@kapitaltransaction.com")

        yield item_loader.load_item()

