# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from python_spiders.helper import extract_number_only, remove_white_spaces

class MySpider(Spider):
    name = 'ginet_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Ginet_PySpider_france" 
    custom_settings = {
          
        "PROXY_TR_ON": True, 
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
 
    }
    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cookie": "_ga=GA1.2.1696716408.1636093793; PHPSESSID=pgdbr35a5e6via44jsi58j230m; SRV=c84; _gid=GA1.2.1675227622.1636959717; _gat_UA-199966121-1=1",
        "Host": "www.ginet.fr",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    }

    def start_requests(self): 
        start_urls = [
            {
                "url" : "https://www.ginet.fr/location/1",
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,headers=self.headers
            )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@itemprop='url']"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        pagination = response.xpath("(//div[@class='nbrPage'])[1]//li/p[contains(@class,'actived')]/parent::li/following-sibling::li[1]/a/@href").get()
        if pagination:
            yield Request(
                response.urljoin(pagination),
                callback=self.parse,
            )       
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        check_prop = response.url
        if check_prop and "appartement" in check_prop.lower():
            property_type = "apartment"
        elif check_prop and "maison" in check_prop.lower():
            property_type = "house"
        elif check_prop and "villa" in check_prop.lower():
            property_type = "house"
        elif check_prop and "studio" in check_prop.lower():
            property_type = "studio"
        elif check_prop and "autre" in check_prop.lower():
            prop = "".join(response.xpath("//div[@class='offreContent']/p/text()").getall())
            if prop and "appartement" in prop.lower():
                property_type = "apartment"
            else:
                return        
        else:
            return
        
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1[@class='titleBien']/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        rent = response.xpath("//p[@class='price']/text()").get()
        if rent:
            rent = rent.replace('€','').replace(' ','').strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("external_source",self.external_source)
        
        deposit = response.xpath("//li[@class='data']/text()[contains(.,'garantie')]").get()
        if deposit:
            deposit = deposit.replace('€','').replace(' ','').strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[@class='data']/text()[contains(.,'locatives')]").get()
        if utilities:
            utilities = utilities.replace('€','').replace(' ','').strip()
            item_loader.add_value("utilities", utilities)

        room_count = response.xpath("//li[@class='data']/text()[contains(.,'pièce')]").get()
        if room_count:
            room_count = room_count.split(':')[1].strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@class='data']/text()[contains(.,'chambre')]").get()
            if room_count:
                room_count = room_count.split(':')[1].strip()
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[@class='data']/text()[contains(.,'salle')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(':')[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//li[@class='data']/text()[contains(.,'Surface')]").get()
        if square_meters:
            square_meters = square_meters.split(':')[1].replace('m²','').strip()
            item_loader.add_value("square_meters", square_meters)

        floor = response.xpath("//li[@class='data']/text()[contains(.,'niveaux')]").get()
        if floor:
            floor = floor.split(':')[1].strip()
            item_loader.add_value("floor", floor)

        address = response.xpath("//div[@class='title themTitle elementDtTitle']/h1/text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//li[@class='data']/text()[contains(.,'Ville')]").get()
        if city:
            city = city.split(':')[1].strip()
            item_loader.add_value("city", city)

        zipcode = response.xpath("//li[@class='data']/text()[contains(.,'postal')]").get()
        if zipcode:
            zipcode = zipcode.split(':')[1].strip()
            item_loader.add_value("zipcode", zipcode)

        
        desc = "".join(response.xpath("//div[@class='offreContent']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.replace('\n', ' '))

        elevator = response.xpath("//li[@class='data']/text()[contains(.,'Ascenseur')]").get()
        if elevator and "non" not in elevator.lower():
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//li[@class='data']/text()[contains(.,'Meublé')]").get()
        if furnished and "non" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//li[@class='data']/text()[contains(.,'Balcon')]").get()
        if balcony and "non" not in balcony.lower():
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[@class='data']/text()[contains(.,'Terrasse')]").get()
        if terrace and "non" not in terrace.lower():
            item_loader.add_value("terrace", True)

        images = [x for x in response.xpath("//li[contains(@class,'container_ImgSlider_Mdl')]/picture/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))    

        item_loader.add_value("landlord_name", "GINET IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 77 71 37 69")
        item_loader.add_value("landlord_email", "ginet.immobilier@ginet.fr")    
       
        yield item_loader.load_item()