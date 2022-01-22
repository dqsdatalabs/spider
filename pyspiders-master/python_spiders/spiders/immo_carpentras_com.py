# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 

class MySpider(Spider):
    name = 'immo_carpentras_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self):
        yield Request("https://www.maisondepierre.fr/location/1", 
                    callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property__content-wrapper']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)        
        
        next_page = response.xpath("//a[@class='pagination__link'][contains(.,'Page suivante')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = " ".join(response.xpath("//ol[@class='breadcrumb__items']/li//text()").getall()).strip()
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: 
            return
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Immo_Carpentras_PySpider_france")

        external_id = response.xpath("//div[@class='detail-1__reference']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        zipcode = response.xpath("//div[span[.='Code postal']]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[span[.='Ville']]/span[2]/text()").get()
        if city:
            address = city.strip()
            if zipcode:
                address += ", "+zipcode.strip()
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", address)
               
        title = response.xpath("//div[@class='main-info__content-wrapper']//div[@class='title__content']/span/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='detail-1__container-text']//text()").getall()).strip()   
        if description: 
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[span[.='Surface habitable (m²)']]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].strip())

        room_count = response.xpath("//div[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif not room_count:
            room1=response.xpath("//div[span[contains(.,'pièces')]]/span[2]/text()").get()
            if room1:
                item_loader.add_value("room_count", room1.strip())
               

        
        bathroom_count = response.xpath("//div[span[contains(.,'salle d')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        floor = response.xpath("//div[span[.='Etage']]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        rent = response.xpath("//div[@class='main-info__price']/span/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
        
        deposit = response.xpath("//div[span[contains(.,'Dépôt de garantie')]]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(' ', '').strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-slide slider-img__swiper-slide']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        utilities = "".join(response.xpath("//div[span[contains(.,'Charges locatives')]]/span[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities)
        furnished = response.xpath("//div[span[.='Meublé']]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        parking = response.xpath("//div[span[.='Nombre de garage']]/span[2]/text()").get()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        elevator = response.xpath("//div[span[.='Ascenseur']]/span[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        terrace = response.xpath("//div[span[.='Terrasse']]/span[2]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
       
        item_loader.add_xpath("latitude", "//div[@class='module-map-poi']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='module-map-poi']/@data-lng")
    
        item_loader.add_value("landlord_phone", "04 90 67 31 91")
        item_loader.add_value("landlord_email", "carpentras@maisondepierre.fr")
        item_loader.add_value("landlord_name", "La Maison de Pierre - Carpentras")
      
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower()):
        return "house"
    else:
        return None