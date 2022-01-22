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
    name = 'agence_immobiliere_manosque_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Agence_Immobiliere_Manosque_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1135&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false",
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1135&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=1&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-2",
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1136&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false",
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1136&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=true#page-2"
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1137&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false"

                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1200&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false",
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1213&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.agence-immobiliere-manosque.fr/immo/c/front/search/pk/99/mode/1/?immo[type]=rent&immo[typeEstate]=1132&immo[home]=false&immo[city]=0&immo[cityKey]=&immo[mode]=ajax&immo[page]=0&immo[perPage]=6&immo[orderOrigin]=price&immo[orderWay]=asc&immo[display]=list&immo[flagSearch]=true&immo[searchPagination]=false",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property-row-image']//a[@class='immoListPhotoLink']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//h3[@class='property-price']/text()").get()
        status = status.split("€")[0].replace(" ","")
        status = int(status)
        if status and 50000 < status:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//span[@id='immoAttributeRef']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        adres=response.xpath("//div[@class='property-location']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            city=adres.split(" ")[1]
            zipcode=adres.split(" ")[0]
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='property-description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        #     # if "agence :" in description.lower():
        #     #     agency_fees = description.lower().split("agence :")[1].split("€")[0].replace(" ","")
        #     #     item_loader.add_value("agency_fees", agency_fees)
            


        square_meters = response.xpath("//strong/span[.='Surface']/parent::strong/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//strong/span[.='Nb pièces']/parent::strong/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//strong/span[.='Nb salle de bain']/parent::strong/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//h3[@class='property-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[@class='immoListDate']//text()[last()]").getall()
        if available_date:
            item_loader.add_value("available_date", available_date[-1].strip())
        
        deposit = response.xpath("//strong[.='Dépôt de garantie']/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(':')[-1].split('€')[0].replace(' ', '').strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='property-gallery']/div//a//@href").getall()]
        if images:
 
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'property.lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('property.lat=')[-1].split(';')[0].strip())
            item_loader.add_value("longitude", latitude.split('property.lng=')[-1].split(';')[0].strip())
        
        energy_label = response.xpath("//div[@class='energy-class']//span/@class").get()
        if energy_label:
            
            item_loader.add_value("energy_label", energy_label.upper())
        
        # floor = response.xpath("//li[contains(.,'Etage')]/div[contains(@class,'val')]/text()").get()
        # if floor:
        #     item_loader.add_value("floor", floor.strip())

        utilities =response.xpath("//p[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(' ')[-1].split('€')[0].strip())
        
        parking = response.xpath("//strong[.='Garage']").get()
        if parking: 
            item_loader.add_value("parking", True)


        # balcony = response.xpath("//li[contains(.,'Balcon')]/div[contains(@class,'val')]/text()").get()
        # if balcony:
        #     if int(balcony.strip()) > 0:
        #         item_loader.add_value("balcony", True)
        #     elif int(balcony.strip()) == 0:
        #         item_loader.add_value("balcony", False)

        # furnished = response.xpath("//li[contains(.,'Meublé')]/div[contains(@class,'val')]/text()").get()
        # if furnished:
        #     if furnished.strip().lower() == 'oui':
        #         item_loader.add_value("furnished", True)
        #     elif furnished.strip().lower() == 'non':
        #         item_loader.add_value("furnished", False)



        terrace = response.xpath("//strong[.='Terrasse']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        elevator = response.xpath("//strong[.='Ascenseur']").get()
        if elevator:
            item_loader.add_value("elevator", True)

        # swimming_pool = response.xpath("//li[contains(.,'Piscine')]/div[contains(@class,'val')]").get()
        # if swimming_pool:
        #     if swimming_pool.strip().lower() == 'oui':
        #         item_loader.add_value("swimming_pool", True)
        #     elif swimming_pool.strip().lower() == 'non':
        #         item_loader.add_value("swimming_pool", False)

        item_loader.add_value("landlord_name", "AGENCE ERA")
        item_loader.add_value("landlord_phone", "04 92 74 01 69")
        item_loader.add_value("landlord_email","contact@agence-immobiliere-manosque.com")

        yield item_loader.load_item()