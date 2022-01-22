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
    name = 'abc_immobilier_angers_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://abc-immobilier-angers.fr/advanced-search/?keyword=&status=location&type=appartement&bedrooms=&min-area=&max-price=&property_id=&bedrooms=&max-area=&min-price=",
                    "https://abc-immobilier-angers.fr/advanced-search/?keyword=&status=location&type=duplex&bedrooms=&min-area=&max-price=&property_id=&bedrooms=&max-area=&min-price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://abc-immobilier-angers.fr/advanced-search/?keyword=&status=location&type=maison&bedrooms=&min-area=&max-price=&property_id=&bedrooms=&max-area=&min-price=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://abc-immobilier-angers.fr/advanced-search/?keyword=&status=location&type=studio&bedrooms=&min-area=&max-price=&property_id=&bedrooms=&max-area=&min-price=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-listing')]/div/div[contains(@id,'ID')]//div[contains(@class,'body-right')]//a[contains(.,'Détails')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[@rel='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Abc_Immobilier_Angers_PySpider_france")
        item_loader.add_xpath("title", "//div/h1/text()")
  
        zipcode = response.xpath("//ul/li[@class='detail-zip']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip()) 
        city = response.xpath("//ul/li[@class='detail-city']/text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip()) 
        address = response.xpath("//div[@id='address']//address[@class='property-address']/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip()) 
        elif city:
            if zipcode:
                city = zipcode.strip() +", "+city.strip()
            item_loader.add_value("address",city.strip()) 
        room_count = response.xpath("//li/div[strong[contains(.,'Chambres ')]]/label/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
        else:
            room_count = response.xpath("substring-before(//li/div[strong[contains(.,'Pièces :')]]/label/text(),'pièce')").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip()) 
        item_loader.add_xpath("external_id", "//li/div[strong[contains(.,'Référence :')]]/label/text()")
        
        item_loader.add_xpath("bathroom_count", "//li/div[strong[contains(.,'Salle(s) de Bain')]]/label/text()")
        item_loader.add_xpath("floor", "//li/div[strong[contains(.,'Etage :')]]/label/text()")

        energy_label = response.xpath("substring-after(//div[@class='col-md-6 DPEBOX']/h5[contains(.,'DPE')]/text(),':')").get()
        if energy_label:        
            energy_label = energy_label.split("(")[0].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy_label)

        rent = response.xpath("//span[@class='item-price']/text()").extract_first()
        if rent:    
            item_loader.add_value("rent_string",rent.replace(" ","").replace('\xa0', ''))  
           
        square = response.xpath("//li/div[strong[contains(.,'Surface :')]]/label/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        deposit = response.xpath("//li/div[strong[contains(.,'Dépôt de Garantie')]]/label/text()").extract_first()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace(":","").replace(" ","")
            item_loader.add_value("deposit",int(float(deposit.replace(",",".")))) 

        utilities = response.xpath("//li/div[strong[contains(.,'charge du locataire ')]]/label/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.split("€")[0].strip().replace(",",".")))) 

        parking = response.xpath("//li/div[strong[contains(.,'Garages ')]]/label/text() | //ul[contains(@class,'list-features')]/li[contains(.,'Parking')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//ul[contains(@class,'list-features')]/li[contains(.,'Terrasse')]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//ul[contains(@class,'list-features')]/li[contains(.,'Balcon')]//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//ul[contains(@class,'list-features')]/li[contains(.,'Ascenseur')]//text()").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)
     
        latlng = response.xpath("//script[contains(.,'property_lng') and contains(.,'property_lat')]//text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('"property_lat":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latlng.split('"property_lng":"')[1].split('"')[0].strip())

        desc = " ".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery-inner']/div/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "ABC Immobilier")
        item_loader.add_value("landlord_phone", "0241883327")
        item_loader.add_value("landlord_email", "contact@abc-immobilier-angers.fr")
  
        yield item_loader.load_item()