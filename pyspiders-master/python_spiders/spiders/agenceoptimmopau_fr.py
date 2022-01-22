# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'agenceoptimmopau_fr'  
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.agence-optimmo-pau.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&tri=d_dt_crea&", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
 
    # 1. FOLLOWING
    def parse(self, response): 
        
        for item in response.xpath("//div[@class='annonce']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item ,meta={'property_type': response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agenceoptimmopau_PySpider_"+ self.country + "_" + self.locale)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1/text()").get()
        if title:
            title = title.strip().replace('\n', ' ').replace('\t', '').replace('\r', '')
            item_loader.add_value("title", title)
        
        rent="".join(response.xpath("//div[@itemprop='offers']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//div[@class='detail-surf']/div[contains(.,'m²')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        room_count=response.xpath("//div[contains(text(),'Chambre')]/following-sibling::div/text()").get()
        if room_count: 
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count1=response.xpath("//div[contains(text(),'Pièce')]/following-sibling::div/text()").get()
            if room_count1:
                item_loader.add_value("room_count", room_count1.strip())


        
        address="".join(response.xpath("//strong[@class='detail-ville']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'LATITUDE')]//text()").get()
        latitude = latitude_longitude.split('LATITUDE_CARTO: "')[1].split('"')[0]
        longitude = latitude_longitude.split('LONGITUDE_CARTO: "')[1].split('"')[0]
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
       
            
        external_id="".join(response.xpath("//p[@class='detail-ref']/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split('.')[1].split('Maj')[0].strip())

        desc="".join(response.xpath("//p[@class='detail-desc-txt']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        image="".join(response.xpath("//div[@class='detail-galerie-item']/@style").getall())
        images=image.split('background-image:url(')
        image_size=len(images)
        for i in range(1,image_size):
            item_loader.add_value("images", image.split('image:url(')[i].split(')')[0])
        item_loader.add_value("external_images_count", str(image_size))
        
        item_loader.add_value("landlord_name","OPTIMMO")
        item_loader.add_value("landlord_email", "referencementprestataire@gmail.com")
        item_loader.add_value("landlord_phone","05 59 98 82 85")
        
        floor=response.xpath("//li[@title='Etage']/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        furnished=response.xpath("//li[@title='Meublé']/div[2]/text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        utilities="".join(response.xpath("//div[@class='detail-desc-prix']/ul/li[contains(.,'Charges')]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split(':')[1].split('€')[0].strip())
        
        deposit=response.xpath("//div[@class='detail-desc-prix']/strong[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(':')[1].split('€')[0].strip())
        
        elevator=response.xpath("//li[@title='Ascenseur']/div[2]/text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        energy_label=response.xpath("//div[contains(@class,'bg-white')]/p[contains(@class,'diagLettre')]/text()[not(contains(.,'VI'))]").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
            
        parking=response.xpath("//li[@title='Parking']/div[2]/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        city = response.xpath("//h1/text()").get()
        if city:
            item_loader.add_value("city", city.strip().split(" ")[-2].strip())
            item_loader.add_value("zipcode", city.strip().split(" ")[-1].split("(")[-1].split(")")[0].strip())
        
        bathroom_count = response.xpath("//div[contains(text(),\"Salle d'eau\")]/following-sibling::div/text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        yield item_loader.load_item()