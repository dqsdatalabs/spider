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
import re

class MySpider(Spider):
    name = 'centraleimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Centraleimmo_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.centraleimmo.fr/location/appartement", "property_type": "apartment"},
	        {"url": "https://www.centraleimmo.fr/location/maison", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='inner']/h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//li[contains(@class,'pager-next first')]/a/@href").get()
        if next_page:        
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Centraleimmo_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        rent=response.xpath("//div[@class='info'][contains(.,'Loyer')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
                
        square_meters=response.xpath("//div[@class='row infos-comp']/div[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
                
        room_count = "".join(response.xpath("//div[contains(., 'Nombre de chambres')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count=''.join(response.xpath("//div[contains(., 'Nombre de pièces')]/text()").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count=response.xpath("//div[@class='row infos-comp']/div[contains(.,'de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)       
       
        address=response.xpath("//div[@class='adresse']/text()").get()
        if address:
            item_loader.add_value("address", address)
            try:
                zipcode = address.split(" - ")[-1].strip().split(" ")[0].strip()
                print(zipcode)
                city = address.split(zipcode)[1].strip()
                item_loader.add_value("city", city)
                if zipcode.isdigit():
                    item_loader.add_value("zipcode", zipcode)
            except:
                pass

        external_id=response.xpath("//div[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('.')[1].strip())

        desc="".join(response.xpath("//div[@class='col-xs-12']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        floor=response.xpath("//div[@class='row infos-comp']/div[contains(.,'Étage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)           
        
            
        images=[x for x in response.xpath("//div[@class='photos']/div/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilities=response.xpath("//div[@class='row infos-comp']/div[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())
        
        deposit=response.xpath("//div[@class='row infos-comp']/div[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip().replace(" ",""))
        
        elevator=response.xpath("//div[@class='row infos-comp']/div[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator",False)
            else:
                item_loader.add_value("elevator",True)
        
        terrace = response.xpath("//div[@class='row infos-comp']/div[contains(.,'Terrasse')]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace",False)
            else:
                item_loader.add_value("terrace",True)
        balcony=response.xpath("//div[@class='row infos-comp']/div[contains(.,'Balcon')]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony",False)
            else:
                item_loader.add_value("balcony",True)
        
        item_loader.add_value("landlord_name","CENTRALE IMMOBILIÈRE")
        item_loader.add_value("landlord_phone", "04.78.73.19.11")

        latitude_longitude=response.xpath("//div[@id='localisation']/div[@id='map']/@data-pos").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('[')[1].split(',')[0]
            longitude = latitude_longitude.split(',')[1].split(']')[0]
            if latitude and longitude:
                item_loader.add_value("longitude", longitude.strip())
                item_loader.add_value("latitude", latitude.strip())

        energy_label = response.xpath("//div[@class='row']//img[contains(@src,'/dpe-')]/@src[not(contains(.,'dpe-z'))]").get()
        if energy_label:
            energy_label = energy_label.split('/dpe-')[1].split('.')[0].strip().upper()            
            if energy_label.isalpha():
                item_loader.add_value("energy_label", energy_label)
        available_date = response.xpath("//div[@class='row infos-comp']/div[contains(.,'Disponibilité')]/text()[.!='Disponible']").get()
        if available_date:           
            try:
                available_date = available_date.replace("le","").strip()           
                item_loader.add_value("available_date", dateparser.parse(available_date).strftime("%Y-%m-%d"))     
            except:
                pass
        
        yield item_loader.load_item()