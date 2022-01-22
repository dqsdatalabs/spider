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
    name = 'barneslyon_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.barnes-lyon.com/location-immobilier-prestige/lyon/appartement?", "property_type": "apartment"},
	        {"url": "https://www.barnes-lyon.com/location-immobilier-prestige/lyon/maison-villa?", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='container']//div[contains(@class,'anim-fade-up')]/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//ul[contains(@class,'pagination')]/li[@class='next']/a/@href").extract_first()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Barneslyon_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//h2[@class='h1 anim-fade-up']/text()").get()
        if title:
            item_loader.add_value("title", title)
            address = title.split(" - ")[0].strip() + ", " + title.split(" -")[1].strip()            
            item_loader.add_value("address", address)
            if "garage" in title.lower():
                item_loader.add_value("parking", True)
            if "meublé" in title.lower():
                item_loader.add_value("furnished", True)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//div[./div[.='RÉF']]/em//text()").get())
   
        description = response.xpath("//div[@class='wysiwyg-content']/text()").get()
        if description:
            item_loader.add_value("description", description.strip())
        city = response.xpath("//div[./div[.='QUARTIER / SECTEUR']]/em//text()[.!=' - ']").get()
        if city:
            item_loader.add_value("city", city.strip())

        latitude = response.xpath("//div[@class='map anim-fade-up']/@data-map-center-lat").get()
        longitude = response.xpath("//div[@class='map anim-fade-up']/@data-map-center-lng").get()
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
  
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("//div[./div[.='SURFACE']]/em//text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",",".")
        else:
            if "m²" in description:
                square_meters = description.split("m²")[0].strip().split(" ")[-1].strip().replace(",",".")
        item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//div[./div[contains(.,'CHAMBRE')]]/em//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)  
        elif "Studio" in title:
            item_loader.add_value("room_count", "1")
        else:
            roomcount1=response.xpath("//h2[@class='h1 anim-fade-up']/text()").get()
            if roomcount1:
                roomcount1=roomcount1.split("-")[-1].strip()
                roomcount1=re.findall("\d+",roomcount1)
                item_loader.add_value("room_count",roomcount1)



        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//div[./div[contains(.,'PRIX')]]/em//text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        #     price = price.split("/")[0].replace("€","").replace(" ","")
        #     item_loader.add_value("rent", price)
        # item_loader.add_value("currency", "EUR")
        utilities = response.xpath("//li/span[contains(.,'Charges')]//strong/text()").get()
        if utilities:            
            utilities = utilities.split('€')[0].replace(',', '.').replace(' ', '')
            if utilities:     
                item_loader.add_value("utilities", str(int(float(utilities))))
  
        elevator = response.xpath("//ul[contains(@class,'accommodation-list')]/li[contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        swimming_pool = response.xpath("//ul[contains(@class,'accommodation-list')]/li[contains(.,'Piscine')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)    
        energy_label = response.xpath("//li/span[contains(.,' énergie')]//strong/text()").get()
        if energy_label:
            if energy_label.isalpha():
                item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//div[@class='agent-info']/h2/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[@class='agent-info']//b/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//div[@class='agent-info']/em[contains(.,'@')]/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()