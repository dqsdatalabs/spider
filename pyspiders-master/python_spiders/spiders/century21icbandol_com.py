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
    name = 'century21icbandol_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source= 'Century21icbandol_PySpider_france_fr'
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True
    }
 

    def start_requests(self):
        start_urls = [
            {"url": "https://www.century21-ic-bandol.com/annonces/location-appartement/", "property_type": "apartment"},
	        
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='tw-w-full md:tw-w-auto tw-flex-grow'][2]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())

            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//div[@class='c-text-theme-cta tw-text-c21-gold tw-font-bold tw-tracking-c21-theme-120 tw-uppercase']//text()").get().split(":")[1].strip())
     
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        title = response.xpath("//h1//span[@class='tw-block c-text-theme-heading-1']//text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace("\u00e0","").replace("\n",""))
   
        description = response.xpath("//div[@class='has-formated-text tw-inline-block']//text()").get()
        if description:
            item_loader.add_value("description", description.strip().replace("\u00b2","").replace("\u00e7","").replace("\u00e9",""))
        

        address = response.xpath("//h1//span[@class='tw-block tw-text-lg md:tw-text-xl tw-font-bold tw-tracking-c21-theme-120']//text()").get()
        if address:    
            city = address.split(" -")[0].replace("-","").strip()
            zipcode = address.split(" -")[1].replace("-","").strip()
            item_loader.add_value("address", f"{city} {zipcode}".strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]//text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
        item_loader.add_value("square_meters", square_meters)
        
        room_count = "".join(response.xpath("//li[contains(.,'Nombre de pièces')]//text()").get())
        if room_count:
            room_count = room_count.split(":")[1].strip()
        item_loader.add_value("room_count", room_count)
       
        
        furnished = response.xpath("//li[contains(.,'meublée')]//text()").get()
        if furnished: 
             item_loader.add_value("furnished", True)
        
        utilities = response.xpath("//ul[@class='c-list md:tw-pl-14']//p//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split(",")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='tw-absolute tw-inset-0 tw-w-full tw-h-full md:tw-transform md:tw-transition-transform md:tw-duration-550 md:tw-ease-out-quint md:hover:tw-scale-105']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//li[contains(.,'Détail du loyer')]//text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0","").replace(" ","").strip())
            price = price.split("/")[0].split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip(":").strip("€").replace(" ",""))
        
        services = response.xpath("//div[@class='tw-grid tw-grid-cols-3 tw-gap-y-2 tablet-landscape:tw-grid-cols-4 tw-gap-y-4 tw-text-center tw-mb-6 tablet-landscape:tw-mb-0']//div[@class='tw-flex tw-flex-col tw-items-center']//p//text()").getall()
        if "Ascenseur" in services:
            item_loader.add_value("elevator", True)
        if "Balkon" in services:
            item_loader.add_value("balcony", True)
        if "Terrasse" in services:
            item_loader.add_value("terrace", True)
        if "outdoor parking" in services:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//div[@class='c-the-dpeges-new__dpe__arrow c-the-dpeges-new__dpe__arrow--d is-active']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)


        item_loader.add_value("landlord_name", "CENTURY 21 Immobilière Charlemagne")
        item_loader.add_value("landlord_phone", "04 94 90 60 99")
        
        yield item_loader.load_item()
