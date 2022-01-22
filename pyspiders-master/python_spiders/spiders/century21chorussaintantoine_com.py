# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from geopy.geocoders import Nominatim


class MySpider(Spider):
    name = 'century21chorussaintantoine_com'
    execution_type='testing'
    country='france'
    locale='fr'

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
            {"url": "https://www.century21-chorus-saint-antoine.com/annonces/location/?types_biens=appartement&xhr=true", "property_type": "apartment"},
	        {"url": "https://www.century21-chorus-saint-antoine.com/annonces/location/?types_biens=maison&xhr=true", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[contains(@class,'annoncesListeBien')]/li[contains(@class,'annonce')]"):
            follow_url = response.urljoin(item.xpath("./div[@class='contentAnnonce']/a/@href").extract_first())
            # prop_type = item.xpath("./div[@class='contentAnnonce']/a//h4/text()[1]").extract_first().strip()

            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Century21chorussaintantoine_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.meta.get("property_type")
        
        item_loader.add_css("title", "h1.h1_page")

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//span[@class='tw-text-c21-gold font20 margL20 tw-font-semibold']/text()").get().split(":")[1].strip())
   
        description = response.xpath("//div[@class='desc-fr']/p/text()").get()
        if description:
            item_loader.add_value("description", description.strip())
        

        latlng = response.xpath("//script[contains(.,'points')]/text()").get()
        if latlng:
            latitude = latlng.split("lat\":")[1].strip().split(",")[0]
            longitude = latlng.split("lng\":")[1].strip().split(",")[0]
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
   
        address = response.xpath("//div[@id='filAriane']/ol/li[last()]/a/span/text()").get()
        if address:
            city = address.split("(")[0].strip().split(" ")[-1]
            zipcode = address.split("(")[1].strip().split(")")[0]
            item_loader.add_value("address", "{} {}".format(city,zipcode))
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip()) 
    
        item_loader.add_value("property_type", prop_type)
        
        square_meters = response.xpath("//span[contains(.,'Surface habitable')]/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].split(",")[0].strip()
        item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//span[contains(.,'pièces')]/following-sibling::text()[1]").get()
        if room_count:
            room_count = room_count.strip()
        item_loader.add_value("room_count", room_count)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='gal-item']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        price = response.xpath("//span[contains(.,'loyer')]/following-sibling::text()[1]").get()
        if price:
            price = price.split("/")[0].strip(":").split("€")[0].strip()

        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::text()[1]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip(":").strip("€").strip())

        
        landlord_phone = response.xpath("//a[@class='btn-contact contact-tel tw-block']/@data-tel").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        item_loader.add_value("landlord_name", "CENTURY 21 Chorus Saint-Antoine")

        
        services = response.xpath("//div[./span[.='Equipement ']]/ul/li//text()").getall()
        if "Ascenseur" in services:
            item_loader.add_value("elevator", True)
        if "Balkon" in services:
            item_loader.add_value("balcony", True)
        if "Terrasse" in services:
            item_loader.add_value("terrace", True)
        if "Parking" in services:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//ul[@id='dep1']//span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))

        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label