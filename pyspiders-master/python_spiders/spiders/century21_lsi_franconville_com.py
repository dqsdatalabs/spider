# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'century21_lsi_franconville_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
        "CONCURRENT_REQUESTS" : 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_FR_ON" : True,
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.century21-lsi-franconville.com/annonces/location-appartement/", "property_type": "apartment"},
	        {"url": "https://www.century21-lsi-franconville.com/annonces/location-maison/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        if response.xpath("//div[contains(@class,'c-the-property-thumbnail')]/a/@href").extract_first():
            for item in response.xpath("//div[contains(@class,'c-the-property-thumbnail')]/a"):
                follow_url = response.urljoin(item.xpath("./@href").extract_first())
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        else:
            for item in response.xpath("//div[contains(@class,'c-the-property-thumbnail')]//div"):
                follow_url = response.urljoin(item.xpath(".//@href").extract_first())
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        parking = "".join(response.xpath("//h1[@class='h1_page']/text()[contains(.,'Parking')]").extract())
        if parking:return
        item_loader.add_value("external_source", "Century21_Lsi_Franconville_PySpider_france")
     
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        external_id = response.xpath("//div[contains(@class,'c-text-theme-cta')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
   
        description = response.xpath("//div[contains(@class,'has-formated-text')]/text()").getall()
        if description:
            item_loader.add_value("description", "".join(description).strip())
        
        item_loader.add_xpath("latitude", "//div/@data-lat")
        item_loader.add_xpath("longitude", "//div/@data-lng")
        
        address = response.xpath("//h1/span[last()]/text()").get()
        if address:    
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" -")[0])
        zipcode = response.xpath("//li[@itemprop='itemListElement'][last()]//span[@itemprop='name']/text()").get()
        if zipcode:        
            item_loader.add_value("zipcode", zipcode.split("(")[1].split(")")[0].strip())
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li[contains(.,'Nombre de pi??ces')]/text()").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
        else:
            if title and "pi??ces" in title.lower():
                for i in title.split(" - "):
                    if "pi??ces" in i:
                        room_count = i.split(" ")[0].strip()
                        break
        item_loader.add_value("room_count", room_count)


        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'tw-absolute tw-inset-0')]/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        # images = response.xpath("//section//div/div[@class='tw-absolute tw-inset-0 tw-w-full tw-h-full']/img/@src").get()
        # if images:
        #     images = images.split("source: '")
        #     for i in range(1,len(images)):
        #         item_loader.add_value("images", response.urljoin(images[i].split("'")[0]))
        
        price = response.xpath("//span[contains(@class,'price ')]/text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0","").replace(" ","").replace("&hairsp;","").strip())

        deposit = response.xpath("//li[contains(.,'D??p??t de garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].strip("???").replace(" ",""))
        
        # utilities = response.xpath("//p[contains(.,'charges')]/text()").get()
        # if utilities:
        #     item_loader.add_value("utilities", utilities.split(":")[1].split(",")[0].replace(" ",""))
        
        parking = response.xpath("//p[@class='tw-leading-none' and contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'meubl??e')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//p[@class='tw-leading-none' and contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        energy_label = response.xpath("//span[contains(@class,'tw-block tw-m-auto')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label.strip().replace(",",".")))
        
        item_loader.add_value("landlord_name", "CENTURY 21 C.A.I.")
        item_loader.add_value("landlord_phone", "01 34 44 20 00")

        
        yield item_loader.load_item()
def energy_label_calculate(energy_number):
    energy_number = int(float(energy_number))
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