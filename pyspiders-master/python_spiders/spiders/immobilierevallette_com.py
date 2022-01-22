# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin

class MySpider(Spider):
    name = 'immobilierevallette_com'
    execution_type='testing'
    country='france'
    locale='en'
    external_source = "Immobilierevallette_PySpider_france_en"
    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = url.get("type")

            payload = {
                "nature": "2",
                "type[]": str(r_type),
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }
            
            yield FormRequest(url="http://www.immobilierevallette.com/en/search/",
                                callback=self.parse,
                                formdata=payload,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='buttons']//a[@class='button']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))  
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//div[@class='titles']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
    

        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)
     
        address = response.xpath("//section[@class='showPictures']//h2/text()[last()]").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        room_count = response.xpath("//li[@class='alt'][contains(.,'Rooms')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0])
        bathroom_count = response.xpath("//section[@class='showPictures']//li[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(' ')[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        rent = response.xpath("//li[contains(text(),'Month')]/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(',', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
      
        external_id = response.xpath("//li[contains(text(),'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
            item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//section[@class='showPictures']/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Guarantee')]/span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//img[contains(@alt,'Conventional')]/@src").get()
        if energy_label:
            energy_label = float(energy_label.split('/')[-1].strip().replace('%2C', '.'))
            if energy_label > 0:
                if energy_label <= 50:
                    energy_label = 'A'
                elif energy_label >= 51 and energy_label <= 90:
                    energy_label = 'B'
                elif energy_label >= 91 and energy_label <= 150:
                    energy_label = 'C'
                elif energy_label >= 151 and energy_label <= 230:
                    energy_label = 'D'
                elif energy_label >= 231 and energy_label <= 330:
                    energy_label = 'E'
                elif energy_label >= 331 and energy_label <= 450:
                    energy_label = 'F'
                elif energy_label >= 451:
                    energy_label = 'G'
                item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//h2[.='Services']/following-sibling::ul/li[contains(.,'Furnished')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            floor = floor.strip().split('/')[0].strip()
            item_loader.add_value("floor", floor)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)
        
        swimming_pool = response.xpath("//h2[.='Services']/following-sibling::ul/li[contains(.,'Swimming')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        parking = response.xpath("//li[contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        utilities = response.xpath("//li[contains(.,'Fees')]//span//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        landlord_name = response.xpath("//p[@class='smallIcon userName']/strong/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "CABINET VALLETTE (C.G.I.V)")

        landlord_phone = response.xpath("//span[@class='phone smallIcon']/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "+33 (0)6 16 11 88 02")

        landlord_email = response.xpath("//span[@class='mail smallIcon']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

